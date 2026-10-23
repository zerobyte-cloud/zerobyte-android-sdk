package cloud.zerobyte

import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL

/** Relay errors. `last` is the relay's highest sequence on a 409. */
class RelayException(val code: String, val status: Int, val last: ULong? = null) :
    IOException("$code ($status)")

/** One page of a fetch. */
class Page(val envelopes: List<ByteArray>, val more: Boolean)

/** What a [Link] needs from a relay. [Relay] is the HTTPS one; tests use a memory one. */
interface RelayTransport {
    fun claim()
    fun put(envelope: ByteArray, direction: Direction, sequence: ULong)
    fun fetch(direction: Direction, after: ULong?): Page
    fun lastSequence(direction: Direction): ULong?
    fun prune(direction: Direction, through: ULong)
    fun erase()
}

/**
 * The HTTPS side of the protocol, with no dependencies beyond the platform.
 * Blocking: call it off the main thread (on Android, e.g. `Dispatchers.IO`).
 *
 * The watch WebSocket is not wrapped here, because Android has no platform
 * WebSocket client. Open `watchURL(direction)` with any client (OkHttp works)
 * offering the subprotocols `zerobyte.v1` and `"zerobyte.bearer." + bearerToken`,
 * or send `Authorization: Bearer <token>`; each text message is a sequence.
 */
class Relay @JvmOverloads constructor(
    appID: String,
    private val credentials: Credentials,
    base: String,
    private val timeoutMillis: Int = 30_000,
) : RelayTransport {
    val mailbox: String = URL(URL(base), "$appID/mailbox/${credentials.mailboxName}").toString()

    fun watchURL(direction: Direction): String =
        "$mailbox/${direction.path}/watch".replaceFirst("https://", "wss://").replaceFirst("http://", "ws://")

    override fun claim() {
        val body = """{"tokenDigest":"${credentials.tokenDigest}"}""".toByteArray()
        call("PUT", "", body, "application/json")
    }

    override fun put(envelope: ByteArray, direction: Direction, sequence: ULong) {
        call("PUT", "/${direction.path}/$sequence", envelope, "application/octet-stream")
    }

    override fun fetch(direction: Direction, after: ULong?): Page {
        val (connection, body) = call("GET", "/${direction.path}?after=${after ?: 0uL}&limit=16")
        return Page(Wire.split(body), connection.getHeaderField("X-Relay-More") == "1")
    }

    override fun lastSequence(direction: Direction): ULong? =
        call("HEAD", "/${direction.path}").first.getHeaderField("X-Relay-Last")?.toULongOrNull()

    override fun prune(direction: Direction, through: ULong) {
        call("DELETE", "/${direction.path}?through=$through")
    }

    override fun erase() {
        call("DELETE", "")
    }

    private fun call(
        method: String,
        path: String,
        body: ByteArray? = null,
        type: String? = null,
    ): Pair<HttpURLConnection, ByteArray> {
        val connection = URL(mailbox + path).openConnection() as HttpURLConnection
        try {
            connection.requestMethod = method
            connection.connectTimeout = timeoutMillis
            connection.readTimeout = timeoutMillis
            connection.useCaches = false
            connection.instanceFollowRedirects = false
            connection.setRequestProperty("Authorization", "Bearer ${credentials.bearerToken}")
            if (body != null) {
                connection.doOutput = true
                connection.setRequestProperty("Content-Type", type)
                connection.setFixedLengthStreamingMode(body.size)
                connection.outputStream.use { it.write(body) }
            }
            val status = connection.responseCode
            if (status !in 200..299) {
                connection.errorStream?.use { it.readBytes() }
                val code = when (status) {
                    401, 403 -> "unauthorized"
                    409 -> "sequence_conflict"
                    413, 507 -> "quota_exceeded"
                    else -> "unavailable"
                }
                throw RelayException(code, status, connection.getHeaderField("X-Relay-Last")?.toULongOrNull())
            }
            val bytes = if (method == "HEAD" || status == 204) ByteArray(0)
            else connection.inputStream.use { it.readBytes() }
            return connection to bytes
        } finally {
            connection.disconnect()
        }
    }
}
