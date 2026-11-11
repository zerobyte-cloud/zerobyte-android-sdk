package cloud.zerobyte.timeline

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.SecureRandom

// The wire format of Timeline v1 (timeline-v1.md in zerobyte-protocol). Every
// list is sorted and every field fixed-width or length-prefixed, so one change
// set has exactly one encoding and every SDK computes the same commit id.

/** Timeline errors. `code` is the same string in every ZeroByte SDK: `malformed`, `unsupported_version`, `too_large`, `diverged`. */
class TimelineException(val code: String) : Exception(code)

internal fun malformed() = TimelineException("malformed")

object TimelineLimits {
    const val MAX_MESSAGE_BYTES = 1_048_576
    const val MAX_COLLECTION_BYTES = 64
    const val MAX_KEY_BYTES = 1_024
    internal const val COMMIT_FIXED_BYTES = 71
    internal const val COMMITS_MESSAGE_OVERHEAD = 7
    internal const val CATCH_UP_PART_OVERHEAD = 8
}

internal fun sha256(data: ByteArray): ByteArray = MessageDigest.getInstance("SHA-256").digest(data)

internal fun ByteArray.hex(): String {
    val digits = "0123456789abcdef"
    val out = StringBuilder(size * 2)
    for (b in this) {
        val v = b.toInt() and 0xff
        out.append(digits[v ushr 4]).append(digits[v and 0x0f])
    }
    return out.toString()
}

internal fun hexBytes(text: String): ByteArray? {
    if (text.length % 2 != 0) return null
    val out = ByteArray(text.length / 2)
    for (i in out.indices) {
        val hi = Character.digit(text[i * 2], 16)
        val lo = Character.digit(text[i * 2 + 1], 16)
        if (hi < 0 || lo < 0) return null
        out[i] = ((hi shl 4) or lo).toByte()
    }
    return out
}

// Bytewise, unsigned: how the protocol orders replica ids and names.
internal fun compareBytes(a: ByteArray, b: ByteArray): Int {
    for (i in 0 until minOf(a.size, b.size)) {
        val x = a[i].toInt() and 0xff
        val y = b[i].toInt() and 0xff
        if (x != y) return x - y
    }
    return a.size - b.size
}

internal val ZERO_ID = ByteArray(32)

// Strict UTF-8 both ways: a lone surrogate or an invalid byte is refused, a
// leading U+FEFF kept, so a name decodes to exactly its bytes.
internal fun strictUtf8(text: String): ByteArray? = try {
    val buffer = StandardCharsets.UTF_8.newEncoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .encode(CharBuffer.wrap(text))
    ByteArray(buffer.remaining()).also { buffer.get(it) }
} catch (_: CharacterCodingException) {
    null
}

internal fun strictString(bytes: ByteArray): String? = try {
    StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes))
        .toString()
} catch (_: CharacterCodingException) {
    null
}

/** A replica's id: 16 bytes, not all zero, compared as bytes. */
class ReplicaID private constructor(internal val raw: ByteArray) : Comparable<ReplicaID> {
    val bytes: ByteArray get() = raw.copyOf()

    /** 32 lowercase hex characters. */
    val hex: String get() = raw.hex()

    override fun compareTo(other: ReplicaID): Int = compareBytes(raw, other.raw)
    override fun equals(other: Any?): Boolean = other is ReplicaID && raw.contentEquals(other.raw)
    override fun hashCode(): Int = raw.contentHashCode()
    override fun toString(): String = hex

    companion object {
        private val random = SecureRandom()

        /** Null unless 16 bytes, not all zero. */
        @JvmStatic
        fun of(bytes: ByteArray): ReplicaID? =
            if (bytes.size == 16 && bytes.any { it != 0.toByte() }) ReplicaID(bytes.copyOf()) else null

        /** From 32 hex characters; null otherwise. */
        @JvmStatic
        fun fromHex(hex: String): ReplicaID? = hexBytes(hex)?.let(::of)

        /** A fresh id: 16 random bytes. */
        @JvmStatic
        fun random(): ReplicaID {
            while (true) {
                val bytes = ByteArray(16).also(random::nextBytes)
                of(bytes)?.let { return it }
            }
        }
    }
}

/** Where an entry lives: the app's collection and key, compared as UTF-8 bytes. */
class TimelineSlot(val collection: String, val key: String) : Comparable<TimelineSlot> {
    internal val collectionBytes: ByteArray
    internal val keyBytes: ByteArray
    private val wellFormed: Boolean

    init {
        val c = strictUtf8(collection)
        val k = strictUtf8(key)
        wellFormed = c != null && k != null
        collectionBytes = c ?: collection.toByteArray(StandardCharsets.UTF_8)
        keyBytes = k ?: key.toByteArray(StandardCharsets.UTF_8)
    }

    /** Both names 1 to 64 and 1 to 1,024 bytes of well-formed UTF-8. */
    val isValid: Boolean
        get() = wellFormed &&
            collectionBytes.size in 1..TimelineLimits.MAX_COLLECTION_BYTES &&
            keyBytes.size in 1..TimelineLimits.MAX_KEY_BYTES

    internal val encodedLength: Int get() = 3 + collectionBytes.size + keyBytes.size

    override fun compareTo(other: TimelineSlot): Int {
        val c = compareBytes(collectionBytes, other.collectionBytes)
        return if (c != 0) c else compareBytes(keyBytes, other.keyBytes)
    }

    override fun equals(other: Any?): Boolean =
        other is TimelineSlot &&
            collectionBytes.contentEquals(other.collectionBytes) &&
            keyBytes.contentEquals(other.keyBytes)

    override fun hashCode(): Int = 31 * collectionBytes.contentHashCode() + keyBytes.contentHashCode()
    override fun toString(): String = "$collection/$key"

    internal companion object {
        fun decode(collection: ByteArray, key: ByteArray): TimelineSlot {
            if (collection.size !in 1..TimelineLimits.MAX_COLLECTION_BYTES ||
                key.size !in 1..TimelineLimits.MAX_KEY_BYTES
            ) {
                throw malformed()
            }
            val c = strictString(collection) ?: throw malformed()
            val k = strictString(key) ?: throw malformed()
            return TimelineSlot(c, k)
        }
    }
}

/** Who wrote an entry and when. Stamps order by clock, then replica id, then sequence. */
data class TimelineStamp(val clock: ULong, val replica: ReplicaID, val sequence: ULong) : Comparable<TimelineStamp> {
    override fun compareTo(other: TimelineStamp): Int {
        if (clock != other.clock) return clock.compareTo(other.clock)
        val r = replica.compareTo(other.replica)
        if (r != 0) return r
        return sequence.compareTo(other.sequence)
    }
}

/** A replica's last commit: its sequence, clock and id (32 bytes). */
class TimelineHead(val sequence: ULong, val clock: ULong, id: ByteArray) {
    private val idBytes = id.copyOf()
    val id: ByteArray get() = idBytes.copyOf()

    internal fun sameId(other: ByteArray): Boolean = idBytes.contentEquals(other)
    internal fun writeId(w: TimelineWriter) = w.bytes(idBytes)

    override fun equals(other: Any?): Boolean =
        other is TimelineHead && sequence == other.sequence && clock == other.clock && idBytes.contentEquals(other.idBytes)

    override fun hashCode(): Int = (sequence.hashCode() * 31 + clock.hashCode()) * 31 + idBytes.contentHashCode()
    override fun toString(): String = "TimelineHead($sequence, $clock, ${idBytes.hex()})"
}

/** One change in a commit: a value, or null for a deletion. */
class TimelineChange(val slot: TimelineSlot, value: ByteArray?) {
    private val bytes = value?.copyOf()
    val value: ByteArray? get() = bytes?.copyOf()

    internal val raw: ByteArray? get() = bytes
    internal val encodedLength: Int get() = slot.encodedLength + 1 + (bytes?.let { 4 + it.size } ?: 0)

    override fun equals(other: Any?): Boolean =
        other is TimelineChange && slot == other.slot && sameValue(bytes, other.bytes)

    override fun hashCode(): Int = slot.hashCode() * 31 + (bytes?.contentHashCode() ?: 0)
}

/** An entry as a catch-up carries it: where, who wrote it, and its value (null: deleted). */
class TimelineStampedEntry(val slot: TimelineSlot, val stamp: TimelineStamp, value: ByteArray?) {
    private val bytes = value?.copyOf()
    val value: ByteArray? get() = bytes?.copyOf()

    internal val raw: ByteArray? get() = bytes
    internal val encodedLength: Int get() = slot.encodedLength + 33 + (bytes?.let { 4 + it.size } ?: 0)

    override fun equals(other: Any?): Boolean =
        other is TimelineStampedEntry && slot == other.slot && stamp == other.stamp && sameValue(bytes, other.bytes)

    override fun hashCode(): Int = (slot.hashCode() * 31 + stamp.hashCode()) * 31 + (bytes?.contentHashCode() ?: 0)
}

internal fun sameValue(a: ByteArray?, b: ByteArray?): Boolean =
    if (a == null || b == null) a == null && b == null else a.contentEquals(b)

/** The hybrid clock (timeline-v1.md §2). */
object TimelineClock {
    private const val WALL_LIMIT = 1L shl 48

    /** `max(last + 1, now_ms << 16)`, with `now` in milliseconds since 1970. */
    @JvmStatic
    fun next(last: ULong, nowMilliseconds: Long): ULong {
        val wall = when {
            nowMilliseconds >= WALL_LIMIT -> ULong.MAX_VALUE
            nowMilliseconds <= 0L -> 0uL
            else -> nowMilliseconds.toULong() shl 16
        }
        val after = if (last == ULong.MAX_VALUE) ULong.MAX_VALUE else last + 1uL
        return maxOf(after, wall)
    }
}

/** Why a replica asks the other for a catch-up (timeline-v1.md §5): bit flags. */
data class TimelineRequest(val rawValue: Int) {
    val isEmpty: Boolean get() = rawValue == 0

    operator fun contains(other: TimelineRequest): Boolean = (rawValue and other.rawValue) == other.rawValue
    operator fun plus(other: TimelineRequest): TimelineRequest = TimelineRequest(rawValue or other.rawValue)

    companion object {
        @JvmField val NONE = TimelineRequest(0)

        /** This replica is new or lost its state. */
        @JvmField val NEW_REPLICA = TimelineRequest(1)

        /** A commit arrived after a sequence that never did. */
        @JvmField val GAP = TimelineRequest(2)
    }
}

/** One replica's changes (timeline-v1.md §3). `bytes` is its encoding, `id` their SHA-256. */
class TimelineCommit private constructor(
    val replica: ReplicaID,
    val sequence: ULong,
    parent: ByteArray,
    val clock: ULong,
    /** The other replicas' sequences merged before this commit. */
    val seen: Map<ReplicaID, ULong>,
    val changes: List<TimelineChange>,
    encoded: ByteArray,
) {
    internal val raw: ByteArray = encoded
    internal val rawId: ByteArray = sha256(encoded)
    internal val rawParent: ByteArray = parent

    val parent: ByteArray get() = rawParent.copyOf()
    val bytes: ByteArray get() = raw.copyOf()
    val id: ByteArray get() = rawId.copyOf()

    override fun equals(other: Any?): Boolean = other is TimelineCommit && raw.contentEquals(other.raw)
    override fun hashCode(): Int = raw.contentHashCode()

    companion object {
        internal fun create(
            replica: ReplicaID,
            sequence: ULong,
            parent: ByteArray,
            clock: ULong,
            seen: Map<ReplicaID, ULong>,
            changes: List<TimelineChange>,
        ): TimelineCommit {
            val sorted = changes.sortedBy { it.slot }
            val w = TimelineWriter()
            w.byte(1)
            w.bytes(replica.raw)
            w.uint64(sequence)
            w.bytes(parent)
            w.uint64(clock)
            val others = seen.keys.sorted()
            w.uint16(others.size)
            for (other in others) {
                w.bytes(other.raw)
                w.uint64(seen.getValue(other))
            }
            w.uint32(sorted.size)
            for (change in sorted) {
                w.slot(change.slot)
                w.value(change.raw)
            }
            return TimelineCommit(replica, sequence, parent.copyOf(), clock, HashMap(seen), sorted, w.finish())
        }

        /** Reads and checks one commit (timeline-v1.md §3). */
        @JvmStatic
        @JvmOverloads
        fun decode(data: ByteArray, maximumMessageBytes: Int = TimelineLimits.MAX_MESSAGE_BYTES): TimelineCommit {
            if (data.size < TimelineLimits.COMMIT_FIXED_BYTES) throw malformed()
            val r = TimelineReader(data)
            if (r.byte() != 1) throw TimelineException("unsupported_version")
            val replica = r.replica()
            val sequence = r.uint64()
            val parent = r.take(32)
            val clock = r.uint64()
            if (sequence == 0uL || (sequence == 1uL) != parent.contentEquals(ZERO_ID)) throw malformed()
            val seen = HashMap<ReplicaID, ULong>()
            var previous: ReplicaID? = null
            repeat(r.uint16()) {
                val other = r.replica()
                if (other == replica) throw malformed()
                val otherSequence = r.uint64()
                if (otherSequence == 0uL || previous?.let { other <= it } == true) throw malformed()
                seen[other] = otherSequence
                previous = other
            }
            val count = r.uint32()
            if (count == 0L) throw malformed()
            val changes = ArrayList<TimelineChange>()
            var previousSlot: TimelineSlot? = null
            for (i in 0 until count) {
                val slot = r.slot()
                val value = r.value()
                if (previousSlot != null && slot <= previousSlot) throw malformed()
                changes.add(TimelineChange(slot, value))
                previousSlot = slot
            }
            if (!r.isAtEnd) throw malformed()
            if (data.size > maximumMessageBytes - TimelineLimits.COMMITS_MESSAGE_OVERHEAD) {
                throw TimelineException("too_large")
            }
            return TimelineCommit(replica, sequence, parent, clock, seen, changes, data.copyOf())
        }
    }
}

/** The three messages of Timeline v1 (timeline-v1.md §5). */
sealed class TimelineMessage {
    /** Commits, oldest first. */
    data class Commits(val commits: List<TimelineCommit>) : TimelineMessage()

    /** What a replica has merged, and whether it asks for a catch-up. */
    data class State(val request: TimelineRequest, val heads: Map<ReplicaID, TimelineHead>) : TimelineMessage()

    /** One part of a catch-up; the final part carries the heads it covers. */
    data class CatchUp(
        val isFinal: Boolean,
        val heads: Map<ReplicaID, TimelineHead>,
        val entries: List<TimelineStampedEntry>,
    ) : TimelineMessage()

    /** The message as bytes. */
    fun encoded(): ByteArray {
        val w = TimelineWriter()
        when (this) {
            is Commits -> {
                w.byte(1)
                w.uint16(commits.size)
                for (commit in commits) {
                    w.uint32(commit.raw.size)
                    w.bytes(commit.raw)
                }
            }
            is State -> {
                w.byte(2)
                w.byte(request.rawValue)
                w.heads(heads)
            }
            is CatchUp -> {
                w.byte(3)
                w.byte(if (isFinal) 1 else 0)
                w.heads(if (isFinal) heads else emptyMap())
                val sorted = entries.sortedBy { it.slot }
                w.uint32(sorted.size)
                for (entry in sorted) {
                    w.slot(entry.slot)
                    w.uint64(entry.stamp.clock)
                    w.bytes(entry.stamp.replica.raw)
                    w.uint64(entry.stamp.sequence)
                    w.value(entry.raw)
                }
            }
        }
        return w.finish()
    }

    companion object {
        /** Reads and checks one message (timeline-v1.md §5). */
        @JvmStatic
        @JvmOverloads
        fun decode(data: ByteArray, maximumMessageBytes: Int = TimelineLimits.MAX_MESSAGE_BYTES): TimelineMessage {
            if (data.isEmpty()) throw malformed()
            if (data.size > maximumMessageBytes) throw TimelineException("too_large")
            val r = TimelineReader(data)
            when (r.byte()) {
                1 -> {
                    val count = r.uint16()
                    if (count == 0) throw malformed()
                    val raw = (0 until count).map { r.take(r.length()) }
                    if (!r.isAtEnd) throw malformed()
                    return Commits(raw.map { TimelineCommit.decode(it, maximumMessageBytes) })
                }
                2 -> {
                    val flags = r.byte()
                    if (flags and 0b11.inv() != 0) throw malformed()
                    val heads = r.heads()
                    if (!r.isAtEnd) throw malformed()
                    return State(TimelineRequest(flags), heads)
                }
                3 -> {
                    val flags = r.byte()
                    if (flags and 0b1.inv() != 0) throw malformed()
                    val isFinal = flags and 1 == 1
                    val heads = r.heads()
                    if (!isFinal && heads.isNotEmpty()) throw malformed()
                    val entries = ArrayList<TimelineStampedEntry>()
                    var previous: TimelineSlot? = null
                    for (i in 0 until r.uint32()) {
                        val slot = r.slot()
                        val clock = r.uint64()
                        val replica = r.replica()
                        val sequence = r.uint64()
                        if (sequence == 0uL) throw malformed()
                        val value = r.value()
                        if (previous != null && slot <= previous) throw malformed()
                        entries.add(TimelineStampedEntry(slot, TimelineStamp(clock, replica, sequence), value))
                        previous = slot
                    }
                    if (!r.isAtEnd) throw malformed()
                    return CatchUp(isFinal, heads, entries)
                }
                else -> throw TimelineException("unsupported_version")
            }
        }
    }
}

internal class TimelineWriter {
    private val out = ByteArrayOutputStream(256)

    val size: Int get() = out.size()

    fun byte(value: Int) = out.write(value)
    fun bytes(value: ByteArray) = out.write(value, 0, value.size)

    fun uint16(value: Int) {
        out.write(value ushr 8)
        out.write(value)
    }

    fun uint32(value: Int) {
        for (shift in 24 downTo 0 step 8) out.write(value ushr shift)
    }

    fun uint64(value: ULong) {
        val v = value.toLong()
        for (shift in 56 downTo 0 step 8) out.write((v ushr shift).toInt())
    }

    fun slot(slot: TimelineSlot) {
        byte(slot.collectionBytes.size)
        bytes(slot.collectionBytes)
        uint16(slot.keyBytes.size)
        bytes(slot.keyBytes)
    }

    fun value(value: ByteArray?) {
        if (value == null) {
            byte(0)
            return
        }
        byte(1)
        uint32(value.size)
        bytes(value)
    }

    fun heads(heads: Map<ReplicaID, TimelineHead>) {
        val replicas = heads.keys.sorted()
        uint16(replicas.size)
        for (replica in replicas) {
            val head = heads.getValue(replica)
            bytes(replica.raw)
            uint64(head.sequence)
            uint64(head.clock)
            head.writeId(this)
        }
    }

    fun finish(): ByteArray = out.toByteArray()
}

internal class TimelineReader(private val data: ByteArray) {
    private var offset = 0

    val isAtEnd: Boolean get() = offset == data.size

    fun take(count: Int): ByteArray {
        if (count < 0 || data.size - offset < count) throw malformed()
        val out = data.copyOfRange(offset, offset + count)
        offset += count
        return out
    }

    fun byte(): Int {
        if (offset >= data.size) throw malformed()
        return data[offset++].toInt() and 0xff
    }

    fun uint16(): Int = (byte() shl 8) or byte()

    fun uint32(): Long {
        var v = 0L
        repeat(4) { v = (v shl 8) or byte().toLong() }
        return v
    }

    fun uint64(): ULong {
        var v = 0L
        repeat(8) { v = (v shl 8) or byte().toLong() }
        return v.toULong()
    }

    /** A UInt32 length; one beyond the data fails at `take`. */
    fun length(): Int {
        val n = uint32()
        if (n > Int.MAX_VALUE) throw malformed()
        return n.toInt()
    }

    fun replica(): ReplicaID = ReplicaID.of(take(16)) ?: throw malformed()

    fun slot(): TimelineSlot {
        val collection = take(byte())
        val key = take(uint16())
        return TimelineSlot.decode(collection, key)
    }

    fun value(): ByteArray? = when (byte()) {
        0 -> null
        1 -> take(length())
        else -> throw malformed()
    }

    fun heads(): Map<ReplicaID, TimelineHead> {
        val heads = HashMap<ReplicaID, TimelineHead>()
        var previous: ReplicaID? = null
        repeat(uint16()) {
            val replica = replica()
            val sequence = uint64()
            val clock = uint64()
            val id = take(32)
            if (sequence == 0uL || previous?.let { replica <= it } == true) throw malformed()
            heads[replica] = TimelineHead(sequence, clock, id)
            previous = replica
        }
        return heads
    }
}
