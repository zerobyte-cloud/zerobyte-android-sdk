# ZeroByte Android SDK

The Kotlin client for ZeroByte, an end-to-end encrypted relay between two
paired devices of one app. It is a plain JVM library: it runs on Android API 28
and newer (the first release with ChaCha20-Poly1305 in the platform) and on any
JVM 11+. It has no runtime dependency beyond the Kotlin standard library, and
HTTP goes through `HttpURLConnection`.

## How it works

Two devices pair once and agree a 32-byte link key and a link id. From these,
each device derives the mailbox name, an access token and one encryption key
per direction. Every message is sealed with ChaCha20-Poly1305 on the device
that sends it. The relay holds the sealed envelope until the other device
collects it, and it never has a key to open it with.

The wire format is specified in
[Relay Protocol v1](https://github.com/szymonmichalik/zerobyte-protocol/blob/master/relay-v1.md).

## Installation

The library is not on Maven Central yet. Publish it to your local Maven
repository:

```sh
git clone https://github.com/szymonmichalik/zerobyte-android-sdk.git
cd zerobyte-android-sdk
./gradlew :zerobyte:publishToMavenLocal
```

Then, in your app:

```kotlin
repositories { mavenLocal() }

dependencies {
    implementation("cloud.zerobyte:zerobyte:1.0.0")
}
```

## Usage

```kotlin
import cloud.zerobyte.*

// Both devices agreed linkKey (32 bytes) and linkID when they paired.
val credentials = Credentials(linkKey, linkID)
val relay = Relay(
    appID = "<your app id>",
    credentials = credentials,
    base = "https://relay.example.com/v1/", // your relay
)
val link = Link(credentials, Direction.C2P, relay, savedLastReceived)

// The calls block: run them off the main thread.
withContext(Dispatchers.IO) {
    link.send("hello".toByteArray())
    link.receive().forEach { handle(it.plaintext) }
}
```

- **Pairing** is the app's job: for example a QR code carrying a fresh 32-byte
  key, or an ECDH exchange confirmed by comparing digits on both screens. Keep
  the link key in the Android Keystore or encrypted storage.
- **Directions.** The primary holds the source of truth and sends
  `Direction.P2C`; the companion sends `Direction.C2P`.
- **State.** Persist `link.lastReceived` and pass it back on the next start, so
  messages are not delivered twice.
- **Unpairing.** Call `link.erase()` to delete the mailbox at once.
- **The relay.** `base` is your relay's `/v1/` URL, with the trailing slash.
  There is no default.

### Watching for new messages

The SDK does not wrap the watch WebSocket, because Android has no platform
WebSocket client. Open `relay.watchURL(direction)` with the client your app
already uses (OkHttp works), offer the subprotocol `zerobyte.v1` and send the
header `Authorization: Bearer <credentials.bearerToken>`. Each text message is
a sequence number: call `link.receive()` when one arrives, and once after the
socket opens.

The app id identifies your app on your relay. Generate one with
`npm run app-id` in a clone of
[zerobyte-relay](https://github.com/szymonmichalik/zerobyte-relay) and add it
to `RELAY_APPS`.

## Testing

```sh
./gradlew test
```

The tests run on the JVM: protocol vectors and `Link`. To also test against a
real relay, start one locally (see zerobyte-relay) and run:

```sh
RELAY_BASE=http://127.0.0.1:8080/v1/ RELAY_APP=<app id> ./gradlew test
```

`zerobyte/src/test/resources/relay-v1.json` is copied from the independent
reference in
[zerobyte-protocol](https://github.com/szymonmichalik/zerobyte-protocol/tree/master/spec).
Never regenerate it from this library.

## Licence

Apache-2.0, see [`LICENSE`](LICENSE).

## About ZeroByte

ZeroByte is an open-source project by Szymon Michalik: the protocol, the relay
and the SDKs. relay.zerobyte.cloud runs the same code for Derived and isn't
open to other apps. To use ZeroByte in your app, run your own relay: see
[zerobyte-relay](https://github.com/szymonmichalik/zerobyte-relay).
