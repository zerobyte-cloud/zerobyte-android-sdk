package cloud.zerobyte.timeline

import cloud.zerobyte.Credentials
import cloud.zerobyte.Direction
import cloud.zerobyte.Notify
import cloud.zerobyte.Page
import cloud.zerobyte.RelayException
import cloud.zerobyte.RelayTransport
import java.security.SecureRandom
import java.util.TreeMap
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

// The relay in memory, with an optional envelope limit per direction.
private class MemoryRelay(private val capacity: Int? = null) : RelayTransport {
    private val stored = mapOf(Direction.P2C to TreeMap<ULong, ByteArray>(), Direction.C2P to TreeMap())
    private val last = HashMap<Direction, ULong>()
    val notified = ArrayList<Notify>()

    override fun claim() {}

    override fun put(envelope: ByteArray, direction: Direction, sequence: ULong) =
        put(envelope, direction, sequence, Notify.NONE)

    override fun put(envelope: ByteArray, direction: Direction, sequence: ULong, notify: Notify) {
        val previous = last[direction]
        if (previous != null && sequence <= previous) throw RelayException("sequence_conflict", 409, previous)
        val mailbox = stored.getValue(direction)
        if (capacity != null && mailbox.size >= capacity) throw RelayException("quota_exceeded", 507)
        mailbox[sequence] = envelope
        last[direction] = sequence
        if (notify != Notify.NONE) notified.add(notify)
    }

    override fun fetch(direction: Direction, after: ULong?): Page {
        val mailbox = stored.getValue(direction)
        val keys = mailbox.tailMap(after ?: 0uL, false).keys.toList()
        val page = keys.take(16)
        return Page(page.map(mailbox::getValue), keys.size > page.size)
    }

    override fun lastSequence(direction: Direction): ULong? = last[direction]

    override fun prune(direction: Direction, through: ULong) {
        stored.getValue(direction).headMap(through, true).clear()
    }

    override fun erase() {
        stored.values.forEach { it.clear() }
        last.clear()
    }

    fun count(direction: Direction) = stored.getValue(direction).size
}

// One device: its timeline, saved after every step as an app would.
private class Device(val sends: Direction, val credentials: Credentials, val relay: MemoryRelay, keepsOwn: Boolean) {
    val timeline = Timeline(forgetsOwnValues = if (keepsOwn) emptySet() else setOf("day", "stream", "workout"))
    var saved: ByteArray = timeline.encoded()

    // What an app that does not keep its own values would produce again.
    val source = HashMap<TimelineSlot, ByteArray>()

    fun sync(budget: Int? = null, at: Long = 1_762_855_200): TimelineSyncOutcome =
        TimelineRelay.sync(
            timeline, credentials, relay, sends,
            now = at * 1000,
            commitBudget = budget,
            notify = Notify.SILENT,
            values = { source[it] },
        ) { saved = it.encoded() }

    fun set(value: String, collection: String, key: String) {
        val slot = TimelineSlot(collection, key)
        source[slot] = value.toByteArray()
        timeline.set(slot, value.toByteArray())
    }

    fun text(collection: String, key: String): String? =
        timeline.value(TimelineSlot(collection, key))?.toString(Charsets.UTF_8)
}

private class Devices(capacity: Int? = null) {
    val credentials = SecureRandom().let { random ->
        Credentials(ByteArray(32).also(random::nextBytes), ByteArray(16).also(random::nextBytes))
    }
    val relay = MemoryRelay(capacity)
    val phone = Device(Direction.P2C, credentials, relay, keepsOwn = false)
    val mac = Device(Direction.C2P, credentials, relay, keepsOwn = true)
}

class TimelineRelayTest {
    @Test
    fun changesTravelBothWays() {
        val p = Devices()
        p.phone.set("hrv 45", "day", "20261110")
        p.phone.set("hrv 47", "day", "20261111")
        p.phone.sync()
        val received = p.mac.sync()
        assertEquals("hrv 45", p.mac.text("day", "20261110"))
        assertEquals(2, received.updates.size)

        p.mac.set("tempo", "plan", "p-1")
        p.mac.sync()
        val back = p.phone.sync()
        assertEquals(listOf(TimelineSlot("plan", "p-1")), back.updates.map { it.slot })
        assertEquals("tempo", p.phone.text("plan", "p-1"))
        // The phone keeps foreign values, not its own.
        assertNull(p.phone.timeline.value(TimelineSlot("day", "20261110")))
        assertNotNull(p.phone.timeline.entry(TimelineSlot("day", "20261110"))!!.digest)
        assertTrue(p.relay.notified.isNotEmpty() && p.relay.notified.all { it == Notify.SILENT })
    }

    @Test
    fun quietOnceBothAreCurrent() {
        val p = Devices()
        p.phone.set("hrv 45", "day", "1")
        p.mac.set("tempo", "plan", "p")
        repeat(3) {
            p.phone.sync()
            p.mac.sync()
        }
        val phoneRun = p.phone.sync()
        val macRun = p.mac.sync()
        assertEquals(0, phoneRun.written)
        assertEquals(0, macRun.written)
        assertTrue(phoneRun.isPeerCurrent && macRun.isPeerCurrent)
        assertEquals("hrv 45", p.mac.text("day", "1"))
        assertEquals("tempo", p.phone.text("plan", "p"))
    }

    @Test
    fun onlyChangesTravel() {
        val p = Devices()
        for (day in 1..30) p.phone.set("day $day", "day", "$day")
        p.phone.sync()
        p.mac.sync()
        // Everything set again, one value different: one change is pending.
        for (day in 1..30) p.phone.set(if (day == 7) "day 7 edited" else "day $day", "day", "$day")
        assertEquals(1, p.phone.timeline.pendingCount)
        p.phone.sync(at = 1_762_855_300)
        val outcome = p.mac.sync()
        assertEquals(listOf("7"), outcome.updates.map { it.slot.key })
        assertEquals("day 7 edited", p.mac.text("day", "7"))
    }

    @Test
    fun concurrentEditsConverge() {
        val p = Devices()
        p.phone.set("easy", "plan", "p-1")
        p.phone.sync(at = 1_762_855_200)
        p.mac.sync(at = 1_762_855_201)
        // Both edit without seeing the other; the later clock wins on both.
        p.phone.set("tempo", "plan", "p-1")
        p.mac.set("intervals", "plan", "p-1")
        p.phone.sync(at = 1_762_855_210)
        p.mac.sync(at = 1_762_855_220)
        p.phone.sync(at = 1_762_855_230)
        assertEquals("intervals", p.mac.text("plan", "p-1"))
        assertEquals("intervals", p.phone.text("plan", "p-1"))
        // A change made after seeing another always wins, whatever the clock says.
        p.phone.set("long run", "plan", "p-1")
        p.phone.sync(at = 1_762_855_000)
        p.mac.sync(at = 1_762_855_240)
        assertEquals("long run", p.mac.text("plan", "p-1"))
    }

    @Test
    fun deletionsWin() {
        val p = Devices()
        p.phone.set("ride", "workout", "w-1")
        p.phone.sync()
        p.mac.sync()
        val slot = TimelineSlot("workout", "w-1")
        assertTrue(p.phone.timeline.delete(slot))
        p.phone.source.remove(slot)
        p.phone.sync(at = 1_762_855_300)
        val outcome = p.mac.sync()
        assertNull(outcome.updates[0].value)
        assertTrue(p.mac.timeline.entry(slot)!!.isDeleted)
    }

    @Test
    fun aReplicaThatLostItsStateCatchesUp() {
        val p = Devices()
        for (day in 1..5) p.phone.set("day $day", "day", "$day")
        p.mac.set("tempo", "plan", "p-1")
        p.phone.sync()
        p.mac.sync()
        p.phone.sync()
        p.mac.sync()
        assertTrue(p.phone.timeline.relay.confirmed >= 1uL)

        // The Mac starts again from nothing, under a new replica id.
        val fresh = Device(Direction.C2P, p.credentials, p.relay, keepsOwn = true)
        fresh.timeline.relay.readThrough = p.mac.timeline.relay.readThrough
        fresh.sync()
        p.phone.sync()
        fresh.sync()
        for (day in 1..5) assertEquals("day $day", fresh.text("day", "$day"))
        // Its own plan comes back from the phone, under the old Mac's stamp.
        assertEquals("tempo", fresh.text("plan", "p-1"))
    }

    @Test
    fun aFullMailboxWaitsForTheReader() {
        val p = Devices(capacity = 2)
        val value = "x".repeat(300_000)
        for (i in 0 until 12) p.phone.set(value + i, "stream", "$i")
        val first = p.phone.sync()
        assertTrue(first.waiting > 0)
        assertEquals(2, p.relay.count(Direction.P2C))
        for (round in 0 until 20) {
            p.mac.sync()
            val outcome = p.phone.sync()
            if (outcome.waiting == 0 && p.phone.timeline.pendingCount == 0) break
        }
        p.mac.sync()
        for (i in 0 until 12) assertEquals(value + i, p.mac.text("stream", "$i"))
    }

    @Test
    fun aBudgetLeavesTheRestPending() {
        val p = Devices()
        val value = "y".repeat(200_000)
        for (i in 0 until 10) p.phone.set(value + i, "stream", "$i")
        p.phone.sync(budget = 500_000)
        assertTrue(p.phone.timeline.pendingCount > 0)
        p.phone.sync()
        p.mac.sync()
        assertEquals(10, p.mac.timeline.entries("stream").size)
    }

    @Test
    fun savedStateReadsBack() {
        val p = Devices(capacity = 1)
        p.phone.set("a", "day", "1")
        p.phone.set("b", "day", "2")
        p.phone.timeline.set(TimelineSlot("day", "3"), "c".toByteArray())
        p.phone.sync()
        p.mac.set("plan", "plan", "p")
        p.mac.sync()
        for (device in listOf(p.phone, p.mac)) {
            val restored = Timeline.decode(device.saved)
            assertContentEquals(device.saved, restored.encoded())
            assertEquals(device.timeline.heads, restored.heads)
            assertEquals(device.timeline.relay, restored.relay)
        }
    }

    @Test
    fun aLongCatchUpSpansRuns() {
        val p = Devices(capacity = 3)
        val value = "z".repeat(250_000)
        for (i in 0 until 10) p.phone.set(value + i, "stream", "$i")
        repeat(12) {
            p.phone.sync()
            p.mac.sync()
        }
        assertEquals(10, p.mac.timeline.entries("stream").size)

        val fresh = Device(Direction.C2P, p.credentials, p.relay, keepsOwn = true)
        fresh.timeline.relay.readThrough = p.mac.timeline.relay.readThrough
        fresh.sync()
        var runs = 0
        while (runs < 12) {
            val outcome = p.phone.sync(budget = 600_000)
            fresh.sync()
            runs += 1
            if (outcome.waiting == 0 && p.phone.timeline.relay.catchUp == null) break
        }
        assertTrue(runs > 1)
        fresh.sync()
        for (i in 0 until 10) assertEquals(value + i, fresh.text("stream", "$i"))
        val own = p.phone.timeline.replica
        assertEquals(p.phone.timeline.heads[own], fresh.timeline.heads[own])
    }

    @Test
    fun aDirectExchangeNeedsNoRelay() {
        val phone = Timeline()
        val mac = Timeline()
        phone.set(TimelineSlot("day", "1"), "hrv".toByteArray())
        mac.set(TimelineSlot("plan", "p"), "tempo".toByteArray())
        val now = 1_762_855_200_000
        phone.commit(now)
        mac.commit(now)
        val phoneHeads = phone.heads
        val macHeads = mac.heads
        for (part in phone.catchUp(macHeads)) mac.receive(part)
        for (part in mac.catchUp(phoneHeads)) phone.receive(part.encoded())
        assertContentEquals("hrv".toByteArray(), mac.value(TimelineSlot("day", "1")))
        assertContentEquals("tempo".toByteArray(), phone.value(TimelineSlot("plan", "p")))
        assertEquals(phone.heads, mac.heads)
    }
}
