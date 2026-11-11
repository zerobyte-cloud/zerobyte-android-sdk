package cloud.zerobyte.timeline

import org.json.JSONArray
import org.json.JSONObject
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

// Known answers from the independent reference (zerobyte-protocol, spec/timeline.py),
// copied as timeline.json. Never regenerate them from this library.
class TimelineVectorTest {
    private val v = JSONObject(javaClass.getResource("/timeline.json")!!.readText())

    private fun JSONArray.objects() = (0 until length()).map { getJSONObject(it) }
    private fun JSONArray.strings() = (0 until length()).map { getString(it) }
    private fun JSONObject.u64(key: String) = get(key).toString().toULong()
    private fun bytes(text: String) = hexBytes(text)!!
    private fun replica(text: String) = ReplicaID.fromHex(text)!!

    private val commits: Map<String, TimelineCommit> = v.getJSONArray("commits").objects()
        .associate { it.getString("name") to TimelineCommit.decode(bytes(it.getString("hex"))) }

    private fun commitsOf(vararg names: String) = TimelineMessage.Commits(names.map(commits::getValue))

    private fun code(expected: String, block: () -> Unit) =
        assertEquals(expected, assertFailsWith<TimelineException> { block() }.code)

    @Test
    fun limitsMatchTheVectors() {
        assertEquals(v.getInt("message_limit"), TimelineLimits.MAX_MESSAGE_BYTES)
        assertEquals(v.getInt("commit_limit"), TimelineLimits.MAX_MESSAGE_BYTES - TimelineLimits.COMMITS_MESSAGE_OVERHEAD)
    }

    @Test
    fun theClockFollowsTheVectors() {
        for (item in v.getJSONArray("clock").objects()) {
            assertEquals(item.u64("next"), TimelineClock.next(item.u64("last"), item.getLong("now_ms")))
        }
        assertEquals(ULong.MAX_VALUE, TimelineClock.next(ULong.MAX_VALUE, 0))
        assertEquals(ULong.MAX_VALUE, TimelineClock.next(0uL, 1L shl 48))
        assertEquals(6uL, TimelineClock.next(5uL, -1))
    }

    @Test
    fun commitsEncodeAndDecodeAsTheVectorsSay() {
        for (item in v.getJSONArray("commits").objects()) {
            val name = item.getString("name")
            val built = TimelineCommit.create(
                replica = replica(item.getString("replica")),
                sequence = item.u64("sequence"),
                parent = bytes(item.getString("parent")),
                clock = item.u64("clock"),
                seen = item.getJSONArray("seen").objects().associate { replica(it.getString("replica")) to it.u64("sequence") },
                changes = item.getJSONArray("entries").objects().map {
                    TimelineChange(
                        TimelineSlot(it.getString("collection"), it.getString("key")),
                        if (it.isNull("value")) null else bytes(it.getString("value")),
                    )
                },
            )
            assertEquals(item.getString("hex"), built.bytes.hex(), name)
            assertEquals(item.getString("id"), built.id.hex(), name)
            val decoded = commits.getValue(name)
            assertEquals(built, decoded, name)
            assertContentEquals(built.id, decoded.id)
            assertContentEquals(built.parent, decoded.parent)
            assertEquals(built.sequence, decoded.sequence)
            assertEquals(built.clock, decoded.clock)
            assertEquals(built.seen, decoded.seen)
            assertEquals(built.changes, decoded.changes)
        }
    }

    @Test
    fun messagesRoundTrip() {
        for (item in v.getJSONArray("messages").objects()) {
            val hex = item.getString("hex")
            assertEquals(hex, TimelineMessage.decode(bytes(hex)).encoded().hex(), item.getString("name"))
        }
    }

    private fun stateOf(timeline: Timeline): List<String> {
        val expected = v.getJSONObject("merge").getJSONArray("state").objects()
        return expected.map { it.getString("collection") }.toSortedSet().flatMap { collection ->
            timeline.entries(collection).map { e ->
                listOf(
                    e.slot.collection, e.slot.key, e.stamp.clock, e.stamp.replica.hex, e.stamp.sequence, e.value?.hex(),
                ).joinToString("|")
            }
        }
    }

    @Test
    fun everyOrderReachesTheSameState() {
        val merge = v.getJSONObject("merge")
        val expected = merge.getJSONArray("state").objects().map {
            listOf(
                it.getString("collection"), it.getString("key"), it.u64("clock"), it.getString("replica"),
                it.u64("sequence"), if (it.isNull("value")) null else it.getString("value"),
            ).joinToString("|")
        }
        val orders = merge.getJSONArray("orders").let { a -> (0 until a.length()).map { a.getJSONArray(it).strings() } }
        for (order in orders) {
            val timeline = Timeline()
            for (name in order) assertEquals(1, timeline.receive(commitsOf(name)).merged)
            assertEquals(expected, stateOf(timeline), order.joinToString(","))
            for (h in merge.getJSONArray("heads").objects()) {
                val mine = timeline.heads.getValue(replica(h.getString("replica")))
                assertEquals(h.u64("sequence"), mine.sequence)
                assertEquals(h.u64("clock"), mine.clock)
                assertEquals(h.getString("id"), mine.id.hex())
            }
            assertEquals(merge.u64("last"), timeline.last)
        }
    }

    @Test
    fun aGapIsFoundARepeatSkipped() {
        val gap = Timeline()
        val receipt = gap.receive(commitsOf(v.getJSONObject("gap").getJSONArray("order").getString(0)))
        assertTrue(receipt.foundGap)
        assertEquals(0, receipt.merged)
        assertEquals(0, gap.entryCount)

        val again = Timeline()
        val first = v.getJSONObject("skip").getJSONArray("order").getString(0)
        again.receive(commitsOf(first))
        val repeated = again.receive(commitsOf(first).encoded())
        assertEquals(0, repeated.merged)
        assertFalse(repeated.foundGap)
    }

    @Test
    fun aCatchUpSelectsWhatTheRequesterLacks() {
        val timeline = Timeline()
        for (name in v.getJSONObject("merge").getJSONArray("orders").getJSONArray(0).strings()) {
            timeline.receive(commitsOf(name))
        }
        val catchUp = v.getJSONObject("catch_up")
        val heads = catchUp.getJSONArray("requester_heads").objects().associate {
            replica(it.getString("replica")) to TimelineHead(it.u64("sequence"), 0uL, ByteArray(32))
        }
        val parts = timeline.catchUp(heads)
        assertEquals(1, parts.size)
        assertEquals(catchUp.getString("hex"), parts[0].encoded().hex())
    }

    @Test
    fun everyRejectionVectorIsRefusedWithItsError() {
        for (item in v.getJSONArray("rejections").objects()) {
            val data = bytes(item.getString("hex"))
            val error = assertFailsWith<TimelineException>(item.getString("name")) {
                if (item.getString("kind") == "commit") TimelineCommit.decode(data) else TimelineMessage.decode(data)
            }
            assertEquals(item.getString("error"), error.code, item.getString("name"))
        }
    }

    @Test
    fun aHistoryThatDoesNotContinueIsDiverged() {
        for (item in v.getJSONArray("diverged").objects()) {
            val timeline = Timeline()
            val error = assertFailsWith<TimelineException>(item.getString("name")) {
                for (hex in item.getJSONArray("commits").strings()) {
                    timeline.receive(TimelineMessage.Commits(listOf(TimelineCommit.decode(bytes(hex)))))
                }
            }
            assertEquals("diverged", error.code, item.getString("name"))
        }
    }

    @Test
    fun tooLargeAndInvalidNamesAreRefused() {
        val timeline = Timeline()
        val x = "x".toByteArray()
        code("too_large") { timeline.set(TimelineSlot("stream", "w"), ByteArray(TimelineLimits.MAX_MESSAGE_BYTES)) }
        code("too_large") { TimelineMessage.decode(ByteArray(TimelineLimits.MAX_MESSAGE_BYTES + 1)) }
        code("malformed") { timeline.set(TimelineSlot("", "k"), x) }
        code("malformed") { timeline.set(TimelineSlot("c".repeat(65), "k"), x) }
        code("malformed") { timeline.set(TimelineSlot("day", "\ud800"), x) }
        code("malformed") { timeline.delete(TimelineSlot("day", "")) }
    }

    @Test
    fun namesSortAsUtf8BytesAndKeepALeadingBom() {
        val timeline = Timeline()
        val names = listOf("\ufeffa", "a", "ab", "\u00e9", "\ud83d\ude00", "b")
        for (key in names) timeline.set(TimelineSlot("c", key), key.toByteArray())
        val commit = timeline.commit(1_762_855_200_000).single()
        val decoded = TimelineCommit.decode(commit.bytes)
        val byBytes = names.sortedWith { a, b -> compareBytes(a.toByteArray(), b.toByteArray()) }
        assertEquals(byBytes, decoded.changes.map { it.slot.key })
        assertContentEquals(commit.id, decoded.id)
    }
}
