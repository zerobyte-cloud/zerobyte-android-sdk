package cloud.zerobyte.timeline

/** One key's current entry. */
class TimelineEntry internal constructor(
    val slot: TimelineSlot,
    val stamp: TimelineStamp,
    value: ByteArray?,
    digest: ByteArray?,
) {
    private val bytes = value
    private val digestBytes = digest

    /** Null for a deletion, and for an own value this replica does not keep. */
    val value: ByteArray? get() = bytes?.copyOf()

    val isDeleted: Boolean get() = digestBytes == null

    /** SHA-256 of the value; null for a deletion. */
    val digest: ByteArray? get() = digestBytes?.copyOf()
}

/** A key whose entry changed in a merge: its new value (null: deleted) and stamp. */
class TimelineUpdate internal constructor(val slot: TimelineSlot, value: ByteArray?, val stamp: TimelineStamp) {
    private val bytes = value
    val value: ByteArray? get() = bytes?.copyOf()
}

/** What [Timeline.receive] did. */
class TimelineReceipt internal constructor() {
    internal val updateList = ArrayList<TimelineUpdate>()

    /** The keys whose entries changed. */
    val updates: List<TimelineUpdate> get() = updateList

    /** Commits merged. */
    var merged: Int = 0
        internal set

    /** A commit arrived after a sequence that never did. */
    var foundGap: Boolean = false
        internal set

    /** A state message: what the other side has merged, and what it asks for. */
    var peerState: TimelineMessage.State? = null
        internal set

    /** The highest own sequence the other side said it has. */
    var confirmed: ULong? = null
        internal set
}

/** A message waiting to be written to the relay. */
class TimelineOutboxItem @JvmOverloads constructor(
    val kind: Kind,
    message: ByteArray,
    /** For commits, the last own sequence they carry. */
    val ownThrough: ULong? = null,
) {
    enum class Kind(internal val wire: Int) { COMMITS(1), STATE(2), CATCH_UP(3) }

    internal val raw: ByteArray = message.copyOf()
    val message: ByteArray get() = raw.copyOf()

    override fun equals(other: Any?): Boolean =
        other is TimelineOutboxItem && kind == other.kind && ownThrough == other.ownThrough && raw.contentEquals(other.raw)

    override fun hashCode(): Int = (kind.hashCode() * 31 + ownThrough.hashCode()) * 31 + raw.contentHashCode()
}

/** A catch-up being written a few parts at a time. */
class TimelineCatchUpCursor internal constructor(
    val requesterHeads: Map<ReplicaID, TimelineHead>,
    val includingOwn: Boolean,
    val finalHeads: Map<ReplicaID, TimelineHead>,
    after: TimelineSlot?,
) {
    /** The last slot looked at. */
    var after: TimelineSlot? = after
        internal set

    override fun equals(other: Any?): Boolean =
        other is TimelineCatchUpCursor &&
            requesterHeads == other.requesterHeads &&
            includingOwn == other.includingOwn &&
            finalHeads == other.finalHeads &&
            after == other.after

    override fun hashCode(): Int =
        ((requesterHeads.hashCode() * 31 + includingOwn.hashCode()) * 31 + finalHeads.hashCode()) * 31 + after.hashCode()
}

/** The next parts of a catch-up; `isFinished` once the final part is among them. */
class TimelineCatchUpParts internal constructor(val parts: List<TimelineMessage.CatchUp>, val isFinished: Boolean)

/** What [TimelineRelay] remembers between runs. */
data class TimelineRelayCursor(
    /** The last envelope read from the other direction. */
    var readThrough: ULong? = null,
    var nextWrite: ULong? = null,
    /** Messages not yet on the relay, oldest first. */
    var outbox: MutableList<TimelineOutboxItem> = ArrayList(),
    /** The other side's heads, as it last reported them. */
    var peerHeads: Map<ReplicaID, TimelineHead>? = null,
    /** The highest own sequence the other side confirmed. */
    var confirmed: ULong = 0uL,
    /** A catch-up the other side asked for and this side has not answered. */
    var peerRequest: TimelineRequest = TimelineRequest.NONE,
    var peerRequestHeads: Map<ReplicaID, TimelineHead> = emptyMap(),
    /** A catch-up this side still has to ask for. */
    var ownRequest: TimelineRequest = TimelineRequest.NEW_REPLICA,
    /** A catch-up being written, when it did not fit one run. */
    var catchUp: TimelineCatchUpCursor? = null,
)

/**
 * One replica of a link's keyed state (timeline-v1.md §1–§5). Local changes
 * wait as pending until [commit]; commits and catch-ups from the other side
 * merge by stamp, so both replicas converge whatever order things arrive in.
 * [relay] is the bookkeeping [TimelineRelay] keeps between runs; [encoded]
 * saves it with the rest. Not thread-safe: use one at a time.
 *
 * `forgetsOwnValues` names collections whose own values this replica keeps
 * only as digests, for an app that can produce them again
 * (`TimelineRelay.sync(values = ...)`), so the state does not hold a second
 * copy of its database. `maximumMessageBytes` is a relay's lower plaintext
 * limit for the app.
 */
class Timeline @JvmOverloads constructor(
    val replica: ReplicaID = ReplicaID.random(),
    forgetsOwnValues: Set<String> = emptySet(),
    maximumMessageBytes: Int = TimelineLimits.MAX_MESSAGE_BYTES,
) {
    val forgetsOwnValues: Set<String> = forgetsOwnValues.toSet()
    val maximumMessageBytes: Int = minOf(maximumMessageBytes, TimelineLimits.MAX_MESSAGE_BYTES)

    private class Stored(val slot: TimelineSlot, val stamp: TimelineStamp, val digest: ByteArray?, val value: ByteArray?)
    private class Pending(val value: ByteArray?)

    private var headMap = HashMap<ReplicaID, TimelineHead>()
    private val stored = HashMap<TimelineSlot, Stored>()
    private val pending = HashMap<TimelineSlot, Pending>()

    /** The greatest clock written or merged. */
    var last: ULong = 0uL
        private set

    var relay = TimelineRelayCursor()

    init {
        require(this.forgetsOwnValues.size <= 255) { "forgetsOwnValues: at most 255 collections" }
        for (collection in this.forgetsOwnValues) {
            require(TimelineSlot(collection, "-").isValid) { "forgetsOwnValues: $collection" }
        }
    }

    /** Per replica id, the last commit merged from it. */
    val heads: Map<ReplicaID, TimelineHead> get() = HashMap(headMap)

    private val maxCommitBytes: Int get() = maximumMessageBytes - TimelineLimits.COMMITS_MESSAGE_OVERHEAD

    // Reading

    private fun view(s: Stored) = TimelineEntry(s.slot, s.stamp, s.value, s.digest)

    /** One key's current entry, or null. */
    fun entry(slot: TimelineSlot): TimelineEntry? = stored[slot]?.let(::view)

    /** The current value at a key; null when absent, deleted or not kept. */
    fun value(slot: TimelineSlot): ByteArray? = stored[slot]?.value?.copyOf()

    /** A collection's entries, deletions included, in key order. */
    fun entries(collection: String): List<TimelineEntry> =
        stored.values.filter { it.slot.collection == collection }.sortedBy { it.slot }.map(::view)

    val entryCount: Int get() = stored.size

    // Local changes

    /** Stages a value. Returns false when the key already holds exactly it. */
    fun set(slot: TimelineSlot, value: ByteArray): Boolean {
        if (!slot.isValid) throw malformed()
        if (TimelineLimits.COMMIT_FIXED_BYTES + slot.encodedLength + 5 + value.size > maxCommitBytes) {
            throw TimelineException("too_large")
        }
        val current = stored[slot]
        if (current?.digest != null && current.digest.contentEquals(sha256(value))) {
            pending.remove(slot)
            return false
        }
        val staged = pending[slot]?.value
        if (staged != null && staged.contentEquals(value)) return false
        pending[slot] = Pending(value.copyOf())
        return true
    }

    /** Stages a deletion. Returns false when the key holds nothing. */
    fun delete(slot: TimelineSlot): Boolean {
        if (!slot.isValid) throw malformed()
        val current = stored[slot]
        if (current?.digest == null) {
            pending.remove(slot)
            return false
        }
        pending[slot] = Pending(null)
        return true
    }

    val hasPendingChanges: Boolean get() = pending.isNotEmpty()

    val pendingCount: Int get() = pending.size

    /** Whether a change at this key waits to be committed. */
    fun hasPendingChange(slot: TimelineSlot): Boolean = slot in pending

    val pendingBytes: Long get() = pending.values.sumOf { (it.value?.size ?: 0).toLong() }

    fun discardPendingChanges() = pending.clear()

    /**
     * Turns pending changes into commits, each small enough for one message.
     * With `maximumBytes`, stops once the commits made reach it; the rest stay
     * pending. At least one commit is made when anything is pending.
     */
    @JvmOverloads
    fun commit(now: Long = System.currentTimeMillis(), maximumBytes: Int? = null): List<TimelineCommit> {
        val slots = pending.keys.sorted()
        val commits = ArrayList<TimelineCommit>()
        val batch = ArrayList<TimelineChange>()
        var size = 0
        var total = 0L
        val fixed = TimelineLimits.COMMIT_FIXED_BYTES + headMap.size * 24
        fun flush() {
            if (batch.isEmpty()) return
            val commit = makeCommit(batch.toList(), now)
            commits.add(commit)
            total += commit.raw.size
            batch.clear()
            size = 0
        }
        for (slot in slots) {
            if (maximumBytes != null && commits.isNotEmpty() && total >= maximumBytes) break
            val change = TimelineChange(slot, pending.getValue(slot).value)
            val length = change.encodedLength
            if (batch.isNotEmpty() && fixed + size + length > maxCommitBytes) {
                flush()
                if (maximumBytes != null && total >= maximumBytes) break
            }
            batch.add(change)
            size += length
            pending.remove(slot)
        }
        flush()
        return commits
    }

    private fun makeCommit(changes: List<TimelineChange>, now: Long): TimelineCommit {
        val own = headMap[replica]
        val seen = HashMap<ReplicaID, ULong>()
        for ((other, head) in headMap) if (other != replica) seen[other] = head.sequence
        val commit = TimelineCommit.create(
            replica = replica,
            sequence = (own?.sequence ?: 0uL) + 1uL,
            parent = own?.id ?: ZERO_ID,
            clock = TimelineClock.next(last, now),
            seen = seen,
            changes = changes,
        )
        apply(commit)
        return commit
    }

    // Merging

    /** Merges a message as bytes. */
    fun receive(data: ByteArray): TimelineReceipt = receive(TimelineMessage.decode(data, maximumMessageBytes))

    /** Merges a message. */
    fun receive(message: TimelineMessage): TimelineReceipt {
        val receipt = TimelineReceipt()
        when (message) {
            is TimelineMessage.Commits -> for (commit in message.commits) {
                when (val outcome = merge(commit)) {
                    Merge.Skipped -> {}
                    Merge.Gap -> receipt.foundGap = true
                    is Merge.Applied -> {
                        receipt.updateList.addAll(outcome.updates)
                        receipt.merged += 1
                        val own = commit.seen[replica]
                        val confirmed = receipt.confirmed
                        if (own != null && (confirmed == null || own > confirmed)) receipt.confirmed = own
                    }
                }
            }
            is TimelineMessage.State -> {
                receipt.peerState = message
                message.heads[replica]?.let { receipt.confirmed = it.sequence }
            }
            is TimelineMessage.CatchUp -> {
                for (item in message.entries) {
                    if (item.stamp.clock > last) last = item.stamp.clock
                    place(item.slot, item.stamp, item.raw)?.let(receipt.updateList::add)
                }
                if (message.isFinal) {
                    for ((other, head) in message.heads) {
                        if ((headMap[other]?.sequence ?: 0uL) < head.sequence) {
                            headMap[other] = head
                            if (head.clock > last) last = head.clock
                        }
                    }
                }
            }
        }
        return receipt
    }

    private sealed class Merge {
        object Skipped : Merge()
        object Gap : Merge()
        class Applied(val updates: List<TimelineUpdate>) : Merge()
    }

    private fun merge(commit: TimelineCommit): Merge {
        val head = headMap[commit.replica]
        if (head != null && commit.sequence <= head.sequence) {
            if (commit.sequence == head.sequence && !head.sameId(commit.rawId)) throw TimelineException("diverged")
            return Merge.Skipped
        }
        if (commit.sequence != (head?.sequence ?: 0uL) + 1uL) return Merge.Gap
        if (head != null && (!head.sameId(commit.rawParent) || commit.clock <= head.clock)) {
            throw TimelineException("diverged")
        }
        return Merge.Applied(apply(commit))
    }

    private fun apply(commit: TimelineCommit): List<TimelineUpdate> {
        val updates = ArrayList<TimelineUpdate>()
        val stamp = TimelineStamp(commit.clock, commit.replica, commit.sequence)
        for (change in commit.changes) place(change.slot, stamp, change.raw)?.let(updates::add)
        headMap[commit.replica] = TimelineHead(commit.sequence, commit.clock, commit.rawId)
        if (commit.clock > last) last = commit.clock
        return updates
    }

    // `value` is never written to: it is shared with the commit or message it came in.
    private fun place(slot: TimelineSlot, stamp: TimelineStamp, value: ByteArray?): TimelineUpdate? {
        val current = stored[slot]
        if (current != null && current.stamp >= stamp) return null
        val keeps = stamp.replica != replica || slot.collection !in forgetsOwnValues
        stored[slot] = Stored(slot, stamp, value?.let(::sha256), if (keeps) value else null)
        val staged = pending[slot]?.value
        if (staged != null && value != null && staged.contentEquals(value)) pending.remove(slot)
        return TimelineUpdate(slot, value, stamp)
    }

    // Telling and catching up

    /** What this replica has merged, for the other side. */
    @JvmOverloads
    fun state(request: TimelineRequest = TimelineRequest.NONE): TimelineMessage.State =
        TimelineMessage.State(request, HashMap(headMap))

    /**
     * Every entry after the requester's heads (timeline-v1.md §5), in parts
     * that each fit a message; the last part is final and carries the heads.
     * `includingOwn = false` leaves out this replica's own entries and head:
     * over the relay, when they are still on their way. `values` produces an
     * own value this replica does not keep; one whose digest is not the
     * entry's is left out.
     */
    @JvmOverloads
    fun catchUp(
        requesterHeads: Map<ReplicaID, TimelineHead>,
        includingOwn: Boolean = true,
        values: (TimelineSlot) -> ByteArray? = { null },
    ): List<TimelineMessage.CatchUp> =
        continueCatchUp(beginCatchUp(requesterHeads, includingOwn), Long.MAX_VALUE, values).parts

    /**
     * Starts a catch-up to be written a few parts at a time. Its final part
     * carries the heads of now, so commits made while it is written are never
     * taken as covered by it.
     */
    @JvmOverloads
    fun beginCatchUp(requesterHeads: Map<ReplicaID, TimelineHead>, includingOwn: Boolean = true): TimelineCatchUpCursor {
        val finalHeads = HashMap(headMap)
        if (!includingOwn) finalHeads.remove(replica)
        return TimelineCatchUpCursor(HashMap(requesterHeads), includingOwn, finalHeads, null)
    }

    /**
     * The next parts of a catch-up, about `maximumBytes` of them; the call
     * that reaches the end adds the final part. Advances `cursor`.
     */
    @JvmOverloads
    fun continueCatchUp(
        cursor: TimelineCatchUpCursor,
        maximumBytes: Long,
        values: (TimelineSlot) -> ByteArray? = { null },
    ): TimelineCatchUpParts {
        fun part(entries: List<TimelineStampedEntry>) = TimelineMessage.CatchUp(false, emptyMap(), entries)
        val parts = ArrayList<TimelineMessage.CatchUp>()
        var batch = ArrayList<TimelineStampedEntry>()
        var size = TimelineLimits.CATCH_UP_PART_OVERHEAD
        var produced = 0L
        val start = cursor.after
        val slots = stored.keys.filter { start == null || it > start }.sorted()
        for (slot in slots) {
            if (produced >= maximumBytes) {
                if (batch.isNotEmpty()) parts.add(part(batch))
                return TimelineCatchUpParts(parts, false)
            }
            val entry = stored.getValue(slot)
            cursor.after = slot
            val origin = entry.stamp.replica
            if (origin == replica && !cursor.includingOwn) continue
            val seen = cursor.requesterHeads[origin]?.sequence
            if (seen != null && entry.stamp.sequence <= seen) continue
            var value: ByteArray? = null
            if (entry.digest != null) {
                val found = entry.value ?: values(slot)
                if (found == null || !sha256(found).contentEquals(entry.digest)) continue
                value = found
            }
            val item = TimelineStampedEntry(slot, entry.stamp, value)
            val length = item.encodedLength
            if (batch.isNotEmpty() && size + length > maximumMessageBytes) {
                parts.add(part(batch))
                batch = ArrayList()
                size = TimelineLimits.CATCH_UP_PART_OVERHEAD
            }
            batch.add(item)
            size += length
            produced += length
        }
        if (batch.isNotEmpty() && size + cursor.finalHeads.size * 64 > maximumMessageBytes) {
            parts.add(part(batch))
            batch = ArrayList()
        }
        parts.add(TimelineMessage.CatchUp(true, HashMap(cursor.finalHeads), batch))
        return TimelineCatchUpParts(parts, true)
    }

    /**
     * Records that the other side has this replica's commits through
     * `sequence`, and drops queued commit messages it already holds.
     */
    fun noteConfirmed(sequence: ULong) {
        if (sequence > relay.confirmed) relay.confirmed = sequence
        val confirmed = relay.confirmed
        relay.outbox.removeAll { item -> item.ownThrough.let { it != null && it <= confirmed } }
    }

    // At rest

    /** Everything, for the app to save as one file. Not a wire format. */
    fun encoded(): ByteArray {
        val w = TimelineWriter()
        w.bytes(MAGIC)
        w.byte(1)
        w.bytes(replica.raw)
        w.uint64(last)
        val forgets = forgetsOwnValues.map { strictUtf8(it)!! }.sortedWith(::compareBytes)
        w.byte(forgets.size)
        for (collection in forgets) {
            w.byte(collection.size)
            w.bytes(collection)
        }
        w.uint32(maximumMessageBytes)
        w.heads(headMap)
        val slots = stored.keys.sorted()
        w.uint32(slots.size)
        for (slot in slots) {
            val entry = stored.getValue(slot)
            w.slot(slot)
            w.uint64(entry.stamp.clock)
            w.bytes(entry.stamp.replica.raw)
            w.uint64(entry.stamp.sequence)
            when {
                entry.digest == null -> w.byte(0)
                entry.value != null -> {
                    w.byte(1)
                    w.bytes(entry.digest)
                    w.uint32(entry.value.size)
                    w.bytes(entry.value)
                }
                else -> {
                    w.byte(2)
                    w.bytes(entry.digest)
                }
            }
        }
        val pendingSlots = pending.keys.sorted()
        w.uint32(pendingSlots.size)
        for (slot in pendingSlots) {
            w.slot(slot)
            w.value(pending.getValue(slot).value)
        }
        w.uint64(relay.readThrough ?: 0uL)
        w.uint64(relay.nextWrite ?: 0uL)
        w.uint64(relay.confirmed)
        w.uint32(relay.outbox.size)
        for (item in relay.outbox) {
            w.byte(item.kind.wire)
            w.uint64(item.ownThrough ?: 0uL)
            w.uint32(item.raw.size)
            w.bytes(item.raw)
        }
        w.byte(if (relay.peerHeads == null) 0 else 1)
        w.heads(relay.peerHeads ?: emptyMap())
        w.byte(relay.peerRequest.rawValue)
        w.heads(relay.peerRequestHeads)
        w.byte(relay.ownRequest.rawValue)
        val cursor = relay.catchUp
        if (cursor == null) {
            w.byte(0)
        } else {
            w.byte(if (cursor.includingOwn) 3 else 1)
            w.heads(cursor.requesterHeads)
            w.heads(cursor.finalHeads)
            val after = cursor.after
            if (after == null) {
                w.byte(0)
            } else {
                w.byte(1)
                w.slot(after)
            }
        }
        return w.finish()
    }

    companion object {
        private val MAGIC = byteArrayOf(0x5a, 0x42, 0x54, 0x4c) // "ZBTL"

        /** A timeline saved with [encoded]. */
        @JvmStatic
        fun decode(data: ByteArray): Timeline {
            val r = TimelineReader(data)
            if (!r.take(4).contentEquals(MAGIC) || r.byte() != 1) throw malformed()
            val replica = r.replica()
            val last = r.uint64()
            val forgets = LinkedHashSet<String>()
            repeat(r.byte()) { forgets.add(strictString(r.take(r.byte())) ?: throw malformed()) }
            val timeline = try {
                Timeline(replica, forgets, r.length())
            } catch (_: IllegalArgumentException) {
                throw malformed()
            }
            timeline.last = last
            timeline.headMap = HashMap(r.heads())
            for (i in 0 until r.uint32()) {
                val slot = r.slot()
                val clock = r.uint64()
                val origin = r.replica()
                val stamp = TimelineStamp(clock, origin, r.uint64())
                timeline.stored[slot] = when (r.byte()) {
                    0 -> Stored(slot, stamp, null, null)
                    1 -> {
                        val digest = r.take(32)
                        Stored(slot, stamp, digest, r.take(r.length()))
                    }
                    2 -> Stored(slot, stamp, r.take(32), null)
                    else -> throw malformed()
                }
            }
            for (i in 0 until r.uint32()) {
                val slot = r.slot()
                timeline.pending[slot] = Pending(r.value())
            }
            val relay = timeline.relay
            relay.readThrough = r.uint64().takeIf { it != 0uL }
            relay.nextWrite = r.uint64().takeIf { it != 0uL }
            relay.confirmed = r.uint64()
            for (i in 0 until r.uint32()) {
                val wire = r.byte()
                val kind = TimelineOutboxItem.Kind.entries.firstOrNull { it.wire == wire } ?: throw malformed()
                val through = r.uint64()
                val message = r.take(r.length())
                relay.outbox.add(TimelineOutboxItem(kind, message, through.takeIf { it != 0uL }))
            }
            val hasPeerHeads = r.byte() == 1
            val peerHeads = r.heads()
            relay.peerHeads = if (hasPeerHeads) peerHeads else null
            relay.peerRequest = TimelineRequest(r.byte())
            relay.peerRequestHeads = r.heads()
            relay.ownRequest = TimelineRequest(r.byte())
            val catchUp = r.byte()
            if (catchUp != 0) {
                val requesterHeads = r.heads()
                val finalHeads = r.heads()
                val after = if (r.byte() == 1) r.slot() else null
                relay.catchUp = TimelineCatchUpCursor(requesterHeads, catchUp == 3, finalHeads, after)
            }
            if (!r.isAtEnd) throw malformed()
            return timeline
        }
    }
}
