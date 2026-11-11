package cloud.zerobyte.timeline

import cloud.zerobyte.Credentials
import cloud.zerobyte.Direction
import cloud.zerobyte.Envelope
import cloud.zerobyte.Notify
import cloud.zerobyte.RelayException
import cloud.zerobyte.RelayTransport
import cloud.zerobyte.ZeroByteException
import java.io.IOException

/** What one [TimelineRelay.sync] run did. */
class TimelineSyncOutcome internal constructor() {
    internal val updateList = ArrayList<TimelineUpdate>()

    /** The keys whose entries changed, in the order merged. */
    val updates: List<TimelineUpdate> get() = updateList

    /** Commits merged. */
    var merged: Int = 0
        internal set

    /** Envelopes written. */
    var written: Int = 0
        internal set

    /** Messages still waiting for the relay (a full mailbox): the next run writes them. */
    var waiting: Int = 0
        internal set

    /** The other side's heads, as it last reported them. */
    var peerHeads: Map<ReplicaID, TimelineHead>? = null
        internal set

    /** Whether the other side confirmed every own commit. */
    var isPeerCurrent: Boolean = false
        internal set
}

/**
 * One run of a replica over the relay (timeline-v1.md §6): read and merge
 * what the other side wrote, commit what is pending, answer a catch-up
 * request, write. Every step is safe to repeat: a message written twice
 * merges once, so a run cut short anywhere only costs a repeat. Blocking:
 * call it off the main thread, one run per timeline at a time.
 */
object TimelineRelay {
    /** About how much of a catch-up one run writes when no budget is given. */
    const val CATCH_UP_BUDGET: Long = 8L * 1_048_576

    /**
     * `sends` is [Direction.P2C] on the primary, [Direction.C2P] on the
     * companion. `commitBudget` commits at most about this many bytes of
     * pending changes this run; the rest wait for the next. `notify` is
     * asked of the relay with the run's last envelope. `values` produces own
     * values the timeline does not keep, for a catch-up (`forgetsOwnValues`).
     * `persist` saves the timeline: it is called after reading, before the
     * relay is pruned, and after writing.
     */
    @JvmStatic
    @JvmOverloads
    fun sync(
        timeline: Timeline,
        credentials: Credentials,
        transport: RelayTransport,
        sends: Direction,
        now: Long = System.currentTimeMillis(),
        commitBudget: Int? = null,
        notify: Notify = Notify.NONE,
        values: (TimelineSlot) -> ByteArray? = { null },
        persist: (Timeline) -> Unit,
    ): TimelineSyncOutcome {
        transport.claim()
        val outcome = TimelineSyncOutcome()
        val reads = sends.reversed
        val relay = timeline.relay

        // 1. Read and merge.
        val started = relay.readThrough
        val headsBefore = timeline.heads
        var fetched = started
        var after = started
        while (true) {
            val page = transport.fetch(reads, fetched)
            val before = fetched
            for (envelope in page.envelopes) {
                try {
                    val sequence = Envelope.readHeader(envelope).second
                    if (fetched == null || sequence > fetched) fetched = sequence
                } catch (_: ZeroByteException) {
                    // No header: nothing to note.
                }
                val opened = try {
                    Envelope.open(envelope, reads, after, credentials)
                } catch (_: ZeroByteException) {
                    continue // Not written by the other side under the link key: never applied.
                }
                after = opened.sequence
                val message = try {
                    TimelineMessage.decode(opened.plaintext, timeline.maximumMessageBytes)
                } catch (e: TimelineException) {
                    // A newer peer: stop here, unread, until this app is updated.
                    if (e.code == "unsupported_version") throw e
                    continue
                }
                val receipt = timeline.receive(message)
                outcome.updateList.addAll(receipt.updates)
                outcome.merged += receipt.merged
                if (receipt.foundGap) relay.ownRequest += TimelineRequest.GAP
                receipt.confirmed?.let(timeline::noteConfirmed)
                receipt.peerState?.let { state ->
                    relay.peerHeads = state.heads
                    if (!state.request.isEmpty) {
                        relay.peerRequest += state.request
                        relay.peerRequestHeads = state.heads
                    }
                }
                relay.readThrough = opened.sequence
            }
            if (!page.more || fetched == before) break
        }
        val newest = fetched
        if (newest != null && newest != started) {
            val through = relay.readThrough
            if (through == null || newest > through) relay.readThrough = newest
            persist(timeline)
            try {
                transport.prune(reads, newest)
            } catch (_: IOException) {
                // The next run prunes again.
            }
        }

        // 2. Commit, answer, tell.
        var queued = false
        val commits = timeline.commit(now, commitBudget)
        for (item in pack(commits, timeline.maximumMessageBytes)) {
            relay.outbox.add(item)
            queued = true
        }
        if (!relay.peerRequest.isEmpty) {
            val request = relay.peerRequest
            val heads = relay.peerRequestHeads
            val theirs = heads[timeline.replica]?.sequence ?: 0uL
            // A gap, or a head that went back: the other side lost what was sent.
            val lost = TimelineRequest.GAP in request || theirs < relay.confirmed
            // A newer request replaces a catch-up still being written.
            relay.outbox.removeAll { it.kind == TimelineOutboxItem.Kind.CATCH_UP }
            relay.catchUp = timeline.beginCatchUp(heads, lost)
            relay.peerRequest = TimelineRequest.NONE
            relay.peerRequestHeads = emptyMap()
            queued = true
        }
        val cursor = relay.catchUp
        if (cursor != null && relay.outbox.none { it.kind == TimelineOutboxItem.Kind.CATCH_UP }) {
            val next = timeline.continueCatchUp(cursor, commitBudget?.toLong() ?: CATCH_UP_BUDGET, values)
            // Nothing the requester lacks: no answer at all.
            val only = next.parts.singleOrNull()
            val isEmpty = only != null &&
                only.entries.isEmpty() &&
                only.heads.all { (replica, head) -> (cursor.requesterHeads[replica]?.sequence ?: 0uL) >= head.sequence }
            if (!(next.isFinished && isEmpty)) {
                for (part in next.parts) relay.outbox.add(TimelineOutboxItem(TimelineOutboxItem.Kind.CATCH_UP, part.encoded()))
            }
            relay.catchUp = if (next.isFinished) null else cursor
            queued = true
        }
        val merged = outcome.merged > 0 || outcome.updates.isNotEmpty() || timeline.heads != headsBefore
        if (!relay.ownRequest.isEmpty || (merged && commits.isEmpty())) {
            var request = relay.ownRequest
            val items = relay.outbox.iterator()
            while (items.hasNext()) {
                val item = items.next()
                if (item.kind != TimelineOutboxItem.Kind.STATE) continue
                try {
                    val earlier = TimelineMessage.decode(item.raw)
                    if (earlier is TimelineMessage.State) request += earlier.request
                } catch (_: TimelineException) {
                    // Written by this replica: it decodes.
                }
                items.remove()
            }
            relay.outbox.add(TimelineOutboxItem(TimelineOutboxItem.Kind.STATE, timeline.state(request).encoded()))
            relay.ownRequest = TimelineRequest.NONE
            queued = true
        }
        if (queued) persist(timeline)

        // 3. Write.
        if (relay.outbox.isNotEmpty()) {
            val last = transport.lastSequence(sends) ?: 0uL
            var sequence = maxOf(relay.nextWrite ?: 1uL, last + 1uL)
            var conflicts = 0
            while (relay.outbox.isNotEmpty()) {
                val item = relay.outbox[0]
                val envelope = Envelope.seal(item.raw, sends, sequence, credentials)
                try {
                    transport.put(envelope, sends, sequence, if (relay.outbox.size == 1) notify else Notify.NONE)
                } catch (e: RelayException) {
                    when (e.code) {
                        "sequence_conflict" -> {
                            conflicts += 1
                            if (conflicts > 3) throw e
                            sequence = (e.last ?: sequence) + 1uL
                            continue
                        }
                        "quota_exceeded" -> break
                        else -> throw e
                    }
                }
                relay.outbox.removeAt(0)
                sequence += 1uL
                relay.nextWrite = sequence
                outcome.written += 1
            }
            if (outcome.written > 0) persist(timeline)
        }

        outcome.waiting = relay.outbox.size
        outcome.peerHeads = relay.peerHeads
        outcome.isPeerCurrent = relay.confirmed >= (timeline.heads[timeline.replica]?.sequence ?: 0uL)
        return outcome
    }

    /**
     * Queues commits made outside a relay run, on a direct channel for
     * example, so the relay still carries them if the other side did not
     * confirm them there ([Timeline.noteConfirmed] drops them once it does).
     */
    @JvmStatic
    fun enqueue(commits: List<TimelineCommit>, timeline: Timeline) {
        timeline.relay.outbox.addAll(pack(commits, timeline.maximumMessageBytes))
    }

    // Commits in as few messages as fit, in order.
    private fun pack(commits: List<TimelineCommit>, limit: Int): List<TimelineOutboxItem> {
        val items = ArrayList<TimelineOutboxItem>()
        var batch = ArrayList<TimelineCommit>()
        var size = 3
        fun flush() {
            if (batch.isEmpty()) return
            items.add(
                TimelineOutboxItem(
                    TimelineOutboxItem.Kind.COMMITS,
                    TimelineMessage.Commits(batch).encoded(),
                    batch.last().sequence,
                ),
            )
            batch = ArrayList()
            size = 3
        }
        for (commit in commits) {
            if (batch.isNotEmpty() && (size + 4 + commit.raw.size > limit || batch.size == 0xffff)) flush()
            batch.add(commit)
            size += 4 + commit.raw.size
        }
        flush()
        return items
    }
}
