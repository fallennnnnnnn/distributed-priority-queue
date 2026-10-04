package com.queue.distributedpriorityqueue.log;

import com.queue.distributedpriorityqueue.core.ShardEvent;
import java.util.List;
import java.util.Optional;

/**
 * The ordered events of one shard. Offsets start at 0 and have no gaps. Entries below the high
 * watermark are committed and are never removed.
 */
public interface ShardLog {

    /** Adds an event at the end and returns its offset. Epochs never go backwards. */
    long append(int epoch, ShardEvent event);

    /** Up to {@code maxEntries} entries starting at {@code fromOffset}; empty at or past the end. */
    List<LogEntry> read(long fromOffset, int maxEntries);

    /** The offset the next entry will get. */
    long endOffset();

    /** Drops every entry at {@code toOffset} and beyond. Committed entries cannot be dropped. */
    void truncate(long toOffset);

    /** The offset of the first entry that is not yet committed. */
    long highWatermark();

    /** Moves the high watermark forward, up to the end of the log at most. */
    void advanceHighWatermark(long highWatermark);

    /**
     * Where the entries of {@code epoch} end in this log, used to find where two logs last agree.
     * If this log has no entries from that epoch, answers for the closest earlier epoch it does
     * have. Empty when the log has nothing from that epoch or any earlier one.
     */
    Optional<EpochEnd> endOfEpoch(int epoch);

    /** {@code endOffset} is the offset just past the last entry written in {@code epoch}. */
    record EpochEnd(int epoch, long endOffset) {
    }
}
