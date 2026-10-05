package com.queue.distributedpriorityqueue.replication;

import com.queue.distributedpriorityqueue.log.LogEntry;
import java.util.List;

/**
 * The leader's side of a fetch stream, as the follower sees it. Calls arrive in order. They are
 * made while the leader holds its shard lock, so an implementation must hand the data off and
 * return without blocking.
 */
public interface FetchListener {

    /** New entries, possibly none, and the leader's current high watermark. */
    void onEntries(List<LogEntry> entries, long highWatermark);

    /** The stream is over; nothing more will arrive on it. */
    void onEnd(FetchEnd end);
}
