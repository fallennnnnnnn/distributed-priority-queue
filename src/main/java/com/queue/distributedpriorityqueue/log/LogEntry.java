package com.queue.distributedpriorityqueue.log;

import com.queue.distributedpriorityqueue.core.ShardEvent;

/** One event in a shard's log, with the epoch of the leader that wrote it. */
public record LogEntry(long offset, int epoch, ShardEvent event) {
}
