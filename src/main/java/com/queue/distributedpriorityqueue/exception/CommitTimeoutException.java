package com.queue.distributedpriorityqueue.exception;

import com.queue.distributedpriorityqueue.coordination.ShardId;

/**
 * The in-sync replicas did not all confirm the write in time. The write is still in the leader's
 * log and may be committed later, so a caller that retries can cause a duplicate.
 */
public class CommitTimeoutException extends RuntimeException {

    /** For an error that another broker reported in these words. */
    public CommitTimeoutException(String message) {
        super(message);
    }

    public CommitTimeoutException(ShardId shard) {
        super("shard " + shard + " did not replicate the write to its in-sync replicas in time");
    }
}
