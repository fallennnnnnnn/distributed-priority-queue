package com.queue.distributedpriorityqueue.exception;

import com.queue.distributedpriorityqueue.coordination.ShardId;

/** The shard has no leader this broker can reach right now. The caller may retry. */
public class ShardUnavailableException extends RuntimeException {

    /** For an error that another broker reported in these words. */
    public ShardUnavailableException(String message) {
        super(message);
    }

    public ShardUnavailableException(ShardId shard) {
        super("shard " + shard + " is unavailable");
    }
}
