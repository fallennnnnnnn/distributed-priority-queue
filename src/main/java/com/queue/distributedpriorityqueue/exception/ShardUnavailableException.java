package com.queue.distributedpriorityqueue.exception;

import com.queue.distributedpriorityqueue.coordination.ShardId;

/** The shard has no leader this broker can reach right now. The caller may retry. */
public class ShardUnavailableException extends RuntimeException {

    public ShardUnavailableException(ShardId shard) {
        super("shard " + shard + " is unavailable");
    }
}
