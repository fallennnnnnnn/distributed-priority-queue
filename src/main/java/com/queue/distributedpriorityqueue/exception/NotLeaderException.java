package com.queue.distributedpriorityqueue.exception;

import com.queue.distributedpriorityqueue.coordination.ShardId;

/** The replica asked to serve a shard does not lead it. The caller should look up the leader again. */
public class NotLeaderException extends RuntimeException {

    /** For an error that another broker reported in these words. */
    public NotLeaderException(String message) {
        super(message);
    }

    public NotLeaderException(ShardId shard) {
        super("this broker does not lead shard " + shard);
    }
}
