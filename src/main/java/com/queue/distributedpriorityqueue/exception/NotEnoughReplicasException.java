package com.queue.distributedpriorityqueue.exception;

import com.queue.distributedpriorityqueue.coordination.ShardId;

/** The shard has fewer in-sync replicas than the queue's minInSyncReplicas, so the enqueue is refused. */
public class NotEnoughReplicasException extends RuntimeException {

    public NotEnoughReplicasException(ShardId shard, int inSync, int required) {
        super("shard " + shard + " has " + inSync + " in-sync replicas and the queue requires " + required);
    }
}
