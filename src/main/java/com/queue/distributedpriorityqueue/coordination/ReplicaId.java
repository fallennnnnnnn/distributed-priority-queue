package com.queue.distributedpriorityqueue.coordination;

/**
 * A broker process that holds a copy of a shard. The incarnation changes every time the broker
 * process starts, so a restarted broker, whose log is empty, is not mistaken for the replica it
 * was before.
 */
public record ReplicaId(String brokerId, String incarnation) {
}
