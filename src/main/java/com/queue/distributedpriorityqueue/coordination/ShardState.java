package com.queue.distributedpriorityqueue.coordination;

import java.util.List;

/**
 * Who leads a shard and who may take over.
 *
 * @param leader the replica serving the shard, or null when it has none
 * @param epoch goes up by one with every new leader; requests from an older epoch are rejected
 * @param isr the in-sync replicas: those that have every committed event
 * @param replicas the IDs of the brokers assigned to hold this shard
 */
public record ShardState(ReplicaId leader, int epoch, List<ReplicaId> isr, List<String> replicas) {

    public ShardState {
        isr = List.copyOf(isr);
        replicas = List.copyOf(replicas);
    }
}
