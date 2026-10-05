package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.ShardState;
import com.queue.distributedpriorityqueue.coordination.Versioned;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import java.util.List;

/** A queue as the coordinator describes it: its settings and the state of each shard, by shard number. */
public record QueueMetadata(Versioned<QueueConfig> settings, List<Versioned<ShardState>> shards) {

    public QueueMetadata {
        shards = List.copyOf(shards);
    }

    public QueueConfig config() {
        return settings.value();
    }

    public ShardState shard(int number) {
        return shards.get(number).value();
    }

    /** Grows with every change to the queue's settings or to any of its shards. */
    public long revision() {
        long revision = settings.version();
        for (Versioned<ShardState> shard : shards) {
            revision = Math.max(revision, shard.version());
        }
        return revision;
    }
}
