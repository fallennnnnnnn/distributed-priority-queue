package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.ShardState;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import java.util.List;

/** A queue as the coordinator describes it: its settings and the state of each shard, by shard number. */
public record QueueMetadata(QueueConfig config, long version, List<ShardState> shards) {

    public QueueMetadata {
        shards = List.copyOf(shards);
    }
}
