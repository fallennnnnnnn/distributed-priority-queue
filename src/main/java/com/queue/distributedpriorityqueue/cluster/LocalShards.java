package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.log.InMemoryShardLog;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/** The shard replicas this broker holds, created from the metadata the coordinator publishes. */
final class LocalShards {

    private final String brokerId;
    private final Clock clock;
    private final Map<ShardId, ShardReplica> replicas = new ConcurrentHashMap<>();
    private final List<Consumer<ShardReplica>> listeners = new ArrayList<>();

    LocalShards(String brokerId, Clock clock) {
        this.brokerId = brokerId;
        this.clock = clock;
    }

    /** Creates replicas for the shards assigned to this broker and hands new settings to existing ones. */
    void sync(QueueMetadata metadata) {
        QueueConfig config = metadata.config();
        for (int shard = 0; shard < metadata.shards().size(); shard++) {
            if (!metadata.shards().get(shard).replicas().contains(brokerId)) {
                continue;
            }
            int number = shard;
            ShardReplica replica = replicas.computeIfAbsent(new ShardId(config.name(), shard), id -> {
                ShardReplica created = new ShardReplica(number, config, new InMemoryShardLog(), clock);
                announce(created);
                return created;
            });
            replica.updateConfig(config);
        }
    }

    Optional<ShardReplica> find(ShardId id) {
        return Optional.ofNullable(replicas.get(id));
    }

    Collection<ShardReplica> all() {
        return replicas.values();
    }

    /** Calls the listener for every replica that exists now and every one created later. */
    synchronized void onCreated(Consumer<ShardReplica> listener) {
        listeners.add(listener);
        replicas.values().forEach(listener);
    }

    private synchronized void announce(ShardReplica replica) {
        listeners.forEach(listener -> listener.accept(replica));
    }
}
