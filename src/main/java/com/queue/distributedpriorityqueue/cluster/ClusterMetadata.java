package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.Coordinator;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.coordination.ShardState;
import com.queue.distributedpriorityqueue.coordination.Versioned;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * This broker's cached copy of the coordinator's queue metadata, so that requests do not call the
 * coordinator. The cache is refreshed when the coordinator reports a change, and a queue that is
 * not in the cache is looked up once, which covers a change report that has not arrived yet.
 */
final class ClusterMetadata {

    private final Coordinator coordinator;
    private final Map<String, QueueMetadata> queues = new ConcurrentHashMap<>();
    private volatile Consumer<QueueMetadata> onRefresh = metadata -> { };

    ClusterMetadata(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    /** Runs after every refresh with the newest metadata known for that queue. */
    void onRefresh(Consumer<QueueMetadata> listener) {
        this.onRefresh = listener;
    }

    Optional<QueueMetadata> find(String queue) {
        QueueMetadata cached = queues.get(queue);
        return cached != null ? Optional.of(cached) : refresh(queue);
    }

    Optional<QueueMetadata> refresh(String queue) {
        Optional<Versioned<QueueConfig>> settings = coordinator.queue(queue);
        if (settings.isEmpty()) {
            return Optional.empty();
        }
        List<Versioned<ShardState>> shards = new ArrayList<>();
        for (int shard = 0; shard < settings.get().value().shards(); shard++) {
            Optional<Versioned<ShardState>> state = coordinator.shard(new ShardId(queue, shard));
            if (state.isEmpty()) {
                return Optional.empty();
            }
            shards.add(state.get());
        }
        QueueMetadata read = new QueueMetadata(settings.get(), shards);
        // Two refreshes can finish out of order; keep whichever saw the later changes.
        QueueMetadata newest = queues.merge(queue, read,
                (cached, fresh) -> fresh.revision() >= cached.revision() ? fresh : cached);
        onRefresh.accept(newest);
        return Optional.of(newest);
    }
}
