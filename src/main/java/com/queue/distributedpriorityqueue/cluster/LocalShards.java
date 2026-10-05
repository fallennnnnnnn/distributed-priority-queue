package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.coordination.ShardState;
import com.queue.distributedpriorityqueue.coordination.Versioned;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.log.InMemoryShardLog;
import com.queue.distributedpriorityqueue.replication.FollowerFetcher;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The shard replicas this broker holds. They are created from the metadata the coordinator
 * publishes, which also says whether each one leads its shard or follows another broker, and
 * they change role whenever that metadata names a new leader.
 */
final class LocalShards {

    private final ReplicaId self;
    private final Clock clock;
    private final Duration replicaLagMax;
    private final Duration commitTimeout;
    /** Builds, but does not start, the fetcher that keeps a follower replica supplied by its leader. */
    private final Function<ShardReplica, FollowerFetcher> fetcherFactory;
    private final Map<ShardId, ShardReplica> replicas = new ConcurrentHashMap<>();
    private final Map<ShardId, FollowerFetcher> fetchers = new ConcurrentHashMap<>();
    private final List<Consumer<ShardReplica>> onLeading = new ArrayList<>();
    private final List<Consumer<ShardReplica>> onNoLongerLeading = new ArrayList<>();

    LocalShards(ReplicaId self, Clock clock, Duration replicaLagMax, Duration commitTimeout,
            Function<ShardReplica, FollowerFetcher> fetcherFactory) {
        this.self = self;
        this.clock = clock;
        this.replicaLagMax = replicaLagMax;
        this.commitTimeout = commitTimeout;
        this.fetcherFactory = fetcherFactory;
    }

    /** Creates replicas for the shards assigned to this broker, and gives each its settings and its role. */
    synchronized void sync(QueueMetadata metadata) {
        QueueConfig config = metadata.config();
        for (int shard = 0; shard < metadata.shards().size(); shard++) {
            Versioned<ShardState> state = metadata.shards().get(shard);
            if (!state.value().replicas().contains(self.brokerId())) {
                continue;
            }
            ShardId id = new ShardId(config.name(), shard);
            ShardReplica replica = replicas.computeIfAbsent(id, created -> new ShardReplica(
                    id, self, config, new InMemoryShardLog(), clock, replicaLagMax, commitTimeout));
            replica.updateConfig(config);
            boolean wasLeading = replica.isLeader();
            boolean followsSomeoneNew = false;
            if (self.equals(state.value().leader())) {
                replica.lead(state);
            } else {
                followsSomeoneNew = replica.follow(state);
            }
            if (replica.isLeader()) {
                FollowerFetcher fetcher = fetchers.remove(id);
                if (fetcher != null) {
                    fetcher.stop();
                }
            } else {
                FollowerFetcher fetcher = fetchers.get(id);
                if (fetcher == null) {
                    fetcher = fetcherFactory.apply(replica);
                    fetchers.put(id, fetcher);
                    fetcher.start();
                } else if (followsSomeoneNew) {
                    fetcher.reconnect();
                }
            }
            if (replica.isLeader() != wasLeading) {
                (replica.isLeader() ? onLeading : onNoLongerLeading).forEach(listener -> listener.accept(replica));
            }
        }
    }

    Optional<ShardReplica> find(ShardId id) {
        return Optional.ofNullable(replicas.get(id));
    }

    Collection<ShardReplica> all() {
        return replicas.values();
    }

    List<ShardReplica> leaders() {
        return replicas.values().stream().filter(ShardReplica::isLeader).toList();
    }

    /** Calls the listener for every replica that leads its shard now and every one that comes to lead later. */
    synchronized void onLeading(Consumer<ShardReplica> listener) {
        onLeading.add(listener);
        leaders().forEach(listener);
    }

    /** Calls the listener whenever a replica that led its shard stops leading it. */
    synchronized void onNoLongerLeading(Consumer<ShardReplica> listener) {
        onNoLongerLeading.add(listener);
    }

    void stop() {
        fetchers.values().forEach(FollowerFetcher::stop);
    }
}
