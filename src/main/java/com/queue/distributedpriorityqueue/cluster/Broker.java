package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.BrokerInfo;
import com.queue.distributedpriorityqueue.coordination.Coordinator;
import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.coordination.Versioned;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.exception.NotLeaderException;
import com.queue.distributedpriorityqueue.replication.Acks;
import com.queue.distributedpriorityqueue.replication.FetchEnd;
import com.queue.distributedpriorityqueue.replication.FetchListener;
import com.queue.distributedpriorityqueue.replication.FetchRequest;
import com.queue.distributedpriorityqueue.replication.FetchStream;
import com.queue.distributedpriorityqueue.replication.FollowerFetcher;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One instance of the service. It registers with the coordinator, holds the shard replicas
 * assigned to it, serves the shards it leads and replicates the ones it follows. Several brokers
 * can run in one JVM, each with its own settings, which is how the multi-broker tests work.
 */
public final class Broker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    /** How long a follower waits before trying its leader again. */
    private static final Duration FETCH_RETRY = Duration.ofMillis(50);

    private final BrokerInfo info;
    private final Coordinator coordinator;
    private final BrokerSettings settings;
    private final ClusterMetadata metadata;
    private final LocalShards localShards;
    private final Peers peers;
    private final Map<ShardId, Integer> remoteReadyRanks = new ConcurrentHashMap<>();
    private final QueueService queues;
    private final ScheduledExecutorService scheduler;
    private final BrokerApi api = new LocalApi();
    private Coordinator.Registration registration;
    private Coordinator.Subscription subscription;

    public Broker(BrokerSettings settings, Coordinator coordinator, BrokerTransport transport, Clock clock) {
        this.settings = settings;
        this.info = new BrokerInfo(settings.brokerId(), UUID.randomUUID().toString(), settings.advertised().host(),
                settings.advertised().httpPort(), settings.advertised().grpcPort());
        this.coordinator = coordinator;
        this.scheduler = Executors.newScheduledThreadPool(2, task -> {
            Thread thread = new Thread(task, "dpq-" + info.id());
            thread.setDaemon(true);
            return thread;
        });
        ReplicaId self = info.replicaId();
        this.metadata = new ClusterMetadata(coordinator);
        this.peers = new Peers(self, coordinator, transport);
        this.localShards = new LocalShards(self, clock, settings.replicaLagMax(), settings.commitTimeout(),
                this::fetcherFor);
        this.metadata.onRefresh(localShards::sync);
        this.queues = new QueueService(self, coordinator, metadata, localShards, peers, remoteReadyRanks,
                new Router(), scheduler);
    }

    public void start() {
        registration = coordinator.register(info, () -> { });
        subscription = coordinator.watch(new Coordinator.Listener() {
            @Override
            public void onBrokersChanged() {
                peers.refresh();
            }

            @Override
            public void onQueueChanged(String queue) {
                metadata.refresh(queue);
            }

            @Override
            public void onShardChanged(ShardId shard) {
                metadata.refresh(shard.queue());
            }
        });
        peers.refresh();
        coordinator.queueNames().forEach(metadata::refresh);
        every(settings.sweeperInterval(), this::sweep);
        every(replicationCheckInterval(), this::maintainReplication);
        every(settings.summaryInterval(), this::exchangeSummaries);
        log.info("Broker {} started as incarnation {}", info.id(), info.incarnation());
    }

    public BrokerInfo info() {
        return info;
    }

    public QueueService queues() {
        return queues;
    }

    /** What this broker serves to other brokers. A transport exposes it to them. */
    public BrokerApi api() {
        return api;
    }

    /** This broker's replica of a shard, if it holds one. */
    public Optional<ShardReplica> replica(ShardId shard) {
        return localShards.find(shard);
    }

    /** Calls the listener for every shard replica that leads its shard now, or comes to lead it later. */
    public void onLeading(Consumer<ShardReplica> listener) {
        localShards.onLeading(listener);
    }

    /** Reclaims timed-out and expired messages on every shard this broker leads, so idle shards still make progress. */
    public void sweep() {
        for (ShardReplica replica : localShards.leaders()) {
            try {
                replica.reclaim();
            } catch (RuntimeException e) {
                log.warn("Sweeping shard {} failed", replica.id(), e);
            }
        }
    }

    /**
     * For every shard this broker leads, drops followers that have fallen behind from the in-sync
     * set and adds those that have caught up. Each change is written to the coordinator first.
     */
    public void maintainReplication() {
        for (ShardReplica replica : localShards.leaders()) {
            try {
                replica.proposeIsrChange().ifPresent(proposal -> {
                    OptionalLong version =
                            coordinator.updateShard(proposal.shard(), proposal.expectedVersion(), proposal.state());
                    if (version.isPresent()) {
                        replica.isrChanged(new Versioned<>(proposal.state(), version.getAsLong()));
                        log.info("Shard {}: in-sync replicas are now {}", replica.id(), proposal.state().isr());
                    } else {
                        replica.isrChangeRefused();
                    }
                    metadata.refresh(proposal.shard().queue());
                });
            } catch (RuntimeException e) {
                replica.isrChangeRefused();
                log.warn("Updating the in-sync replicas of shard {} failed", replica.id(), e);
            }
        }
    }

    /**
     * Asks every other broker which priorities are ready in the shards it leads; dequeue routing
     * uses the answers. The returned future completes when every broker has answered or failed.
     */
    public CompletableFuture<Void> exchangeSummaries() {
        List<CompletableFuture<Void>> answers = new ArrayList<>();
        for (BrokerApi broker : peers.all()) {
            try {
                answers.add(broker.readySummary().thenAccept(remoteReadyRanks::putAll).exceptionally(error -> null));
            } catch (RuntimeException e) {
                log.debug("Could not ask a broker for its ready summary", e);
            }
        }
        return CompletableFuture.allOf(answers.toArray(CompletableFuture[]::new));
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        localShards.stop();
        if (subscription != null) {
            subscription.close();
        }
        if (registration != null) {
            registration.close();
        }
    }

    private void every(Duration interval, Runnable task) {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                task.run();
            } catch (RuntimeException e) {
                log.warn("A periodic task failed", e);
            }
        }, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Often enough to notice a lagging follower well within replicaLagMax. */
    private Duration replicationCheckInterval() {
        long millis = Math.max(20, Math.min(1000, settings.replicaLagMax().toMillis() / 4));
        return Duration.ofMillis(millis);
    }

    private FollowerFetcher fetcherFor(ShardReplica replica) {
        ShardId shard = replica.id();
        FollowerFetcher.LeaderConnection leader = (request, toFollower) -> metadata.find(shard.queue())
                .map(queue -> queue.shard(shard.shard()).leader())
                .flatMap(peers::find)
                .map(broker -> broker.openFetch(request, toFollower));
        return new FollowerFetcher(replica, leader, () -> metadata.refresh(shard.queue()), scheduler, FETCH_RETRY);
    }

    /** Requests from other brokers. Each is served only if this broker leads the shard. */
    private final class LocalApi implements BrokerApi {

        @Override
        public CompletableFuture<MessageId> enqueue(ShardId shard, String payload, String priority, Duration ttl,
                Acks acks) {
            return onLeader(shard, replica -> replica.enqueue(payload, priority, ttl, acks));
        }

        @Override
        public CompletableFuture<Optional<MessageView>> dequeue(ShardId shard) {
            return onLeader(shard, ShardReplica::dequeue);
        }

        @Override
        public CompletableFuture<Boolean> ack(ShardId shard, MessageId messageId) {
            return onLeader(shard, replica -> replica.ack(messageId));
        }

        @Override
        public CompletableFuture<ShardMetrics> metrics(ShardId shard) {
            return onLeader(shard, replica -> CompletableFuture.completedFuture(replica.metrics()));
        }

        @Override
        public CompletableFuture<List<MessageView>> deadLetters(ShardId shard) {
            return onLeader(shard, replica -> CompletableFuture.completedFuture(replica.deadLetters()));
        }

        @Override
        public CompletableFuture<Map<ShardId, Integer>> readySummary() {
            Map<ShardId, Integer> summary = new HashMap<>();
            for (ShardReplica replica : localShards.leaders()) {
                summary.put(replica.id(), replica.highestReadyRank());
            }
            return CompletableFuture.completedFuture(summary);
        }

        @Override
        public FetchStream openFetch(FetchRequest request, FetchListener toFollower) {
            Optional<ShardReplica> replica = replicaFor(request.shard());
            if (replica.isEmpty()) {
                toFollower.onEnd(FetchEnd.of(FetchEnd.Reason.NOT_LEADER));
                return FetchStream.REFUSED;
            }
            return replica.get().openFetch(request, toFollower);
        }

        private <T> CompletableFuture<T> onLeader(ShardId shard, Function<ShardReplica, CompletableFuture<T>> request) {
            try {
                return request.apply(replicaFor(shard).orElseThrow(() -> new NotLeaderException(shard)));
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        /** A request can arrive before this broker has heard of the queue, so look it up if needed. */
        private Optional<ShardReplica> replicaFor(ShardId shard) {
            Optional<ShardReplica> replica = localShards.find(shard);
            if (replica.isEmpty()) {
                metadata.refresh(shard.queue());
                replica = localShards.find(shard);
            }
            return replica;
        }
    }
}
