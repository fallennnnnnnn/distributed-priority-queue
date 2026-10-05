package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.BrokerInfo;
import com.queue.distributedpriorityqueue.coordination.Coordinator;
import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.coordination.ShardState;
import com.queue.distributedpriorityqueue.coordination.Versioned;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.exception.NotLeaderException;
import com.queue.distributedpriorityqueue.exception.ShardUnavailableException;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One instance of the service. It registers with the coordinator, holds the shard replicas
 * assigned to it, serves the shards it leads, replicates the ones it follows, and stands for
 * election when a shard it holds loses its leader. Several brokers can run in one JVM, each with
 * its own settings, which is how the multi-broker tests work.
 */
public final class Broker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    /** How long a follower waits before trying its leader again, and a broker before registering again. */
    private static final Duration RETRY = Duration.ofMillis(50);

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
    private volatile Coordinator.Registration registration;
    private Coordinator.Subscription subscription;
    /**
     * True from the moment this broker loses its lease until it has registered again. The other
     * brokers may elect new leaders in that time, so this one must not act as a leader.
     */
    private volatile boolean fenced;
    private volatile boolean stopped;

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
                new Router(), scheduler, () -> fenced);
    }

    public void start() {
        registration = coordinator.register(info, this::leaseLost);
        subscription = coordinator.watch(new Coordinator.Listener() {
            @Override
            public void onBrokersChanged() {
                // A broker that left may have led shards this broker holds.
                inBackground(() -> {
                    peers.refresh();
                    electLeaders();
                });
            }

            @Override
            public void onQueueChanged(String queue) {
                inBackground(() -> metadata.refresh(queue));
            }

            @Override
            public void onShardChanged(ShardId shard) {
                inBackground(() -> metadata.refresh(shard.queue()));
            }
        });
        peers.refresh();
        coordinator.queueNames().forEach(metadata::refresh);
        electLeaders();
        every(settings.sweeperInterval(), this::sweep);
        every(replicationCheckInterval(), () -> {
            maintainReplication();
            electLeaders();
        });
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

    /** Calls the listener whenever a replica that led its shard stops leading it. */
    public void onNoLongerLeading(Consumer<ShardReplica> listener) {
        localShards.onNoLongerLeading(listener);
    }

    public boolean isFenced() {
        return fenced;
    }

    /** Reclaims timed-out and expired messages on every shard this broker leads, so idle shards still make progress. */
    public void sweep() {
        if (fenced) {
            return;
        }
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
        if (fenced) {
            return;
        }
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
     * Stands for election in every shard this broker holds whose leader is no longer a live
     * broker. The coordinator's compare-and-swap lets one candidate win.
     *
     * <p>Only an in-sync replica may stand, because only it is sure to have every committed
     * event. The exception is a shard none of whose in-sync replicas is alive: if the queue
     * allows unclean election, or has only one replica to begin with, any assigned replica takes
     * over with whatever log it has, and that is counted as a reset.
     */
    public void electLeaders() {
        if (fenced || stopped) {
            return;
        }
        Set<ReplicaId> live;
        try {
            live = coordinator.liveBrokers().stream().map(BrokerInfo::replicaId).collect(Collectors.toSet());
        } catch (RuntimeException e) {
            log.debug("Could not list the live brokers", e);
            return;
        }
        ReplicaId self = info.replicaId();
        if (!live.contains(self)) {
            return;
        }
        for (ShardReplica replica : localShards.all()) {
            try {
                standFor(replica, self, live);
            } catch (RuntimeException e) {
                log.warn("Standing for election in shard {} failed", replica.id(), e);
            }
        }
    }

    private void standFor(ShardReplica replica, ReplicaId self, Set<ReplicaId> live) {
        ShardId shard = replica.id();
        Optional<Versioned<ShardState>> read = coordinator.shard(shard);
        if (read.isEmpty()) {
            return;
        }
        ShardState state = read.get().value();
        if (state.leader() != null && live.contains(state.leader())) {
            return;
        }
        boolean inSync = state.isr().contains(self);
        if (!inSync) {
            QueueConfig config = replica.config();
            boolean uncleanAllowed = config.uncleanLeaderElection() || config.replicationFactor() == 1;
            boolean anInSyncReplicaIsAlive = state.isr().stream().anyMatch(live::contains);
            if (!uncleanAllowed || anInSyncReplicaIsAlive || !state.replicas().contains(self.brokerId())) {
                return;
            }
        }
        List<ReplicaId> isr = inSync
                ? state.isr().stream().filter(member -> !member.equals(state.leader())).toList()
                : List.of(self);
        ShardState proposed = new ShardState(self, state.epoch() + 1, isr, state.replicas());
        if (coordinator.updateShard(shard, read.get().version(), proposed).isPresent()) {
            replica.wonElection(!inSync);
            log.info("Shard {}: broker {} is the leader of epoch {}{}", shard, info.id(), proposed.epoch(),
                    inSync ? "" : ", elected without being in sync: committed messages may be lost");
        }
        // Won or lost, take whatever role the coordinator now names.
        metadata.refresh(shard.queue());
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

    /** Shuts down cleanly: the coordinator is told at once, so the other brokers can take over. */
    @Override
    public void close() {
        halt();
        Coordinator.Registration current = registration;
        if (current != null) {
            current.close();
        }
    }

    /** Stops without telling the coordinator, as a crash would. The others find out when the lease runs out. */
    public void kill() {
        halt();
    }

    private void halt() {
        stopped = true;
        fenced = true;
        scheduler.shutdownNow();
        localShards.stop();
        if (subscription != null) {
            subscription.close();
        }
    }

    /** The coordinator no longer lists this broker. It stops leading until it is registered again. */
    private void leaseLost() {
        if (stopped) {
            return;
        }
        fenced = true;
        log.warn("Broker {} lost its lease; it serves nothing as leader until it has registered again", info.id());
        inBackground(this::registerAgain);
    }

    private void registerAgain() {
        if (stopped) {
            return;
        }
        Coordinator.Registration renewed = null;
        try {
            renewed = coordinator.register(info, this::leaseLost);
            peers.refresh();
            // Shards may have new leaders by now; take the role each one has.
            coordinator.queueNames().forEach(metadata::refresh);
            registration = renewed;
            fenced = false;
            log.info("Broker {} registered again", info.id());
            electLeaders();
        } catch (RuntimeException e) {
            if (renewed != null) {
                renewed.close();
            }
            try {
                scheduler.schedule(this::registerAgain, RETRY.toMillis(), TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException shuttingDown) {
                // Nothing left to do.
            }
        }
    }

    private void inBackground(Runnable task) {
        try {
            scheduler.execute(() -> {
                try {
                    task.run();
                } catch (RuntimeException e) {
                    log.warn("A background task failed", e);
                }
            });
        } catch (RejectedExecutionException shuttingDown) {
            // Nothing left to do.
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
        return new FollowerFetcher(replica, leader, () -> metadata.refresh(shard.queue()), scheduler, RETRY);
    }

    /** Requests from other brokers. Each is served only if this broker leads the shard in the sender's epoch. */
    private final class LocalApi implements BrokerApi {

        @Override
        public CompletableFuture<MessageId> enqueue(ShardId shard, int epoch, String payload, String priority,
                Duration ttl, Acks acks) {
            return onLeader(shard, epoch, replica -> replica.enqueue(payload, priority, ttl, acks));
        }

        @Override
        public CompletableFuture<Optional<MessageView>> dequeue(ShardId shard, int epoch) {
            return onLeader(shard, epoch, ShardReplica::dequeue);
        }

        @Override
        public CompletableFuture<Boolean> ack(ShardId shard, int epoch, MessageId messageId) {
            return onLeader(shard, epoch, replica -> replica.ack(messageId));
        }

        @Override
        public CompletableFuture<ShardMetrics> metrics(ShardId shard, int epoch) {
            return onLeader(shard, epoch, replica -> CompletableFuture.completedFuture(replica.metrics()));
        }

        @Override
        public CompletableFuture<List<MessageView>> deadLetters(ShardId shard, int epoch) {
            return onLeader(shard, epoch, replica -> CompletableFuture.completedFuture(replica.deadLetters()));
        }

        @Override
        public CompletableFuture<Map<ShardId, Integer>> readySummary() {
            Map<ShardId, Integer> summary = new HashMap<>();
            if (!fenced) {
                for (ShardReplica replica : localShards.leaders()) {
                    summary.put(replica.id(), replica.highestReadyRank());
                }
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
            if (request.epoch() > replica.get().epoch()) {
                // The follower knows of a newer leader than this broker does.
                inBackground(() -> metadata.refresh(request.shard().queue()));
            }
            return replica.get().openFetch(request, toFollower);
        }

        private <T> CompletableFuture<T> onLeader(ShardId shard, int epoch,
                Function<ShardReplica, CompletableFuture<T>> request) {
            try {
                ShardReplica replica = replicaFor(shard).orElseThrow(() -> new NotLeaderException(shard));
                if (fenced) {
                    throw new ShardUnavailableException(shard);
                }
                if (epoch != replica.epoch()) {
                    if (epoch > replica.epoch()) {
                        inBackground(() -> metadata.refresh(shard.queue()));
                    }
                    throw new NotLeaderException(shard);
                }
                return request.apply(replica);
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
