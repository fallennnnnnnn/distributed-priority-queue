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
import com.queue.distributedpriorityqueue.exception.QueueAlreadyExistsException;
import com.queue.distributedpriorityqueue.exception.QueueNotFoundException;
import com.queue.distributedpriorityqueue.exception.ShardUnavailableException;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

/**
 * Everything a client can ask of a queue. Each request is routed to a shard and carried out by
 * that shard's leader replica.
 */
public final class QueueService {

    /** The longest a dequeue may wait for a message. */
    public static final Duration MAX_WAIT = Duration.ofSeconds(20);

    /** How often a waiting dequeue looks again. */
    private static final Duration WAIT_RETRY = Duration.ofMillis(50);

    private static final int UPDATE_ATTEMPTS = 10;

    private final Coordinator coordinator;
    private final ClusterMetadata metadata;
    private final LocalShards localShards;
    private final Router router;
    private final ScheduledExecutorService scheduler;

    QueueService(Coordinator coordinator, ClusterMetadata metadata, LocalShards localShards, Router router,
            ScheduledExecutorService scheduler) {
        this.coordinator = coordinator;
        this.metadata = metadata;
        this.localShards = localShards;
        this.router = router;
        this.scheduler = scheduler;
    }

    public QueueMetadata createQueue(QueueConfig config) {
        List<BrokerInfo> brokers = coordinator.liveBrokers();
        if (config.replicationFactor() > brokers.size()) {
            throw new IllegalArgumentException("replicationFactor " + config.replicationFactor()
                    + " needs that many live brokers, and there are " + brokers.size());
        }
        if (!coordinator.createQueue(config, assignReplicas(config, brokers))) {
            throw new QueueAlreadyExistsException(config.name());
        }
        return metadata.refresh(config.name()).orElseThrow();
    }

    public QueueMetadata describe(String queue) {
        return require(queue);
    }

    /** Changes a queue's settings. Priority levels, shard count and replication factor cannot change. */
    public QueueMetadata updateQueue(String queue, UnaryOperator<QueueConfig.Builder> change) {
        for (int attempt = 0; attempt < UPDATE_ATTEMPTS; attempt++) {
            Versioned<QueueConfig> current =
                    coordinator.queue(queue).orElseThrow(() -> new QueueNotFoundException(queue));
            QueueConfig before = current.value();
            QueueConfig after = change.apply(before.toBuilder()).build();
            if (!after.priorityLevels().equals(before.priorityLevels()) || after.shards() != before.shards()
                    || after.replicationFactor() != before.replicationFactor()) {
                throw new IllegalArgumentException(
                        "priorityLevels, shards and replicationFactor are fixed when the queue is created");
            }
            if (coordinator.updateQueue(queue, current.version(), after)) {
                return metadata.refresh(queue).orElseThrow();
            }
        }
        throw new IllegalStateException("the settings of queue " + queue + " kept changing; try again");
    }

    /**
     * @param priority one of the queue's level names; null means the lowest level
     * @param ttl null means the queue's default TTL
     * @param shardKey messages with the same key keep their order; null spreads messages evenly
     */
    public CompletableFuture<MessageId> enqueue(String queue, String payload, String priority, Duration ttl,
            String shardKey) {
        QueueConfig config = require(queue).config();
        int shard = router.shardForEnqueue(config, shardKey);
        return leader(queue, shard).enqueue(payload, priority, ttl);
    }

    /**
     * The highest-priority ready message. When the queue is empty, looks again until
     * {@code waitTime} has passed and then completes empty.
     */
    public CompletableFuture<Optional<MessageView>> dequeue(String queue, Duration waitTime) {
        if (waitTime.isNegative() || waitTime.compareTo(MAX_WAIT) > 0) {
            throw new IllegalArgumentException("waitTime must be between 0 and " + MAX_WAIT.toSeconds() + " seconds");
        }
        require(queue);
        CompletableFuture<Optional<MessageView>> result = new CompletableFuture<>();
        dequeueUntil(queue, System.nanoTime() + waitTime.toNanos(), result);
        return result;
    }

    /** False when there is nothing to acknowledge: unknown, never delivered, or already gone. */
    public CompletableFuture<Boolean> ack(String queue, MessageId id) {
        QueueConfig config = require(queue).config();
        if (id.shard() >= config.shards()) {
            return CompletableFuture.completedFuture(false);
        }
        return leader(queue, id.shard()).ack(id);
    }

    public QueueMetrics metrics(String queue) {
        QueueConfig config = require(queue).config();
        long[] ready = new long[config.priorityLevels().size()];
        long inFlight = 0;
        Duration oldest = Duration.ZERO;
        long deadLetters = 0;
        long enqueued = 0;
        long delivered = 0;
        long redelivered = 0;
        long acked = 0;
        long deadLettered = 0;
        long expired = 0;
        double enqueuedPerSecond = 0;
        double ackedPerSecond = 0;
        for (int shard = 0; shard < config.shards(); shard++) {
            ShardMetrics metrics = leader(queue, shard).metrics();
            for (int rank = 0; rank < ready.length; rank++) {
                ready[rank] += metrics.readyByPriority().get(rank);
            }
            inFlight += metrics.inFlight();
            oldest = oldest.compareTo(metrics.oldestReadyAge()) >= 0 ? oldest : metrics.oldestReadyAge();
            deadLetters += metrics.deadLetters();
            enqueued += metrics.enqueuedTotal();
            delivered += metrics.deliveredTotal();
            redelivered += metrics.redeliveredTotal();
            acked += metrics.ackedTotal();
            deadLettered += metrics.deadLetteredTotal();
            expired += metrics.expiredTotal();
            enqueuedPerSecond += metrics.enqueuedPerSecond();
            ackedPerSecond += metrics.ackedPerSecond();
        }
        Map<String, Long> readyByLevel = new LinkedHashMap<>();
        long readyTotal = 0;
        for (int rank = 0; rank < ready.length; rank++) {
            readyByLevel.put(config.levelName(rank), ready[rank]);
            readyTotal += ready[rank];
        }
        return new QueueMetrics(queue, readyByLevel, readyTotal, inFlight, oldest.toMillis() / 1000.0, deadLetters,
                enqueued, delivered, redelivered, acked, deadLettered, expired, enqueuedPerSecond, ackedPerSecond);
    }

    public List<MessageView> deadLetters(String queue) {
        QueueConfig config = require(queue).config();
        List<MessageView> deadLetters = new ArrayList<>();
        for (int shard = 0; shard < config.shards(); shard++) {
            deadLetters.addAll(leader(queue, shard).deadLetters());
        }
        return deadLetters;
    }

    private void dequeueUntil(String queue, long deadlineNanos, CompletableFuture<Optional<MessageView>> result) {
        CompletableFuture<Optional<MessageView>> attempt;
        try {
            attempt = dequeueOnce(queue);
        } catch (RuntimeException e) {
            result.completeExceptionally(e);
            return;
        }
        attempt.whenComplete((message, error) -> {
            long remaining = deadlineNanos - System.nanoTime();
            if (error != null) {
                result.completeExceptionally(error);
            } else if (message.isPresent() || remaining <= 0) {
                result.complete(message);
            } else {
                scheduler.schedule(() -> dequeueUntil(queue, deadlineNanos, result),
                        Math.min(remaining, WAIT_RETRY.toNanos()), TimeUnit.NANOSECONDS);
            }
        });
    }

    /** Tries the shards in the router's order and stops at the first that has a message. */
    private CompletableFuture<Optional<MessageView>> dequeueOnce(String queue) {
        QueueConfig config = require(queue).config();
        List<Integer> order = router.dequeueOrder(config, shard -> localShards.find(new ShardId(queue, shard))
                .map(ShardReplica::highestReadyRank)
                .orElse(-1));
        CompletableFuture<Optional<MessageView>> result = CompletableFuture.completedFuture(Optional.empty());
        for (int shard : order) {
            result = result.thenCompose(found ->
                    found.isPresent() ? CompletableFuture.completedFuture(found) : leader(queue, shard).dequeue());
        }
        return result;
    }

    private QueueMetadata require(String queue) {
        return metadata.find(queue).orElseThrow(() -> new QueueNotFoundException(queue));
    }

    private ShardReplica leader(String queue, int shard) {
        ShardId id = new ShardId(queue, shard);
        return localShards.find(id).orElseThrow(() -> new ShardUnavailableException(id));
    }

    /**
     * Spreads each shard's replicas over the brokers in turn. The starting broker depends on the
     * queue's name, so that many small queues do not all put their first shard on the same broker.
     */
    private static List<ShardState> assignReplicas(QueueConfig config, List<BrokerInfo> brokers) {
        int first = Math.floorMod(config.name().hashCode(), brokers.size());
        List<ShardState> shards = new ArrayList<>(config.shards());
        for (int shard = 0; shard < config.shards(); shard++) {
            List<ReplicaId> replicas = new ArrayList<>(config.replicationFactor());
            for (int i = 0; i < config.replicationFactor(); i++) {
                replicas.add(brokers.get((first + shard + i) % brokers.size()).replicaId());
            }
            shards.add(new ShardState(replicas.get(0), 0, replicas,
                    replicas.stream().map(ReplicaId::brokerId).toList()));
        }
        return shards;
    }
}
