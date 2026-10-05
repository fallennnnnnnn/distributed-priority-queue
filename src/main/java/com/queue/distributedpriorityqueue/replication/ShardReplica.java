package com.queue.distributedpriorityqueue.replication;

import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.core.ShardEvent;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.core.ShardStateMachine;
import com.queue.distributedpriorityqueue.log.ShardLog;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * One broker's copy of a shard: its log, its state machine and the lock that guards both.
 *
 * <p>This is the leader path. Every write decides, appends and applies while holding the shard
 * lock, so each state transition is atomic. The caller's future completes outside the lock, once
 * the events it appended are committed. With a single replica the leader is the whole in-sync
 * set, so events are committed as soon as they are appended.
 */
public final class ShardReplica {

    private final int shard;
    private final ShardLog log;
    private final ShardStateMachine state;
    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();
    /** Futures waiting for the high watermark to pass an offset, keyed by that offset. */
    private final NavigableMap<Long, CompletableFuture<Void>> commitWaiters = new TreeMap<>();
    private final int epoch = 0;
    private volatile QueueConfig config;
    /** Read without the lock by dequeue routing, so it may be a moment out of date. */
    private volatile int highestReadyRank = -1;

    public ShardReplica(int shard, QueueConfig config, ShardLog log, Clock clock) {
        this.shard = shard;
        this.config = config;
        this.log = log;
        this.clock = clock;
        this.state = new ShardStateMachine(shard, config.priorityLevels().size());
    }

    public int shard() {
        return shard;
    }

    public QueueConfig config() {
        return config;
    }

    /** The rank of the highest priority with a ready message, or -1 when nothing is ready. */
    public int highestReadyRank() {
        return highestReadyRank;
    }

    /**
     * @param priority one of the queue's level names; null means the lowest level
     * @param ttl how long the message may wait; null means the queue's default TTL
     */
    public CompletableFuture<MessageId> enqueue(String payload, String priority, Duration ttl) {
        QueueConfig config = this.config;
        int rank = config.rankOf(priority);
        Duration lifetime = ttl != null ? ttl : config.defaultTtl();
        if (lifetime != null && (lifetime.isNegative() || lifetime.isZero())) {
            throw new IllegalArgumentException("ttl must be positive");
        }
        return write(now -> {
            Instant expiresAt = lifetime == null ? null : now.plus(lifetime);
            long offset = appendAndApply(state.enqueue(payload, rank, expiresAt, now));
            return new MessageId(shard, offset);
        });
    }

    /** The highest-priority ready message, now in flight; empty when nothing is ready. */
    public CompletableFuture<Optional<MessageView>> dequeue() {
        QueueConfig config = this.config;
        return write(now -> {
            reclaim(now, config);
            return state.dequeue(now, config.visibilityTimeout()).map(delivered -> {
                appendAndApply(delivered);
                return state.find(delivered.messageOffset()).orElseThrow();
            });
        });
    }

    /** False when there is nothing to acknowledge: unknown, never delivered, or already gone. */
    public CompletableFuture<Boolean> ack(MessageId id) {
        if (id.shard() != shard) {
            return CompletableFuture.completedFuture(false);
        }
        return write(now -> state.ack(id.offset(), now).map(acked -> {
            appendAndApply(acked);
            return true;
        }).orElse(false));
    }

    /** Returns timed-out messages to their lanes, dead-letters or expires what is due. */
    public CompletableFuture<Void> reclaim() {
        QueueConfig config = this.config;
        return write(now -> {
            reclaim(now, config);
            return null;
        });
    }

    public ShardMetrics metrics() {
        reclaim();
        lock.lock();
        try {
            return state.metrics(clock.instant());
        } finally {
            lock.unlock();
        }
    }

    public List<MessageView> deadLetters() {
        lock.lock();
        try {
            return state.deadLetters();
        } finally {
            lock.unlock();
        }
    }

    /** Applies to future deliveries. The number of priority levels cannot change. */
    public void updateConfig(QueueConfig config) {
        if (config.priorityLevels().size() != this.config.priorityLevels().size()) {
            throw new IllegalArgumentException("a queue's priority levels are fixed at creation");
        }
        this.config = config;
    }

    /**
     * Runs {@code action} under the shard lock. The returned future completes with the action's
     * result once everything the action appended is committed.
     */
    private <T> CompletableFuture<T> write(Function<Instant, T> action) {
        T result;
        CompletableFuture<Void> committed;
        List<CompletableFuture<Void>> nowCommitted;
        lock.lock();
        try {
            long before = log.endOffset();
            result = action.apply(clock.instant());
            long end = log.endOffset();
            committed = end == before
                    ? CompletableFuture.completedFuture(null)
                    : commitWaiters.computeIfAbsent(end - 1, offset -> new CompletableFuture<>());
            highestReadyRank = state.highestReadyRank();
            // One replica: the leader is the whole in-sync set, so the end of its log is committed.
            nowCommitted = advanceHighWatermark(end);
        } finally {
            lock.unlock();
        }
        nowCommitted.forEach(waiter -> waiter.complete(null));
        return committed.thenApply(ignored -> result);
    }

    private void reclaim(Instant now, QueueConfig config) {
        for (ShardEvent event : state.reclaim(now, config.maxRetries())) {
            appendAndApply(event);
        }
    }

    private long appendAndApply(ShardEvent event) {
        long offset = log.append(epoch, event);
        state.apply(offset, event);
        return offset;
    }

    /** Returns the waiters that are now committed; the caller completes them after unlocking. */
    private List<CompletableFuture<Void>> advanceHighWatermark(long highWatermark) {
        log.advanceHighWatermark(highWatermark);
        NavigableMap<Long, CompletableFuture<Void>> committed = commitWaiters.headMap(highWatermark, false);
        List<CompletableFuture<Void>> waiters = new ArrayList<>(committed.values());
        committed.clear();
        return waiters;
    }
}
