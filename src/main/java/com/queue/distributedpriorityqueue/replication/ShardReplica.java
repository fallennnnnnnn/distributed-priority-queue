package com.queue.distributedpriorityqueue.replication;

import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.coordination.ShardState;
import com.queue.distributedpriorityqueue.coordination.Versioned;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.core.ShardEvent;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.core.ShardStateMachine;
import com.queue.distributedpriorityqueue.exception.CommitTimeoutException;
import com.queue.distributedpriorityqueue.exception.NotEnoughReplicasException;
import com.queue.distributedpriorityqueue.exception.NotLeaderException;
import com.queue.distributedpriorityqueue.log.LogEntry;
import com.queue.distributedpriorityqueue.log.ShardLog;
import com.queue.distributedpriorityqueue.log.ShardLog.EpochEnd;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * One broker's copy of a shard: its log, its state machine and the lock that guards both.
 *
 * <p><b>As leader</b> it serves requests. Every write decides, appends and applies while holding
 * the shard lock, so each state transition is atomic. The caller's future completes outside the
 * lock, once the events it appended are committed: on every in-sync replica, which is what the
 * high watermark tracks. The leader sends its log to each follower over a fetch stream, one batch
 * at a time, and sends the next batch when the follower reports having the last one.
 *
 * <p><b>As follower</b> it appends what the leader sends and applies only what is committed, so
 * its state never includes an event that could still be discarded.
 */
public final class ShardReplica {

    public enum Role { LEADER, FOLLOWER }

    /** The most entries the leader sends a follower in one message. */
    private static final int FETCH_BATCH = 500;

    private final ShardId id;
    private final ReplicaId self;
    private final ShardLog log;
    /** Replaced by a fresh one, rebuilt from the log, when a leader steps down. */
    private ShardStateMachine state;
    private final Clock clock;
    private final Duration replicaLagMax;
    private final Duration commitTimeout;
    private final ReentrantLock lock = new ReentrantLock();

    private volatile QueueConfig config;
    private volatile Role role = Role.FOLLOWER;
    /** Read without the lock by dequeue routing, so it may be a moment out of date. */
    private volatile int highestReadyRank = -1;
    private volatile int inSyncReplicas = 1;
    private volatile int epoch;

    // Everything below is guarded by the lock.

    /** The shard's state in the coordinator, as this replica last knew it. A leader's copy is the one it wrote. */
    private Versioned<ShardState> shardState;
    /** The offset of the next entry to apply to the state machine. */
    private long applied;
    /** Futures waiting for the high watermark to pass an offset, keyed by that offset. */
    private final NavigableMap<Long, CompletableFuture<Void>> commitWaiters = new TreeMap<>();
    /** Leader: the open fetch stream of each follower. */
    private final Map<ReplicaId, FollowerSession> sessions = new HashMap<>();
    /** Leader: where each follower's log ended when it last reported. */
    private final Map<ReplicaId, Long> followerLogEnd = new HashMap<>();
    /** Leader: the last moment each follower is known to have had everything the leader had. */
    private final Map<ReplicaId, Instant> lastCaughtUp = new HashMap<>();
    private boolean isrChangePending;

    private final AtomicLong electionsWon = new AtomicLong();
    private final AtomicLong resets = new AtomicLong();
    /** The in-sync set proposed to the coordinator and not yet answered, or null. */
    private List<ReplicaId> pendingIsr;

    public ShardReplica(ShardId id, ReplicaId self, QueueConfig config, ShardLog log, Clock clock,
            Duration replicaLagMax, Duration commitTimeout) {
        this.id = id;
        this.self = self;
        this.config = config;
        this.log = log;
        this.clock = clock;
        this.replicaLagMax = replicaLagMax;
        this.commitTimeout = commitTimeout;
        this.state = new ShardStateMachine(id.shard(), config.priorityLevels().size());
        this.shardState = new Versioned<>(new ShardState(null, 0, List.of(), List.of()), -1);
    }

    /** A shard with no other replicas, leading itself: what an unreplicated queue runs on. */
    public static ShardReplica standalone(int shard, QueueConfig config, ShardLog log, Clock clock) {
        ReplicaId self = new ReplicaId("local", "local");
        ShardReplica replica = new ShardReplica(new ShardId(config.name(), shard), self, config, log, clock,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        replica.lead(new Versioned<>(new ShardState(self, 0, List.of(self), List.of(self.brokerId())), 0));
        return replica;
    }

    public ShardId id() {
        return id;
    }

    public int shard() {
        return id.shard();
    }

    public QueueConfig config() {
        return config;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    /** The epoch of the leader this replica is, or follows. */
    public int epoch() {
        return epoch;
    }

    /**
     * Notes that this replica won an election. A reset is an election won without being in sync,
     * which means the shard may have lost committed messages.
     */
    public void wonElection(boolean reset) {
        electionsWon.incrementAndGet();
        if (reset) {
            resets.incrementAndGet();
        }
    }

    public long electionsWon() {
        return electionsWon.get();
    }

    public long resets() {
        return resets.get();
    }

    /** The rank of the highest priority with a ready message, or -1 when nothing is ready. */
    public int highestReadyRank() {
        return highestReadyRank;
    }

    // Roles.

    /** Become the shard's leader, or, when already leading in this epoch, take a newer in-sync set. */
    public void lead(Versioned<ShardState> assigned) {
        List<CompletableFuture<Void>> nowCommitted;
        List<FollowerSession> outdated = List.of();
        lock.lock();
        try {
            if (assigned.version() <= shardState.version()) {
                return;
            }
            boolean alreadyLeading = role == Role.LEADER && epoch == assigned.value().epoch();
            adopt(assigned);
            if (!alreadyLeading) {
                role = Role.LEADER;
                // Everything in a new leader's log is now the shard's history, committed or not.
                applyUpTo(log.endOffset());
                outdated = forgetFollowers();
                Instant now = clock.instant();
                for (ReplicaId member : assigned.value().isr()) {
                    lastCaughtUp.put(member, now);
                }
            }
            highestReadyRank = state.highestReadyRank();
            nowCommitted = advanceHighWatermark();
        } finally {
            lock.unlock();
        }
        outdated.forEach(session -> session.toFollower.onEnd(FetchEnd.of(FetchEnd.Reason.FENCED_EPOCH)));
        complete(nowCommitted);
    }

    /**
     * Follow the leader named in {@code assigned}. A replica that was leading steps down: it ends
     * its followers' streams, fails the callers still waiting for a commit, and rebuilds its state
     * from the committed part of its log, because it had applied entries that were never committed
     * and that the new leader may not have.
     *
     * @return whether the leader or epoch this replica follows has changed
     */
    public boolean follow(Versioned<ShardState> assigned) {
        List<FollowerSession> outdated = List.of();
        List<CompletableFuture<Void>> abandoned = List.of();
        boolean changed;
        lock.lock();
        try {
            if (assigned.version() <= shardState.version()) {
                return false;
            }
            ShardState before = shardState.value();
            changed = role == Role.LEADER || before.epoch() != assigned.value().epoch()
                    || !Objects.equals(before.leader(), assigned.value().leader());
            if (role == Role.LEADER) {
                role = Role.FOLLOWER;
                outdated = forgetFollowers();
                abandoned = new ArrayList<>(commitWaiters.values());
                commitWaiters.clear();
                state = new ShardStateMachine(id.shard(), config.priorityLevels().size());
                applied = 0;
                applyUpTo(log.highWatermark());
                highestReadyRank = -1;
            }
            adopt(assigned);
        } finally {
            lock.unlock();
        }
        outdated.forEach(session -> session.toFollower.onEnd(FetchEnd.of(FetchEnd.Reason.NOT_LEADER)));
        NotLeaderException steppedDown = new NotLeaderException(id);
        abandoned.forEach(waiter -> waiter.completeExceptionally(steppedDown));
        return changed;
    }

    // Leader: requests.

    /**
     * @param priority one of the queue's level names; null means the lowest level
     * @param ttl how long the message may wait; null means the queue's default TTL
     */
    public CompletableFuture<MessageId> enqueue(String payload, String priority, Duration ttl, Acks acks) {
        QueueConfig config = this.config;
        int rank = config.rankOf(priority);
        Duration lifetime = ttl != null ? ttl : config.defaultTtl();
        if (lifetime != null && (lifetime.isNegative() || lifetime.isZero())) {
            throw new IllegalArgumentException("ttl must be positive");
        }
        if (acks == Acks.ALL) {
            requireEnoughReplicas(config);
        }
        CompletableFuture<MessageId> enqueued = write(acks, now -> {
            Instant expiresAt = lifetime == null ? null : now.plus(lifetime);
            long offset = appendAndApply(state.enqueue(payload, rank, expiresAt, now));
            return new MessageId(id.shard(), offset);
        });
        // The in-sync set may have shrunk while the write waited; do not claim more copies than exist.
        return acks == Acks.ALL
                ? enqueued.thenApply(messageId -> {
                    requireEnoughReplicas(config);
                    return messageId;
                })
                : enqueued;
    }

    public CompletableFuture<MessageId> enqueue(String payload, String priority, Duration ttl) {
        return enqueue(payload, priority, ttl, Acks.ALL);
    }

    /** The highest-priority ready message, now in flight; empty when nothing is ready. */
    public CompletableFuture<Optional<MessageView>> dequeue() {
        QueueConfig config = this.config;
        return write(Acks.ALL, now -> {
            reclaim(now, config);
            return state.dequeue(now, config.visibilityTimeout()).map(delivered -> {
                appendAndApply(delivered);
                return state.find(delivered.messageOffset()).orElseThrow();
            });
        });
    }

    /** False when there is nothing to acknowledge: unknown, never delivered, or already gone. */
    public CompletableFuture<Boolean> ack(MessageId messageId) {
        if (messageId.shard() != id.shard()) {
            return CompletableFuture.completedFuture(false);
        }
        return write(Acks.ALL, now -> state.ack(messageId.offset(), now).map(acked -> {
            appendAndApply(acked);
            return true;
        }).orElse(false));
    }

    /** Returns timed-out messages to their lanes, dead-letters or expires what is due. */
    public CompletableFuture<Void> reclaim() {
        QueueConfig config = this.config;
        return write(Acks.ALL, now -> {
            reclaim(now, config);
            return null;
        });
    }

    public ShardMetrics metrics() {
        reclaim();
        lock.lock();
        try {
            requireLeader();
            return state.metrics(clock.instant());
        } finally {
            lock.unlock();
        }
    }

    public List<MessageView> deadLetters() {
        lock.lock();
        try {
            requireLeader();
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

    // Leader: replication to followers.

    /**
     * A follower asks for the log from its fetch offset on. The stream is refused, through
     * {@code toFollower.onEnd}, unless this replica leads the shard in the follower's epoch and
     * the follower's log agrees with this one up to the fetch offset.
     */
    public FetchStream openFetch(FetchRequest request, FetchListener toFollower) {
        List<CompletableFuture<Void>> nowCommitted;
        FollowerSession session;
        lock.lock();
        try {
            int epoch = shardState.value().epoch();
            if (role != Role.LEADER || request.epoch() > epoch) {
                return refuse(toFollower, FetchEnd.of(FetchEnd.Reason.NOT_LEADER));
            }
            if (request.epoch() < epoch) {
                return refuse(toFollower, FetchEnd.of(FetchEnd.Reason.FENCED_EPOCH));
            }
            if (!shardState.value().replicas().contains(request.follower().brokerId())) {
                return refuse(toFollower, FetchEnd.of(FetchEnd.Reason.CLOSED));
            }
            if (!agreesUpTo(request.fetchOffset(), request.lastEntryEpoch())) {
                EpochEnd leaderEpochEnd = log.endOfEpoch(request.lastEntryEpoch()).orElse(new EpochEnd(-1, 0));
                return refuse(toFollower, FetchEnd.diverged(leaderEpochEnd));
            }
            FollowerSession previous = sessions.get(request.follower());
            if (previous != null) {
                previous.closed = true;
            }
            session = new FollowerSession(request.follower(), toFollower, request.fetchOffset());
            sessions.put(request.follower(), session);
            // The logs agree up to the fetch offset, so the follower already has everything below it.
            followerLogEnd.put(request.follower(), request.fetchOffset());
            nowCommitted = advanceHighWatermark();
            sendToAll();
        } finally {
            lock.unlock();
        }
        complete(nowCommitted);
        return session;
    }

    /**
     * What the in-sync set should become, or empty if it is right as it is. A follower leaves when
     * it has been behind for longer than {@code replicaLagMax}, and joins once it has every
     * committed entry. The caller writes the proposal to the coordinator and reports the outcome
     * with {@link #isrChanged} or {@link #isrChangeRefused}; the new set counts only after that.
     */
    public Optional<IsrProposal> proposeIsrChange() {
        lock.lock();
        try {
            if (role != Role.LEADER || isrChangePending) {
                return Optional.empty();
            }
            Instant now = clock.instant();
            ShardState current = shardState.value();
            List<ReplicaId> next = new ArrayList<>();
            for (ReplicaId member : current.isr()) {
                if (member.equals(self) || !hasLaggedTooLong(member, now)) {
                    next.add(member);
                }
            }
            // Only a follower outside the set can join. For one inside it, having every committed
            // entry proves nothing, because it is that follower that holds the high watermark back.
            for (FollowerSession session : sessions.values()) {
                if (!current.isr().contains(session.follower) && session.logEnd >= log.highWatermark()) {
                    next.add(session.follower);
                }
            }
            if (next.equals(current.isr())) {
                return Optional.empty();
            }
            isrChangePending = true;
            pendingIsr = next;
            return Optional.of(new IsrProposal(id, shardState.version(),
                    new ShardState(current.leader(), current.epoch(), next, current.replicas())));
        } finally {
            lock.unlock();
        }
    }

    /** The coordinator accepted the proposed in-sync set; it now decides what is committed. */
    public void isrChanged(Versioned<ShardState> accepted) {
        List<CompletableFuture<Void>> nowCommitted;
        lock.lock();
        try {
            isrChangePending = false;
            pendingIsr = null;
            if (role != Role.LEADER || accepted.version() <= shardState.version()) {
                return;
            }
            Instant now = clock.instant();
            for (ReplicaId member : accepted.value().isr()) {
                lastCaughtUp.putIfAbsent(member, now);
            }
            adopt(accepted);
            nowCommitted = advanceHighWatermark();
            sendToAll();
        } finally {
            lock.unlock();
        }
        complete(nowCommitted);
    }

    public void isrChangeRefused() {
        lock.lock();
        try {
            isrChangePending = false;
            pendingIsr = null;
        } finally {
            lock.unlock();
        }
    }

    // Follower.

    /** What to send the leader when opening a fetch stream. */
    public FetchRequest fetchRequest() {
        lock.lock();
        try {
            long end = log.endOffset();
            int lastEntryEpoch = end == 0 ? -1 : log.read(end - 1, 1).get(0).epoch();
            return new FetchRequest(id, self, shardState.value().epoch(), end, lastEntryEpoch);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stores entries fetched from the leader and applies those that are committed. Returns where
     * the log now ends, which the follower reports back to the leader.
     */
    public long appendFetched(List<LogEntry> entries, long leaderHighWatermark) {
        lock.lock();
        try {
            if (role != Role.FOLLOWER) {
                return log.endOffset();
            }
            for (LogEntry entry : entries) {
                long end = log.endOffset();
                if (entry.offset() > end) {
                    throw new IllegalStateException("shard " + id + ": fetched offset " + entry.offset()
                            + " but the log ends at " + end);
                }
                if (entry.offset() == end) {
                    log.append(entry.epoch(), entry.event());
                }
            }
            long committed = Math.min(leaderHighWatermark, log.endOffset());
            if (committed > log.highWatermark()) {
                log.advanceHighWatermark(committed);
                applyUpTo(committed);
            }
            return log.endOffset();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The leader's log does not match this one. Drops the entries past the point where the two
     * last agree: the end of the given epoch in whichever log ends it sooner. Those entries were
     * never committed, so the state machine has not applied them.
     */
    public void truncateToAgreeWith(EpochEnd leaderEpochEnd) {
        lock.lock();
        try {
            long ownEnd = log.endOfEpoch(leaderEpochEnd.epoch()).map(EpochEnd::endOffset).orElse(0L);
            log.truncate(Math.min(Math.min(ownEnd, leaderEpochEnd.endOffset()), log.endOffset()));
        } finally {
            lock.unlock();
        }
    }

    // Views, valid in either role.

    public Status status() {
        lock.lock();
        try {
            ShardState current = shardState.value();
            return new Status(role, current.epoch(), current.isr(), log.endOffset(), log.highWatermark(),
                    Map.copyOf(followerLogEnd));
        } finally {
            lock.unlock();
        }
    }

    /** The state machine as applied so far, without reclaiming anything. */
    public Snapshot snapshot() {
        lock.lock();
        try {
            return new Snapshot(state.liveMessages(), state.deadLetters(), state.metrics(clock.instant()));
        } finally {
            lock.unlock();
        }
    }

    /** @param followerLogEnds on a leader, where each follower's log ended when it last reported */
    public record Status(Role role, int epoch, List<ReplicaId> isr, long logEnd, long highWatermark,
            Map<ReplicaId, Long> followerLogEnds) {
    }

    public record Snapshot(List<MessageView> liveMessages, List<MessageView> deadLetters, ShardMetrics metrics) {
    }

    /** A new in-sync set for a shard, to be written to the coordinator if its state is still at {@code expectedVersion}. */
    public record IsrProposal(ShardId shard, long expectedVersion, ShardState state) {
    }

    // Internals. Callers hold the lock unless noted.

    /**
     * Runs {@code action} under the shard lock. The returned future completes with the action's
     * result once everything the action appended is committed, or at once for {@link Acks#LEADER}.
     * Takes the lock itself.
     */
    private <T> CompletableFuture<T> write(Acks acks, Function<Instant, T> action) {
        T result;
        CompletableFuture<Void> committed;
        List<CompletableFuture<Void>> nowCommitted;
        lock.lock();
        try {
            requireLeader();
            long before = log.endOffset();
            result = action.apply(clock.instant());
            long end = log.endOffset();
            committed = end == before || acks == Acks.LEADER
                    ? CompletableFuture.completedFuture(null)
                    : commitWaiters.computeIfAbsent(end - 1, offset -> new CompletableFuture<>());
            highestReadyRank = state.highestReadyRank();
            nowCommitted = advanceHighWatermark();
            sendToAll();
        } finally {
            lock.unlock();
        }
        complete(nowCommitted);
        return failAfterCommitTimeout(committed.thenApply(ignored -> result));
    }

    private <T> CompletableFuture<T> failAfterCommitTimeout(CompletableFuture<T> pending) {
        if (pending.isDone()) {
            return pending;
        }
        return pending.orTimeout(commitTimeout.toMillis(), TimeUnit.MILLISECONDS).exceptionallyCompose(failure -> {
            Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                    ? failure.getCause() : failure;
            return CompletableFuture.failedFuture(
                    cause instanceof TimeoutException ? new CommitTimeoutException(id) : cause);
        });
    }

    private void requireLeader() {
        if (role != Role.LEADER) {
            throw new NotLeaderException(id);
        }
    }

    /** Reads volatile fields only, so it needs no lock. */
    private void requireEnoughReplicas(QueueConfig config) {
        int inSync = inSyncReplicas;
        if (inSync < config.minInSyncReplicas()) {
            throw new NotEnoughReplicasException(id, inSync, config.minInSyncReplicas());
        }
    }

    private void reclaim(Instant now, QueueConfig config) {
        for (ShardEvent event : state.reclaim(now, config.maxRetries())) {
            appendAndApply(event);
        }
    }

    private long appendAndApply(ShardEvent event) {
        long offset = log.append(shardState.value().epoch(), event);
        state.apply(offset, event);
        applied = offset + 1;
        return offset;
    }

    private void applyUpTo(long endOffset) {
        while (applied < endOffset) {
            for (LogEntry entry : log.read(applied, (int) Math.min(endOffset - applied, FETCH_BATCH))) {
                state.apply(entry.offset(), entry.event());
                applied = entry.offset() + 1;
            }
        }
    }

    private void adopt(Versioned<ShardState> newer) {
        shardState = newer;
        epoch = newer.value().epoch();
        inSyncReplicas = newer.value().isr().size();
    }

    /** Ends everything a leader tracks about its followers. Returns their sessions, to be told after unlocking. */
    private List<FollowerSession> forgetFollowers() {
        List<FollowerSession> open = new ArrayList<>(sessions.values());
        open.forEach(session -> session.closed = true);
        sessions.clear();
        followerLogEnd.clear();
        lastCaughtUp.clear();
        isrChangePending = false;
        pendingIsr = null;
        return open;
    }

    /**
     * Moves the high watermark to the lowest log end among the in-sync replicas. Returns the
     * waiters that are now committed; the caller completes them after releasing the lock.
     *
     * <p>While a change to the in-sync set is with the coordinator, members of both the old and
     * the proposed set must have an entry before it counts as committed. A replica the
     * coordinator has just accepted can be elected at once, so it must not be missing anything
     * this leader committed in the meantime.
     */
    private List<CompletableFuture<Void>> advanceHighWatermark() {
        long lowestLogEnd = log.endOffset();
        List<ReplicaId> mustHaveIt = new ArrayList<>(shardState.value().isr());
        if (pendingIsr != null) {
            mustHaveIt.addAll(pendingIsr);
        }
        for (ReplicaId member : mustHaveIt) {
            if (!member.equals(self)) {
                lowestLogEnd = Math.min(lowestLogEnd, followerLogEnd.getOrDefault(member, 0L));
            }
        }
        if (lowestLogEnd <= log.highWatermark()) {
            return List.of();
        }
        log.advanceHighWatermark(lowestLogEnd);
        NavigableMap<Long, CompletableFuture<Void>> committed = commitWaiters.headMap(lowestLogEnd, false);
        List<CompletableFuture<Void>> waiters = new ArrayList<>(committed.values());
        committed.clear();
        return waiters;
    }

    /** Does not need the lock. */
    private static void complete(List<CompletableFuture<Void>> waiters) {
        waiters.forEach(waiter -> waiter.complete(null));
    }

    private boolean agreesUpTo(long fetchOffset, int lastEntryEpoch) {
        if (fetchOffset > log.endOffset()) {
            return false;
        }
        return fetchOffset == 0 || log.read(fetchOffset - 1, 1).get(0).epoch() == lastEntryEpoch;
    }

    private static FetchStream refuse(FetchListener toFollower, FetchEnd end) {
        toFollower.onEnd(end);
        return FetchStream.REFUSED;
    }

    private boolean hasLaggedTooLong(ReplicaId follower, Instant now) {
        FollowerSession session = sessions.get(follower);
        if (session != null && session.logEnd >= log.endOffset()) {
            lastCaughtUp.put(follower, now);
            return false;
        }
        Instant caughtUp = lastCaughtUp.computeIfAbsent(follower, unknown -> now);
        return Duration.between(caughtUp, now).compareTo(replicaLagMax) > 0;
    }

    private void sendToAll() {
        sessions.values().forEach(this::send);
    }

    /**
     * Sends the follower its next batch, if it has confirmed the last one. A follower that is up
     * to date is told when the high watermark moves, so that it can apply what became committed.
     */
    private void send(FollowerSession session) {
        if (session.closed || session.logEnd < session.sentUpTo) {
            return;
        }
        long end = log.endOffset();
        long highWatermark = log.highWatermark();
        if (session.sentUpTo < end) {
            List<LogEntry> batch = log.read(session.sentUpTo, FETCH_BATCH);
            session.sentUpTo += batch.size();
            session.leaderEndAtSend = end;
            session.sentAt = clock.instant();
            session.highWatermarkSent = highWatermark;
            session.toFollower.onEntries(batch, highWatermark);
        } else if (highWatermark > session.highWatermarkSent) {
            session.highWatermarkSent = highWatermark;
            session.toFollower.onEntries(List.of(), highWatermark);
        }
    }

    /** Takes the lock itself. */
    private void followerReported(FollowerSession session, long logEndOffset) {
        List<CompletableFuture<Void>> nowCommitted;
        lock.lock();
        try {
            if (session.closed || role != Role.LEADER) {
                return;
            }
            session.logEnd = Math.max(session.logEnd, Math.min(logEndOffset, session.sentUpTo));
            followerLogEnd.put(session.follower, session.logEnd);
            if (session.sentAt != null && session.logEnd >= session.leaderEndAtSend) {
                // The follower has everything this log held when the last batch went out.
                lastCaughtUp.put(session.follower, session.sentAt);
            }
            nowCommitted = advanceHighWatermark();
            sendToAll();
        } finally {
            lock.unlock();
        }
        complete(nowCommitted);
    }

    /** Takes the lock itself. */
    private void followerClosed(FollowerSession session) {
        lock.lock();
        try {
            session.closed = true;
            sessions.remove(session.follower, session);
        } finally {
            lock.unlock();
        }
    }

    /** The leader's view of one follower's fetch stream. */
    private final class FollowerSession implements FetchStream {

        final ReplicaId follower;
        final FetchListener toFollower;
        /** The follower has every entry below this. */
        long logEnd;
        /** The next offset to send. Above {@code logEnd} while a batch is unconfirmed. */
        long sentUpTo;
        long highWatermarkSent = -1;
        /** Where this log ended, and when, at the moment the last batch was sent. */
        long leaderEndAtSend;
        Instant sentAt;
        boolean closed;

        FollowerSession(ReplicaId follower, FetchListener toFollower, long fetchOffset) {
            this.follower = follower;
            this.toFollower = toFollower;
            this.logEnd = fetchOffset;
            this.sentUpTo = fetchOffset;
        }

        @Override
        public void reportLogEnd(long logEndOffset) {
            followerReported(this, logEndOffset);
        }

        @Override
        public void close() {
            followerClosed(this);
        }
    }
}
