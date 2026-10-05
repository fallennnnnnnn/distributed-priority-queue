package com.queue.distributedpriorityqueue.replication;

import com.queue.distributedpriorityqueue.log.LogEntry;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps a follower replica's fetch stream to the shard's leader open. Whenever the stream ends,
 * for whatever reason, it opens a new one from wherever the follower's log now ends.
 */
public final class FollowerFetcher {

    private static final Logger log = LoggerFactory.getLogger(FollowerFetcher.class);

    /** Opens a fetch stream on the shard's current leader. Empty when no leader can be reached. */
    public interface LeaderConnection extends BiFunction<FetchRequest, FetchListener, Optional<FetchStream>> {
    }

    private final ShardReplica replica;
    private final LeaderConnection leader;
    /** Runs when the leader turned this follower away, so that it reads the shard's state again. */
    private final Runnable refreshShardState;
    private final ScheduledExecutorService scheduler;
    private final Duration retryDelay;

    private FetchStream stream;
    /** A log end that could not be reported because the stream was still being opened. */
    private Long unreportedLogEnd;
    /** Identifies the current stream, so that late calls from an earlier one are ignored. */
    private Object current;
    private boolean stopped;

    public FollowerFetcher(ShardReplica replica, LeaderConnection leader, Runnable refreshShardState,
            ScheduledExecutorService scheduler, Duration retryDelay) {
        this.replica = replica;
        this.leader = leader;
        this.refreshShardState = refreshShardState;
        this.scheduler = scheduler;
        this.retryDelay = retryDelay;
    }

    public synchronized void start() {
        connect();
    }

    public synchronized void stop() {
        stopped = true;
        current = null;
        if (stream != null) {
            stream.close();
            stream = null;
        }
    }

    private synchronized void connect() {
        if (stopped || replica.isLeader()) {
            return;
        }
        Object attempt = new Object();
        current = attempt;
        stream = null;
        unreportedLogEnd = null;
        try {
            Optional<FetchStream> opened = leader.apply(replica.fetchRequest(), new FetchListener() {
                @Override
                public void onEntries(List<LogEntry> entries, long highWatermark) {
                    received(attempt, entries, highWatermark);
                }

                @Override
                public void onEnd(FetchEnd end) {
                    ended(attempt, end);
                }
            });
            if (opened.isEmpty()) {
                retryLater(attempt);
            } else if (current == attempt) {
                stream = opened.get();
                if (unreportedLogEnd != null) {
                    stream.reportLogEnd(unreportedLogEnd);
                    unreportedLogEnd = null;
                }
            } else {
                // The stream ended while it was being opened.
                opened.get().close();
            }
        } catch (RuntimeException e) {
            log.debug("Shard {}: could not open a fetch stream", replica.id(), e);
            retryLater(attempt);
        }
    }

    private void received(Object attempt, List<LogEntry> entries, long highWatermark) {
        FetchStream toLeader;
        long logEnd;
        synchronized (this) {
            if (current != attempt) {
                return;
            }
            try {
                logEnd = replica.appendFetched(entries, highWatermark);
            } catch (RuntimeException e) {
                log.warn("Shard {}: could not store fetched entries; reopening the stream", replica.id(), e);
                if (stream != null) {
                    stream.close();
                }
                retryLater(attempt);
                return;
            }
            toLeader = stream;
            if (toLeader == null && !entries.isEmpty()) {
                unreportedLogEnd = logEnd;
            }
        }
        if (toLeader != null && !entries.isEmpty()) {
            toLeader.reportLogEnd(logEnd);
        }
    }

    private synchronized void ended(Object attempt, FetchEnd end) {
        if (current != attempt) {
            return;
        }
        if (end.reason() == FetchEnd.Reason.DIVERGED) {
            replica.truncateToAgreeWith(end.leaderEpochEnd());
            connect();
            return;
        }
        if (end.reason() != FetchEnd.Reason.CLOSED) {
            refreshShardState.run();
        }
        retryLater(attempt);
    }

    private synchronized void retryLater(Object attempt) {
        if (stopped || current != attempt) {
            return;
        }
        current = null;
        stream = null;
        try {
            scheduler.schedule(this::connect, retryDelay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // The broker is shutting down.
        }
    }
}
