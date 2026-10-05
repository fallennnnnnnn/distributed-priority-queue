package com.queue.distributedpriorityqueue.testsupport;

import com.queue.distributedpriorityqueue.cluster.BrokerApi;
import com.queue.distributedpriorityqueue.cluster.BrokerTransport;
import com.queue.distributedpriorityqueue.coordination.BrokerInfo;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.exception.BrokerUnreachableException;
import com.queue.distributedpriorityqueue.log.LogEntry;
import com.queue.distributedpriorityqueue.replication.Acks;
import com.queue.distributedpriorityqueue.replication.FetchEnd;
import com.queue.distributedpriorityqueue.replication.FetchListener;
import com.queue.distributedpriorityqueue.replication.FetchRequest;
import com.queue.distributedpriorityqueue.replication.FetchStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * Connects brokers that run in the same JVM, in place of a real network, and lets a test break it.
 * Calls are carried out on other threads, as they would be over a network.
 *
 * <ul>
 *   <li>{@link #partition} stops two brokers from reaching each other: calls fail and their
 *       replication streams end, the way a broken connection does.
 *   <li>{@link #hold} makes replication messages in one direction wait, which is how a test
 *       plays a follower that has fallen behind without the leader seeing a broken stream.
 *   <li>{@link #delay} slows every delivery down.
 * </ul>
 */
public final class InProcessNetwork implements AutoCloseable {

    private final Map<String, BrokerApi> brokers = new ConcurrentHashMap<>();
    /** Pairs of brokers that cannot reach each other. */
    private final Set<Set<String>> cut = ConcurrentHashMap.newKeySet();
    /** Directions, written "from>to", in which replication messages wait. */
    private final Set<String> held = ConcurrentHashMap.newKeySet();
    private final List<Stream> streams = new CopyOnWriteArrayList<>();
    private final ExecutorService threads = Executors.newCachedThreadPool(task -> {
        Thread thread = new Thread(task, "in-process-network");
        thread.setDaemon(true);
        return thread;
    });
    private volatile Duration delay = Duration.ZERO;

    /** Makes a broker reachable under its ID. */
    public void register(String brokerId, BrokerApi api) {
        brokers.put(brokerId, api);
    }

    /** The broker is gone: calls to it fail and its replication streams end. */
    public void unregister(String brokerId) {
        brokers.remove(brokerId);
        streams.stream().filter(stream -> stream.touches(brokerId)).forEach(Stream::breakConnection);
    }

    /** The transport a broker uses to reach the others. */
    public BrokerTransport transportFor(String brokerId) {
        return new BrokerTransport() {
            @Override
            public BrokerApi connect(BrokerInfo target) {
                return new RemoteBroker(brokerId, target.id());
            }

            @Override
            public void close() {
            }
        };
    }

    public void partition(String a, String b) {
        cut.add(Set.of(a, b));
        streams.stream().filter(stream -> stream.touches(a) && stream.touches(b)).forEach(Stream::breakConnection);
    }

    /** Cuts a broker off from every other broker. */
    public void isolate(String brokerId) {
        brokers.keySet().stream().filter(other -> !other.equals(brokerId)).forEach(other -> partition(brokerId, other));
    }

    /** Ends every partition. Streams that broke are reopened by the followers, not by the network. */
    public void heal() {
        cut.clear();
    }

    /** Replication messages from one broker to another wait until {@link #release}. */
    public void hold(String from, String to) {
        held.add(from + ">" + to);
        streams.forEach(stream -> stream.lane(from, to).ifPresent(Lane::hold));
    }

    public void release(String from, String to) {
        held.remove(from + ">" + to);
        streams.forEach(stream -> stream.lane(from, to).ifPresent(Lane::release));
    }

    public void delay(Duration delay) {
        this.delay = delay;
    }

    @Override
    public void close() {
        threads.shutdownNow();
    }

    private boolean reachable(String from, String to) {
        return brokers.containsKey(to) && !cut.contains(Set.of(from, to));
    }

    private void pause() {
        Duration delay = this.delay;
        if (!delay.isZero()) {
            try {
                Thread.sleep(delay.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** One broker as another broker reaches it. */
    private final class RemoteBroker implements BrokerApi {

        private final String from;
        private final String to;

        RemoteBroker(String from, String to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public CompletableFuture<MessageId> enqueue(ShardId shard, String payload, String priority, Duration ttl,
                Acks acks) {
            return call(broker -> broker.enqueue(shard, payload, priority, ttl, acks));
        }

        @Override
        public CompletableFuture<Optional<MessageView>> dequeue(ShardId shard) {
            return call(broker -> broker.dequeue(shard));
        }

        @Override
        public CompletableFuture<Boolean> ack(ShardId shard, MessageId messageId) {
            return call(broker -> broker.ack(shard, messageId));
        }

        @Override
        public CompletableFuture<ShardMetrics> metrics(ShardId shard) {
            return call(broker -> broker.metrics(shard));
        }

        @Override
        public CompletableFuture<List<MessageView>> deadLetters(ShardId shard) {
            return call(broker -> broker.deadLetters(shard));
        }

        @Override
        public CompletableFuture<Map<ShardId, Integer>> readySummary() {
            return call(BrokerApi::readySummary);
        }

        @Override
        public FetchStream openFetch(FetchRequest request, FetchListener toFollower) {
            BrokerApi leader = brokers.get(to);
            if (leader == null || !reachable(from, to)) {
                throw new BrokerUnreachableException(to);
            }
            Stream stream = new Stream(from, to, toFollower);
            streams.add(stream);
            stream.open(leader, request);
            return stream;
        }

        /** The request and the answer both cross the network, so either can be lost to a partition. */
        private <T> CompletableFuture<T> call(Function<BrokerApi, CompletableFuture<T>> request) {
            CompletableFuture<T> answer = new CompletableFuture<>();
            threads.execute(() -> {
                pause();
                BrokerApi broker = brokers.get(to);
                if (broker == null || !reachable(from, to)) {
                    answer.completeExceptionally(new BrokerUnreachableException(to));
                    return;
                }
                try {
                    request.apply(broker).whenComplete((value, error) -> {
                        if (!reachable(from, to)) {
                            answer.completeExceptionally(new BrokerUnreachableException(to));
                        } else if (error != null) {
                            answer.completeExceptionally(error);
                        } else {
                            answer.complete(value);
                        }
                    });
                } catch (RuntimeException e) {
                    answer.completeExceptionally(e);
                }
            });
            return answer;
        }
    }

    /** A replication stream between a follower and a leader. It is the follower's handle on it. */
    private final class Stream implements FetchStream {

        private final String follower;
        private final String leader;
        private final FetchListener followerListener;
        private final Lane toFollower;
        private final Lane toLeader;
        private volatile FetchStream leaderSide = FetchStream.REFUSED;
        private volatile boolean over;

        Stream(String follower, String leader, FetchListener followerListener) {
            this.follower = follower;
            this.leader = leader;
            this.followerListener = followerListener;
            this.toFollower = new Lane(held.contains(leader + ">" + follower));
            this.toLeader = new Lane(held.contains(follower + ">" + leader));
        }

        void open(BrokerApi leaderBroker, FetchRequest request) {
            leaderSide = leaderBroker.openFetch(request, new FetchListener() {
                @Override
                public void onEntries(List<LogEntry> entries, long highWatermark) {
                    toFollower.submit(() -> {
                        if (!over) {
                            followerListener.onEntries(entries, highWatermark);
                        }
                    });
                }

                @Override
                public void onEnd(FetchEnd end) {
                    toFollower.submit(() -> {
                        if (!over) {
                            finish();
                            followerListener.onEnd(end);
                        }
                    });
                }
            });
        }

        @Override
        public void reportLogEnd(long logEndOffset) {
            toLeader.submit(() -> {
                if (!over) {
                    leaderSide.reportLogEnd(logEndOffset);
                }
            });
        }

        @Override
        public void close() {
            if (!over) {
                finish();
                threads.execute(() -> leaderSide.close());
            }
        }

        /** Both ends learn that the connection is gone. */
        void breakConnection() {
            if (!over) {
                finish();
                threads.execute(() -> {
                    leaderSide.close();
                    followerListener.onEnd(FetchEnd.of(FetchEnd.Reason.CLOSED));
                });
            }
        }

        boolean touches(String brokerId) {
            return follower.equals(brokerId) || leader.equals(brokerId);
        }

        Optional<Lane> lane(String from, String to) {
            if (from.equals(leader) && to.equals(follower)) {
                return Optional.of(toFollower);
            }
            if (from.equals(follower) && to.equals(leader)) {
                return Optional.of(toLeader);
            }
            return Optional.empty();
        }

        private void finish() {
            over = true;
            streams.remove(this);
        }
    }

    /** Delivers one direction of a stream in order, on one thread at a time, unless it is held. */
    private final class Lane {

        private final Deque<Runnable> waiting = new ArrayDeque<>();
        private boolean delivering;
        private boolean held;

        Lane(boolean held) {
            this.held = held;
        }

        synchronized void submit(Runnable delivery) {
            waiting.add(delivery);
            deliverIfIdle();
        }

        synchronized void hold() {
            held = true;
        }

        synchronized void release() {
            held = false;
            deliverIfIdle();
        }

        private void deliverIfIdle() {
            if (!delivering && !held && !waiting.isEmpty()) {
                delivering = true;
                threads.execute(this::deliver);
            }
        }

        private void deliver() {
            while (true) {
                Runnable next;
                synchronized (this) {
                    if (held || waiting.isEmpty()) {
                        delivering = false;
                        return;
                    }
                    next = waiting.poll();
                }
                pause();
                try {
                    next.run();
                } catch (RuntimeException e) {
                    // A failing delivery must not stop the ones behind it.
                }
            }
        }
    }
}
