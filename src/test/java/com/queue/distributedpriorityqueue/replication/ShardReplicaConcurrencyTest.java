package com.queue.distributedpriorityqueue.replication;

import static org.assertj.core.api.Assertions.assertThat;

import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.log.InMemoryShardLog;
import com.queue.distributedpriorityqueue.testsupport.ManualClock;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Real threads against one shard. Each test starts every thread from the same latch, runs until
 * the work is finished and then checks invariants that must hold however the threads interleaved.
 */
@Timeout(60)
class ShardReplicaConcurrencyTest {

    private static final List<String> LEVELS = List.of("HIGH", "MEDIUM", "LOW");
    private static final int PRODUCERS = 4;
    private static final int CONSUMERS = 8;
    private static final int MESSAGES_PER_PRODUCER = 500;
    private static final int TOTAL = PRODUCERS * MESSAGES_PER_PRODUCER;

    private final ExecutorService threads = Executors.newCachedThreadPool();
    private final CountDownLatch start = new CountDownLatch(1);
    private final List<Future<?>> running = new ArrayList<>();
    /** Set when any thread fails, so the others stop instead of waiting for work that will never finish. */
    private final AtomicBoolean failed = new AtomicBoolean();

    @AfterEach
    void stopThreads() {
        threads.shutdownNow();
    }

    @RepeatedTest(5)
    void noMessageIsLostOrHeldByTwoConsumersAtOnce() throws Exception {
        ShardReplica replica = new ShardReplica(0,
                QueueConfig.builder("orders").visibilityTimeout(Duration.ofHours(1)).build(),
                new InMemoryShardLog(), Clock.systemUTC());
        Set<MessageId> held = ConcurrentHashMap.newKeySet();
        Map<MessageId, AtomicInteger> deliveries = new ConcurrentHashMap<>();
        AtomicInteger acked = new AtomicInteger();
        AtomicBoolean done = new AtomicBoolean();

        startProducers(replica);
        for (int consumer = 0; consumer < CONSUMERS; consumer++) {
            run(() -> {
                while (acked.get() < TOTAL && !failed.get()) {
                    Optional<MessageView> delivery = replica.dequeue().join();
                    if (delivery.isEmpty()) {
                        Thread.onSpinWait();
                        continue;
                    }
                    MessageId id = delivery.get().id();
                    assertThat(held.add(id)).as("%s is held by another consumer", id).isTrue();
                    deliveries.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
                    Thread.yield();
                    held.remove(id);
                    assertThat(replica.ack(id).join()).as("ack of %s", id).isTrue();
                    acked.incrementAndGet();
                }
                return null;
            });
        }
        run(() -> {
            while (!done.get() && !failed.get()) {
                assertEveryMessageIsAccountedFor(replica.metrics());
            }
            return null;
        });

        start.countDown();
        awaitAllBut(1);
        done.set(true);
        awaitAllBut(0);

        assertThat(deliveries).hasSize(TOTAL);
        assertThat(deliveries.values()).allSatisfy(count -> assertThat(count.get()).isEqualTo(1));
        ShardMetrics metrics = replica.metrics();
        assertThat(metrics.enqueuedTotal()).isEqualTo(TOTAL);
        assertThat(metrics.ackedTotal()).isEqualTo(TOTAL);
        assertThat(metrics.ready()).isZero();
        assertThat(metrics.inFlight()).isZero();
        assertThat(replica.dequeue().join()).isEmpty();
    }

    @RepeatedTest(5)
    void everyMessageEndsAckedOrDeadLetteredWhenConsumersSkipAcks() throws Exception {
        ManualClock clock = new ManualClock();
        int maxRetries = 2;
        Duration visibility = Duration.ofSeconds(30);
        ShardReplica replica = new ShardReplica(0,
                QueueConfig.builder("orders").visibilityTimeout(visibility).maxRetries(maxRetries).build(),
                new InMemoryShardLog(), clock);
        AtomicBoolean done = new AtomicBoolean();

        startProducers(replica);
        for (int consumer = 0; consumer < CONSUMERS; consumer++) {
            run(() -> {
                while (!done.get() && !failed.get()) {
                    Optional<MessageView> delivery = replica.dequeue().join();
                    if (delivery.isPresent() && ThreadLocalRandom.current().nextInt(100) < 60) {
                        replica.ack(delivery.get().id()).join();
                    }
                }
                return null;
            });
        }
        run(() -> {
            while (!done.get() && !failed.get()) {
                clock.advance(visibility);
                ShardMetrics metrics = replica.metrics();
                assertEveryMessageIsAccountedFor(metrics);
                done.set(metrics.ackedTotal() + metrics.deadLetteredTotal() == TOTAL);
            }
            return null;
        });

        start.countDown();
        awaitAllBut(0);

        ShardMetrics metrics = replica.metrics();
        assertThat(metrics.enqueuedTotal()).isEqualTo(TOTAL);
        assertThat(metrics.ackedTotal() + metrics.deadLetteredTotal()).isEqualTo(TOTAL);
        assertThat(metrics.ready()).isZero();
        assertThat(metrics.inFlight()).isZero();
        assertThat(metrics.deadLetteredTotal()).isPositive();
        List<MessageView> deadLetters = replica.deadLetters();
        assertThat(deadLetters).hasSize((int) metrics.deadLetteredTotal());
        assertThat(deadLetters).allSatisfy(
                deadLetter -> assertThat(deadLetter.deliveryCount()).isEqualTo(maxRetries + 1));
    }

    @RepeatedTest(300)
    void anAckRacingTheVisibilityTimeoutLeavesAConsistentState(RepetitionInfo repetition) throws Exception {
        ManualClock clock = new ManualClock();
        Duration visibility = Duration.ofSeconds(30);
        ShardReplica replica = new ShardReplica(0,
                QueueConfig.builder("orders").visibilityTimeout(visibility).build(), new InMemoryShardLog(), clock);
        MessageId id = replica.enqueue("a", "HIGH", null).join();
        replica.dequeue().join();
        clock.advance(visibility);

        // The deadline has passed. The first consumer's late ack races a second consumer's dequeue,
        // which is what returns the message to the queue and hands it out again. The thread started
        // first usually wins, so the start order alternates to exercise both outcomes.
        Callable<Boolean> lateAck = () -> replica.ack(id).join();
        Callable<Optional<MessageView>> secondDequeue = () -> replica.dequeue().join();
        Future<Boolean> ack;
        Future<Optional<MessageView>> redelivery;
        if (repetition.getCurrentRepetition() % 2 == 0) {
            ack = run(lateAck);
            redelivery = run(secondDequeue);
        } else {
            redelivery = run(secondDequeue);
            ack = run(lateAck);
        }
        start.countDown();

        // The ack counts whichever side wins.
        assertThat(ack.get()).isTrue();
        redelivery.get().ifPresent(second -> {
            assertThat(second.id()).isEqualTo(id);
            assertThat(second.deliveryCount()).isEqualTo(2);
            assertThat(replica.ack(id).join()).as("the message is already acked").isFalse();
        });
        ShardMetrics metrics = replica.metrics();
        assertThat(metrics.ackedTotal()).isEqualTo(1);
        assertThat(metrics.ready()).isZero();
        assertThat(metrics.inFlight()).isZero();
        assertEveryMessageIsAccountedFor(metrics);
    }

    private void startProducers(ShardReplica replica) {
        for (int producer = 0; producer < PRODUCERS; producer++) {
            int id = producer;
            run(() -> {
                for (int i = 0; i < MESSAGES_PER_PRODUCER; i++) {
                    String level = LEVELS.get(ThreadLocalRandom.current().nextInt(LEVELS.size()));
                    replica.enqueue("p" + id + "-" + i, level, null).join();
                }
                return null;
            });
        }
    }

    private <T> Future<T> run(Callable<T> task) {
        Future<T> future = threads.submit(() -> {
            start.await();
            try {
                return task.call();
            } catch (Throwable failure) {
                failed.set(true);
                throw failure;
            }
        });
        running.add(future);
        return future;
    }

    /** Waits for the threads started so far, except the last {@code stillRunning}. Rethrows their failures. */
    private void awaitAllBut(int stillRunning) throws Exception {
        for (Future<?> future : running.subList(0, running.size() - stillRunning)) {
            try {
                future.get(60, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof AssertionError assertion) {
                    throw assertion;
                }
                throw e;
            }
        }
    }

    private static void assertEveryMessageIsAccountedFor(ShardMetrics metrics) {
        assertThat(metrics.enqueuedTotal()).isEqualTo(metrics.ready() + metrics.inFlight()
                + metrics.ackedTotal() + metrics.deadLetteredTotal() + metrics.expiredTotal());
    }
}
