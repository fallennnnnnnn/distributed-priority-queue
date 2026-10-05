package com.queue.distributedpriorityqueue.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import com.queue.distributedpriorityqueue.testsupport.TestCluster;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Timeout;

/**
 * Producers and consumers on real threads, each talking to a different broker of a three-broker
 * cluster, against a queue whose shards are replicated on all three.
 */
@Timeout(120)
class ReplicatedConcurrencyTest {

    private static final List<String> BROKERS = List.of("broker-1", "broker-2", "broker-3");
    private static final int SHARDS = 2;
    private static final int PRODUCERS = 3;
    private static final int CONSUMERS = 6;
    private static final int MESSAGES_PER_PRODUCER = 300;
    private static final int TOTAL = PRODUCERS * MESSAGES_PER_PRODUCER;

    private final TestCluster cluster = new TestCluster(3);
    private final ExecutorService threads = Executors.newCachedThreadPool();
    private final CountDownLatch start = new CountDownLatch(1);
    private final AtomicBoolean failed = new AtomicBoolean();

    @AfterEach
    void stop() {
        threads.shutdownNow();
        cluster.close();
    }

    @RepeatedTest(3)
    void nothingIsLostOrDeliveredTwiceAndEveryReplicaAgrees() throws Exception {
        cluster.queues("broker-1").createQueue(QueueConfig.builder("orders").shards(SHARDS).replicationFactor(3)
                .minInSyncReplicas(2).visibilityTimeout(Duration.ofHours(1)).build());
        Set<MessageId> held = ConcurrentHashMap.newKeySet();
        Map<MessageId, AtomicInteger> deliveries = new ConcurrentHashMap<>();
        AtomicInteger acked = new AtomicInteger();
        List<Future<?>> running = new ArrayList<>();

        for (int producer = 0; producer < PRODUCERS; producer++) {
            QueueService queues = cluster.queues(BROKERS.get(producer % BROKERS.size()));
            int id = producer;
            running.add(run(() -> {
                for (int i = 0; i < MESSAGES_PER_PRODUCER; i++) {
                    queues.enqueue("orders", "p" + id + "-" + i, i % 3 == 0 ? "HIGH" : "LOW", null, null).join();
                }
                return null;
            }));
        }
        for (int consumer = 0; consumer < CONSUMERS; consumer++) {
            QueueService queues = cluster.queues(BROKERS.get(consumer % BROKERS.size()));
            running.add(run(() -> {
                while (acked.get() < TOTAL && !failed.get()) {
                    Optional<MessageView> delivery = queues.dequeue("orders", Duration.ZERO).join();
                    if (delivery.isEmpty()) {
                        Thread.onSpinWait();
                        continue;
                    }
                    MessageId id = delivery.get().id();
                    assertThat(held.add(id)).as("%s is held by another consumer", id).isTrue();
                    deliveries.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
                    held.remove(id);
                    assertThat(queues.ack("orders", id).join()).as("ack of %s", id).isTrue();
                    acked.incrementAndGet();
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<?> thread : running) {
            thread.get(100, TimeUnit.SECONDS);
        }

        assertThat(deliveries).hasSize(TOTAL);
        assertThat(deliveries.values()).allSatisfy(count -> assertThat(count.get()).isEqualTo(1));
        QueueMetrics metrics = cluster.queues("broker-2").metrics("orders").join();
        assertThat(metrics.enqueuedTotal()).isEqualTo(TOTAL);
        assertThat(metrics.ackedTotal()).isEqualTo(TOTAL);
        assertThat(metrics.readyTotal()).isZero();
        assertThat(metrics.inFlight()).isZero();
        for (int number = 0; number < SHARDS; number++) {
            ShardId shard = new ShardId("orders", number);
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                ShardReplica onLeader = cluster.leaderOf(shard).replica(shard).orElseThrow();
                assertThat(cluster.followersOf(shard)).hasSize(2).allSatisfy(follower -> {
                    ShardReplica onFollower = follower.replica(shard).orElseThrow();
                    assertThat(onFollower.status().highWatermark()).isEqualTo(onLeader.status().logEnd());
                    assertThat(onFollower.snapshot()).isEqualTo(onLeader.snapshot());
                });
            });
        }
    }

    private <T> Future<T> run(Callable<T> task) {
        return threads.submit(() -> {
            start.await();
            try {
                return task.call();
            } catch (Throwable failure) {
                failed.set(true);
                throw failure;
            }
        });
    }
}
