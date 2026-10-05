package com.queue.distributedpriorityqueue.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.within;
import static org.awaitility.Awaitility.await;

import com.queue.distributedpriorityqueue.coordination.InMemoryCoordinator;
import com.queue.distributedpriorityqueue.coordination.ShardState;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.exception.QueueAlreadyExistsException;
import com.queue.distributedpriorityqueue.exception.QueueNotFoundException;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import com.queue.distributedpriorityqueue.testsupport.ManualClock;
import com.queue.distributedpriorityqueue.testsupport.TestBrokers;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** One broker, built without Spring, with time moved by hand. */
class QueueServiceTest {

    private static final Duration VISIBILITY = Duration.ofSeconds(30);

    private final ManualClock clock = new ManualClock();
    private final InMemoryCoordinator coordinator = new InMemoryCoordinator();
    private final Broker broker = new Broker(TestBrokers.settings("broker-1", 8080), coordinator, clock);
    private final QueueService queues = broker.queues();

    @BeforeEach
    void start() {
        broker.start();
    }

    @AfterEach
    void stop() {
        broker.close();
        coordinator.close();
    }

    @Nested
    class QueueAdministration {

        @Test
        void createsAQueueLedByThisBroker() {
            QueueMetadata created = queues.createQueue(QueueConfig.builder("orders").shards(3).build());

            assertThat(created.config().name()).isEqualTo("orders");
            assertThat(created.shards()).hasSize(3);
            for (ShardState shard : created.shards()) {
                assertThat(shard.leader()).isEqualTo(broker.info().replicaId());
                assertThat(shard.epoch()).isZero();
                assertThat(shard.isr()).containsExactly(broker.info().replicaId());
                assertThat(shard.replicas()).containsExactly("broker-1");
            }
            assertThat(queues.describe("orders")).isEqualTo(created);
        }

        @Test
        void aQueueNameCanBeUsedOnlyOnce() {
            queues.createQueue(QueueConfig.builder("orders").build());

            assertThatExceptionOfType(QueueAlreadyExistsException.class)
                    .isThrownBy(() -> queues.createQueue(QueueConfig.builder("orders").maxRetries(9).build()));
            assertThat(queues.describe("orders").config().maxRetries()).isEqualTo(3);
        }

        @Test
        void replicationCannotExceedTheLiveBrokers() {
            assertThatIllegalArgumentException().isThrownBy(() -> queues.createQueue(
                    QueueConfig.builder("orders").replicationFactor(2).minInSyncReplicas(1).build()));
            assertThatExceptionOfType(QueueNotFoundException.class).isThrownBy(() -> queues.describe("orders"));
        }

        @Test
        void everyOperationRejectsAnUnknownQueue() {
            assertUnknown(() -> queues.describe("nope"));
            assertUnknown(() -> queues.updateQueue("nope", config -> config));
            assertUnknown(() -> queues.enqueue("nope", "a", null, null, null));
            assertUnknown(() -> queues.dequeue("nope", Duration.ZERO));
            assertUnknown(() -> queues.ack("nope", new MessageId(0, 0)));
            assertUnknown(() -> queues.metrics("nope"));
            assertUnknown(() -> queues.deadLetters("nope"));
        }

        @Test
        void aChangedSettingAppliesToFutureDeliveries() {
            queues.createQueue(QueueConfig.builder("orders").build());
            enqueue("orders", "a", "HIGH");
            enqueue("orders", "b", "HIGH");
            MessageView before = dequeue("orders").orElseThrow();

            QueueMetadata updated = queues.updateQueue("orders",
                    config -> config.visibilityTimeout(Duration.ofSeconds(5)).maxRetries(7));
            MessageView after = dequeue("orders").orElseThrow();

            assertThat(updated.config().visibilityTimeout()).isEqualTo(Duration.ofSeconds(5));
            assertThat(updated.config().maxRetries()).isEqualTo(7);
            assertThat(queues.describe("orders")).isEqualTo(updated);
            assertThat(before.visibleAt()).isEqualTo(clock.instant().plus(VISIBILITY));
            assertThat(after.visibleAt()).isEqualTo(clock.instant().plusSeconds(5));
        }

        @Test
        void settingsFixedAtCreationCannotChange() {
            queues.createQueue(QueueConfig.builder("orders").build());

            assertThatIllegalArgumentException()
                    .isThrownBy(() -> queues.updateQueue("orders", config -> config.shards(2)));
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> queues.updateQueue("orders", config -> config.priorityLevels("a", "b", "c")));
            assertThatIllegalArgumentException().isThrownBy(
                    () -> queues.updateQueue("orders", config -> config.replicationFactor(2).minInSyncReplicas(2)));
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> queues.updateQueue("orders", config -> config.maxRetries(-1)));
        }

        private void assertUnknown(Runnable call) {
            assertThatExceptionOfType(QueueNotFoundException.class).isThrownBy(call::run);
        }
    }

    @Nested
    class Sharding {

        @Test
        void messagesWithTheSameKeyShareAShardAndKeepTheirOrder() {
            queues.createQueue(QueueConfig.builder("orders").shards(4).build());
            List<MessageId> ids = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                ids.add(queues.enqueue("orders", "customer-7 #" + i, "MEDIUM", null, "customer-7").join());
            }

            assertThat(ids).extracting(MessageId::shard).containsOnly(ids.get(0).shard());
            List<String> delivered = drain("orders");
            assertThat(delivered).hasSize(20);
            for (int i = 0; i < 20; i++) {
                assertThat(delivered.get(i)).isEqualTo("customer-7 #" + i);
            }
        }

        @Test
        void differentKeysSpreadOverTheShards() {
            queues.createQueue(QueueConfig.builder("orders").shards(4).build());
            Map<Integer, Integer> perShard = new HashMap<>();
            for (int i = 0; i < 400; i++) {
                MessageId id = queues.enqueue("orders", "a", "MEDIUM", null, "customer-" + i).join();
                perShard.merge(id.shard(), 1, Integer::sum);
            }

            assertThat(perShard).hasSize(4);
            assertThat(perShard.values()).allSatisfy(count -> assertThat(count).isBetween(60, 140));
        }

        @Test
        void messagesWithoutAKeyTakeTurnsAcrossShards() {
            queues.createQueue(QueueConfig.builder("orders").shards(4).build());
            Map<Integer, Integer> perShard = new HashMap<>();
            for (int i = 0; i < 400; i++) {
                perShard.merge(enqueue("orders", "a", "MEDIUM").shard(), 1, Integer::sum);
            }

            assertThat(perShard).containsOnly(Map.entry(0, 100), Map.entry(1, 100), Map.entry(2, 100),
                    Map.entry(3, 100));
        }

        @Test
        void aSingleShardIsStrictlyOrdered() {
            queues.createQueue(QueueConfig.builder("orders").build());
            enqueue("orders", "low-1", "LOW");
            enqueue("orders", "high-1", "HIGH");
            enqueue("orders", "medium-1", "MEDIUM");
            enqueue("orders", "high-2", "HIGH");
            enqueue("orders", "low-2", "LOW");

            assertThat(drain("orders")).containsExactly("high-1", "high-2", "medium-1", "low-1", "low-2");
        }

        @Test
        void aHighPriorityMessageInAnyShardIsServedBeforeLowerOnes() {
            queues.createQueue(QueueConfig.builder("orders").shards(4).build());
            for (int i = 0; i < 40; i++) {
                enqueue("orders", "low-" + i, "LOW");
            }
            for (int i = 0; i < 8; i++) {
                enqueue("orders", "medium-" + i, "MEDIUM");
            }
            enqueue("orders", "high", "HIGH");

            List<String> delivered = drain("orders");

            assertThat(delivered).hasSize(49);
            assertThat(delivered.get(0)).isEqualTo("high");
            assertThat(delivered.subList(1, 9)).allSatisfy(payload -> assertThat(payload).startsWith("medium-"));
            assertThat(delivered.subList(9, 49)).allSatisfy(payload -> assertThat(payload).startsWith("low-"));
        }

        @Test
        void dequeuesTakeTurnsAcrossShardsOfEqualPriority() {
            queues.createQueue(QueueConfig.builder("orders").shards(4).build());
            for (int i = 0; i < 8; i++) {
                enqueue("orders", "a", "MEDIUM");
            }

            List<Integer> shards = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                shards.add(dequeue("orders").orElseThrow().id().shard());
            }

            assertThat(shards).containsExactlyInAnyOrder(0, 1, 2, 3);
        }

        @Test
        void anAckIsRoutedByTheShardInTheMessageId() {
            queues.createQueue(QueueConfig.builder("orders").shards(4).build());
            for (int i = 0; i < 4; i++) {
                enqueue("orders", "a", "MEDIUM");
            }
            List<MessageId> delivered = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                delivered.add(dequeue("orders").orElseThrow().id());
            }

            assertThat(delivered).allSatisfy(id -> assertThat(queues.ack("orders", id).join()).isTrue());

            assertThat(queues.ack("orders", new MessageId(9, 0)).join()).isFalse();
            assertThat(queues.metrics("orders").ackedTotal()).isEqualTo(4);
        }
    }

    @Nested
    class AcrossShards {

        @Test
        void metricsAddUpTheShards() {
            queues.createQueue(QueueConfig.builder("orders").shards(3).maxRetries(0).build());
            enqueue("orders", "h", "HIGH");
            clock.advance(Duration.ofSeconds(4));
            for (int i = 0; i < 5; i++) {
                enqueue("orders", "l" + i, "LOW");
            }
            clock.advance(Duration.ofSeconds(6));
            MessageView first = dequeue("orders").orElseThrow();
            queues.ack("orders", first.id()).join();

            QueueMetrics metrics = queues.metrics("orders");

            assertThat(first.payload()).isEqualTo("h");
            assertThat(metrics.queue()).isEqualTo("orders");
            assertThat(metrics.ready()).containsExactly(Map.entry("HIGH", 0L), Map.entry("MEDIUM", 0L),
                    Map.entry("LOW", 5L));
            assertThat(metrics.readyTotal()).isEqualTo(5);
            assertThat(metrics.inFlight()).isZero();
            assertThat(metrics.oldestMessageAgeSeconds()).isEqualTo(6.0);
            assertThat(metrics.enqueuedTotal()).isEqualTo(6);
            assertThat(metrics.deliveredTotal()).isEqualTo(1);
            assertThat(metrics.ackedTotal()).isEqualTo(1);
            assertThat(metrics.enqueuedPerSecond()).isCloseTo(6 / 60.0, within(1e-9));
            assertThat(metrics.ackedPerSecond()).isCloseTo(1 / 60.0, within(1e-9));
        }

        @Test
        void deadLettersAreCollectedFromEveryShard() {
            queues.createQueue(QueueConfig.builder("orders").shards(3).maxRetries(0).build());
            for (int i = 0; i < 6; i++) {
                enqueue("orders", "poison-" + i, "HIGH");
            }
            drain("orders");

            clock.advance(VISIBILITY);

            assertThat(dequeue("orders")).isEmpty();
            assertThat(queues.deadLetters("orders")).extracting(MessageView::payload).containsExactlyInAnyOrder(
                    "poison-0", "poison-1", "poison-2", "poison-3", "poison-4", "poison-5");
            QueueMetrics metrics = queues.metrics("orders");
            assertThat(metrics.deadLetters()).isEqualTo(6);
            assertThat(metrics.deadLetteredTotal()).isEqualTo(6);
        }
    }

    @Nested
    class Sweeper {

        private final List<ShardReplica> replicas = new CopyOnWriteArrayList<>();

        @BeforeEach
        void captureReplicas() {
            broker.onReplicaCreated(replicas::add);
        }

        @Test
        void aSweepReturnsATimedOutMessageWithoutAnyRequest() {
            queues.createQueue(QueueConfig.builder("orders").build());
            enqueue("orders", "a", "HIGH");
            dequeue("orders");
            clock.advance(VISIBILITY);
            ShardReplica replica = replicas.get(0);
            assertThat(replica.highestReadyRank()).isEqualTo(-1);

            broker.sweep();

            assertThat(replica.highestReadyRank()).isZero();
        }

        @Test
        void theSweeperRunsOnItsOwn() {
            try (InMemoryCoordinator ownCoordinator = new InMemoryCoordinator();
                    Broker sweeping = new Broker(
                            TestBrokers.settings("broker-1", 8080, Duration.ofMillis(20)), ownCoordinator, clock)) {
                List<ShardReplica> sweptReplicas = new CopyOnWriteArrayList<>();
                sweeping.onReplicaCreated(sweptReplicas::add);
                sweeping.start();
                sweeping.queues().createQueue(QueueConfig.builder("orders").build());
                sweeping.queues().enqueue("orders", "a", "HIGH", null, null).join();
                sweeping.queues().dequeue("orders", Duration.ZERO).join();

                clock.advance(VISIBILITY);

                await().atMost(5, TimeUnit.SECONDS)
                        .untilAsserted(() -> assertThat(sweptReplicas.get(0).highestReadyRank()).isZero());
            }
        }
    }

    @Nested
    class WaitingDequeue {

        @Test
        void returnsAsSoonAsAMessageArrives() throws Exception {
            queues.createQueue(QueueConfig.builder("orders").shards(2).build());

            CompletableFuture<Optional<MessageView>> waiting = queues.dequeue("orders", Duration.ofSeconds(10));
            Thread.sleep(150);
            assertThat(waiting).isNotDone();
            enqueue("orders", "late", "LOW");

            assertThat(waiting.get(2, TimeUnit.SECONDS)).map(MessageView::payload).contains("late");
        }

        @Test
        void returnsEmptyWhenTheWaitRunsOut() throws Exception {
            queues.createQueue(QueueConfig.builder("orders").build());
            long start = System.nanoTime();

            Optional<MessageView> nothing = queues.dequeue("orders", Duration.ofMillis(300)).get(5, TimeUnit.SECONDS);

            assertThat(nothing).isEmpty();
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(300));
        }

        @Test
        void noWaitReturnsAtOnce() {
            queues.createQueue(QueueConfig.builder("orders").build());

            assertThat(queues.dequeue("orders", Duration.ZERO)).isCompletedWithValue(Optional.empty());
        }

        @Test
        void theWaitIsLimited() {
            queues.createQueue(QueueConfig.builder("orders").build());

            assertThatIllegalArgumentException()
                    .isThrownBy(() -> queues.dequeue("orders", QueueService.MAX_WAIT.plusMillis(1)));
            assertThatIllegalArgumentException().isThrownBy(() -> queues.dequeue("orders", Duration.ofMillis(-1)));
        }
    }

    private MessageId enqueue(String queue, String payload, String priority) {
        return queues.enqueue(queue, payload, priority, null, null).join();
    }

    private Optional<MessageView> dequeue(String queue) {
        return queues.dequeue(queue, Duration.ZERO).join();
    }

    private List<String> drain(String queue) {
        List<String> payloads = new ArrayList<>();
        for (Optional<MessageView> next = dequeue(queue); next.isPresent(); next = dequeue(queue)) {
            payloads.add(next.get().payload());
        }
        return payloads;
    }
}
