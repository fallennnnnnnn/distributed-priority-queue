package com.queue.distributedpriorityqueue.replication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageState;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.log.InMemoryShardLog;
import com.queue.distributedpriorityqueue.testsupport.ManualClock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Queue behaviour on one shard, with time moved by hand. */
class ShardReplicaTest {

    private static final Duration VISIBILITY = Duration.ofSeconds(30);

    private final ManualClock clock = new ManualClock();
    private ShardReplica replica = replicaWith(QueueConfig.builder("orders"));

    @Nested
    class Ordering {

        @Test
        void emptyQueueReturnsNothing() {
            assertThat(dequeue()).isEmpty();
        }

        @Test
        void deliversHighestPriorityFirst() {
            enqueue("low", "LOW");
            enqueue("high", "HIGH");
            enqueue("medium", "MEDIUM");

            assertThat(drain()).containsExactly("high", "medium", "low");
        }

        @Test
        void deliversFifoWithinAPriority() {
            enqueue("a", "MEDIUM");
            enqueue("b", "MEDIUM");
            enqueue("c", "MEDIUM");

            assertThat(drain()).containsExactly("a", "b", "c");
        }

        @Test
        void aHigherPriorityArrivingLaterIsServedNext() {
            enqueue("low-1", "LOW");
            enqueue("low-2", "LOW");
            assertThat(dequeuePayload()).isEqualTo("low-1");

            enqueue("high", "HIGH");

            assertThat(drain()).containsExactly("high", "low-2");
        }
    }

    @Nested
    class Visibility {

        @Test
        void anInFlightMessageIsInvisible() {
            enqueue("a", "HIGH");

            assertThat(dequeuePayload()).isEqualTo("a");
            assertThat(dequeue()).isEmpty();
        }

        @Test
        void dequeueReportsTheDelivery() {
            MessageId id = enqueue("a", "MEDIUM");

            MessageView delivery = dequeue().orElseThrow();

            assertThat(delivery.id()).isEqualTo(id);
            assertThat(delivery.payload()).isEqualTo("a");
            assertThat(delivery.priority()).isEqualTo(1);
            assertThat(delivery.state()).isEqualTo(MessageState.IN_FLIGHT);
            assertThat(delivery.deliveryCount()).isEqualTo(1);
            assertThat(delivery.enqueuedAt()).isEqualTo(clock.instant());
            assertThat(delivery.visibleAt()).isEqualTo(clock.instant().plus(VISIBILITY));
        }

        @Test
        void aMessageReturnsExactlyWhenItsVisibilityTimeoutPasses() {
            MessageId id = enqueue("a", "HIGH");
            dequeue();

            clock.advance(VISIBILITY.minusMillis(1));
            assertThat(dequeue()).isEmpty();

            clock.advance(Duration.ofMillis(1));
            MessageView redelivery = dequeue().orElseThrow();
            assertThat(redelivery.id()).isEqualTo(id);
            assertThat(redelivery.deliveryCount()).isEqualTo(2);
        }

        @Test
        void aTimedOutMessageReturnsToItsOriginalPosition() {
            enqueue("a", "MEDIUM");
            enqueue("b", "MEDIUM");
            enqueue("c", "MEDIUM");
            assertThat(dequeuePayload()).isEqualTo("a");
            assertThat(dequeuePayload()).isEqualTo("b");

            clock.advance(VISIBILITY);

            assertThat(drain()).containsExactly("a", "b", "c");
        }
    }

    @Nested
    class RetryAndDeadLetter {

        @Test
        void deadLettersAfterMaxRetriesPlusOneDeliveries() {
            replica = replicaWith(QueueConfig.builder("orders").maxRetries(2));
            MessageId id = enqueue("poison", "HIGH");

            for (int delivery = 1; delivery <= 3; delivery++) {
                assertThat(dequeue().orElseThrow().deliveryCount()).isEqualTo(delivery);
                clock.advance(VISIBILITY);
            }

            assertThat(dequeue()).isEmpty();
            List<MessageView> deadLetters = replica.deadLetters();
            assertThat(deadLetters).hasSize(1);
            assertThat(deadLetters.get(0).id()).isEqualTo(id);
            assertThat(deadLetters.get(0).state()).isEqualTo(MessageState.DEAD);
            assertThat(deadLetters.get(0).deliveryCount()).isEqualTo(3);
            ShardMetrics metrics = replica.metrics();
            assertThat(metrics.deadLetteredTotal()).isEqualTo(1);
            assertThat(metrics.deadLetters()).isEqualTo(1);
            assertThat(metrics.redeliveredTotal()).isEqualTo(2);
        }

        @Test
        void zeroRetriesDeadLettersOnTheFirstTimeout() {
            replica = replicaWith(QueueConfig.builder("orders").maxRetries(0));
            enqueue("a", "HIGH");
            dequeue();

            clock.advance(VISIBILITY);

            assertThat(dequeue()).isEmpty();
            assertThat(replica.deadLetters()).hasSize(1);
        }

        @Test
        void aDeadLetterIsNeverDeliveredAgain() {
            replica = replicaWith(QueueConfig.builder("orders").maxRetries(0));
            enqueue("a", "HIGH");
            dequeue();
            clock.advance(VISIBILITY);
            replica.reclaim().join();

            clock.advance(Duration.ofDays(1));

            assertThat(dequeue()).isEmpty();
            assertThat(replica.deadLetters()).hasSize(1);
        }
    }

    @Nested
    class Ttl {

        @Test
        void anExpiredMessageIsNotDelivered() {
            replica.enqueue("a", "HIGH", Duration.ofSeconds(10)).join();

            clock.advance(Duration.ofSeconds(10));

            assertThat(dequeue()).isEmpty();
            assertThat(replica.metrics().expiredTotal()).isEqualTo(1);
        }

        @Test
        void aMessageIsDeliveredUntilItsTtlPasses() {
            replica.enqueue("a", "HIGH", Duration.ofSeconds(10)).join();

            clock.advance(Duration.ofSeconds(10).minusMillis(1));

            assertThat(dequeuePayload()).isEqualTo("a");
        }

        @Test
        void anExpiredInFlightMessageCanStillBeAcked() {
            MessageId id = replica.enqueue("a", "HIGH", Duration.ofSeconds(10)).join();
            dequeue();

            clock.advance(Duration.ofSeconds(15));

            assertThat(ack(id)).isTrue();
            ShardMetrics metrics = replica.metrics();
            assertThat(metrics.ackedTotal()).isEqualTo(1);
            assertThat(metrics.expiredTotal()).isZero();
        }

        @Test
        void anExpiredMessageIsNotRedelivered() {
            replica = replicaWith(QueueConfig.builder("orders").visibilityTimeout(Duration.ofSeconds(5)));
            replica.enqueue("a", "HIGH", Duration.ofSeconds(10)).join();
            dequeue();

            clock.advance(Duration.ofSeconds(12));

            assertThat(dequeue()).isEmpty();
            ShardMetrics metrics = replica.metrics();
            assertThat(metrics.expiredTotal()).isEqualTo(1);
            assertThat(metrics.redeliveredTotal()).isZero();
            assertThat(replica.deadLetters()).isEmpty();
        }

        @Test
        void theQueueDefaultTtlAppliesWhenTheRequestHasNone() {
            replica = replicaWith(QueueConfig.builder("orders").defaultTtl(Duration.ofSeconds(10)));
            enqueue("default", "HIGH");
            replica.enqueue("explicit", "HIGH", Duration.ofMinutes(1)).join();

            clock.advance(Duration.ofSeconds(10));

            assertThat(drain()).containsExactly("explicit");
        }

        @Test
        void aNonPositiveTtlIsRejected() {
            assertThatIllegalArgumentException().isThrownBy(() -> replica.enqueue("a", "HIGH", Duration.ZERO));
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> replica.enqueue("a", "HIGH", Duration.ofSeconds(-1)));
        }
    }

    @Nested
    class Acknowledge {

        @Test
        void ackRemovesTheMessageForGood() {
            MessageId id = enqueue("a", "HIGH");
            dequeue();

            assertThat(ack(id)).isTrue();

            clock.advance(Duration.ofHours(1));
            assertThat(dequeue()).isEmpty();
            assertThat(replica.metrics().ackedTotal()).isEqualTo(1);
        }

        @Test
        void aLateAckIsAcceptedWhenTheMessageIsBackInTheQueue() {
            MessageId id = enqueue("a", "HIGH");
            dequeue();
            clock.advance(VISIBILITY.plusSeconds(1));
            replica.reclaim().join();
            assertThat(replica.metrics().ready()).isEqualTo(1);

            assertThat(ack(id)).isTrue();

            assertThat(dequeue()).isEmpty();
        }

        @Test
        void aLateAckIsAcceptedAfterRedeliveryToAnotherConsumer() {
            MessageId id = enqueue("a", "HIGH");
            dequeue();
            clock.advance(VISIBILITY.plusSeconds(1));
            assertThat(dequeue().orElseThrow().deliveryCount()).isEqualTo(2);

            assertThat(ack(id)).isTrue();

            assertThat(ack(id)).isFalse();
            clock.advance(Duration.ofHours(1));
            assertThat(dequeue()).isEmpty();
            assertThat(replica.metrics().ackedTotal()).isEqualTo(1);
        }

        @Test
        void aRepeatedAckChangesNothing() {
            MessageId id = enqueue("a", "HIGH");
            dequeue();
            ack(id);
            ShardMetrics before = replica.metrics();

            assertThat(ack(id)).isFalse();

            assertThat(replica.metrics()).isEqualTo(before);
        }

        @Test
        void anUnknownIdIsRefused() {
            enqueue("a", "HIGH");

            assertThat(ack(new MessageId(0, 99))).isFalse();
            assertThat(ack(new MessageId(7, 0))).isFalse();
        }

        @Test
        void aMessageThatWasNeverDeliveredCannotBeAcked() {
            MessageId id = enqueue("a", "HIGH");

            assertThat(ack(id)).isFalse();

            assertThat(dequeuePayload()).isEqualTo("a");
        }

        @Test
        void aDeadLetterCannotBeAcked() {
            replica = replicaWith(QueueConfig.builder("orders").maxRetries(0));
            MessageId id = enqueue("a", "HIGH");
            dequeue();
            clock.advance(VISIBILITY);
            replica.reclaim().join();

            assertThat(ack(id)).isFalse();

            assertThat(replica.deadLetters()).hasSize(1);
        }
    }

    @Nested
    class PriorityLevels {

        @Test
        void oneLevelBehavesAsPlainFifo() {
            replica = replicaWith(QueueConfig.builder("jobs").priorityLevels("only"));
            enqueue("a", null);
            enqueue("b", "only");
            enqueue("c", null);

            assertThat(drain()).containsExactly("a", "b", "c");
        }

        @Test
        void fiveLevelsAreServedInTheDeclaredOrder() {
            replica = replicaWith(
                    QueueConfig.builder("jobs").priorityLevels("critical", "high", "normal", "low", "bulk"));
            for (String level : List.of("bulk", "normal", "critical", "low", "high")) {
                enqueue(level, level);
            }

            assertThat(drain()).containsExactly("critical", "high", "normal", "low", "bulk");
        }

        @Test
        void noPriorityMeansTheLowestLevel() {
            enqueue("unspecified", null);
            enqueue("medium", "MEDIUM");

            assertThat(dequeuePayload()).isEqualTo("medium");
            assertThat(dequeue().orElseThrow().priority()).isEqualTo(2);
        }

        @Test
        void levelNamesIgnoreCase() {
            enqueue("low", "low");
            enqueue("high", "high");

            assertThat(drain()).containsExactly("high", "low");
        }

        @Test
        void anUnknownLevelIsRejected() {
            assertThatIllegalArgumentException().isThrownBy(() -> replica.enqueue("a", "URGENT", null));
            assertThat(replica.metrics().enqueuedTotal()).isZero();
        }
    }

    @Nested
    class Metrics {

        @Test
        void countsReadyByPriorityAndInFlight() {
            enqueue("h1", "HIGH");
            enqueue("h2", "HIGH");
            enqueue("m1", "MEDIUM");
            enqueue("l1", "LOW");
            dequeue();

            ShardMetrics metrics = replica.metrics();

            assertThat(metrics.readyByPriority()).containsExactly(1L, 1L, 1L);
            assertThat(metrics.ready()).isEqualTo(3);
            assertThat(metrics.inFlight()).isEqualTo(1);
            assertThat(metrics.enqueuedTotal()).isEqualTo(4);
            assertThat(metrics.deliveredTotal()).isEqualTo(1);
        }

        @Test
        void aTimedOutMessageCountsAsReadyAgain() {
            enqueue("a", "HIGH");
            dequeue();

            clock.advance(VISIBILITY);

            ShardMetrics metrics = replica.metrics();
            assertThat(metrics.inFlight()).isZero();
            assertThat(metrics.readyByPriority()).containsExactly(1L, 0L, 0L);
        }

        @Test
        void oldestMessageAgeIgnoresInFlightAndExpiredMessages() {
            assertThat(replica.metrics().oldestReadyAge()).isEqualTo(Duration.ZERO);

            enqueue("oldest", "HIGH");
            clock.advance(Duration.ofSeconds(2));
            replica.enqueue("expiring", "LOW", Duration.ofSeconds(6)).join();
            clock.advance(Duration.ofSeconds(3));
            enqueue("young", "MEDIUM");
            assertThat(replica.metrics().oldestReadyAge()).isEqualTo(Duration.ofSeconds(5));

            assertThat(dequeuePayload()).isEqualTo("oldest");
            assertThat(replica.metrics().oldestReadyAge()).isEqualTo(Duration.ofSeconds(3));

            clock.advance(Duration.ofSeconds(4));
            assertThat(replica.metrics().oldestReadyAge()).isEqualTo(Duration.ofSeconds(4));
        }

        @Test
        void throughputIsAveragedOverTheLastMinute() {
            List<MessageId> ids = new ArrayList<>();
            for (int second = 0; second < 60; second++) {
                if (second > 0) {
                    clock.advance(Duration.ofSeconds(1));
                }
                ids.add(enqueue("a", "HIGH"));
                ids.add(enqueue("b", "HIGH"));
            }
            for (int i = 0; i < 30; i++) {
                dequeue();
                ack(ids.get(i));
            }

            ShardMetrics busy = replica.metrics();
            assertThat(busy.enqueuedPerSecond()).isEqualTo(2.0);
            assertThat(busy.ackedPerSecond()).isEqualTo(0.5);

            clock.advance(Duration.ofSeconds(60));
            ShardMetrics quiet = replica.metrics();
            assertThat(quiet.enqueuedPerSecond()).isZero();
            assertThat(quiet.ackedPerSecond()).isZero();
            assertThat(quiet.enqueuedTotal()).isEqualTo(120);
        }

        @Test
        void everyMessageIsAccountedFor() {
            replica = replicaWith(QueueConfig.builder("orders").maxRetries(0));
            MessageId acked = enqueue("acked", "HIGH");
            enqueue("dead", "HIGH");
            replica.enqueue("expired", "LOW", Duration.ofSeconds(5)).join();
            enqueue("ready", "LOW");
            dequeue();
            dequeue();
            ack(acked);
            clock.advance(VISIBILITY);
            enqueue("in-flight", "HIGH");
            dequeue();

            ShardMetrics metrics = replica.metrics();

            assertThat(metrics.ackedTotal()).isEqualTo(1);
            assertThat(metrics.deadLetteredTotal()).isEqualTo(1);
            assertThat(metrics.expiredTotal()).isEqualTo(1);
            assertThat(metrics.ready()).isEqualTo(1);
            assertThat(metrics.inFlight()).isEqualTo(1);
            assertThat(metrics.enqueuedTotal()).isEqualTo(metrics.ready() + metrics.inFlight()
                    + metrics.ackedTotal() + metrics.deadLetteredTotal() + metrics.expiredTotal());
        }
    }

    @Test
    void aChangedSettingAppliesToFutureDeliveries() {
        enqueue("a", "HIGH");
        enqueue("b", "HIGH");
        MessageView first = dequeue().orElseThrow();

        replica.updateConfig(QueueConfig.builder("orders").visibilityTimeout(Duration.ofSeconds(5)).build());
        MessageView second = dequeue().orElseThrow();

        assertThat(first.visibleAt()).isEqualTo(clock.instant().plus(VISIBILITY));
        assertThat(second.visibleAt()).isEqualTo(clock.instant().plusSeconds(5));
    }

    @Test
    void theNumberOfPriorityLevelsCannotChange() {
        assertThatIllegalArgumentException().isThrownBy(
                () -> replica.updateConfig(QueueConfig.builder("orders").priorityLevels("a", "b").build()));
    }

    private ShardReplica replicaWith(QueueConfig.Builder config) {
        return new ShardReplica(0, config.build(), new InMemoryShardLog(), clock);
    }

    private MessageId enqueue(String payload, String priority) {
        return replica.enqueue(payload, priority, null).join();
    }

    private Optional<MessageView> dequeue() {
        return replica.dequeue().join();
    }

    private String dequeuePayload() {
        return dequeue().map(MessageView::payload).orElse(null);
    }

    private boolean ack(MessageId id) {
        return replica.ack(id).join();
    }

    private List<String> drain() {
        List<String> payloads = new ArrayList<>();
        for (Optional<MessageView> next = dequeue(); next.isPresent(); next = dequeue()) {
            payloads.add(next.get().payload());
        }
        return payloads;
    }
}
