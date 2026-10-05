package com.queue.distributedpriorityqueue.transport;

import static org.assertj.core.api.Assertions.assertThat;

import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageState;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.ShardEvent;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.exception.BrokerUnreachableException;
import com.queue.distributedpriorityqueue.exception.CommitTimeoutException;
import com.queue.distributedpriorityqueue.exception.NotEnoughReplicasException;
import com.queue.distributedpriorityqueue.exception.NotLeaderException;
import com.queue.distributedpriorityqueue.exception.ShardUnavailableException;
import com.queue.distributedpriorityqueue.log.LogEntry;
import com.queue.distributedpriorityqueue.log.ShardLog.EpochEnd;
import com.queue.distributedpriorityqueue.replication.Acks;
import com.queue.distributedpriorityqueue.replication.FetchEnd;
import com.queue.distributedpriorityqueue.replication.FetchRequest;
import io.grpc.Status;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/** Everything sent between brokers must come out the other side exactly as it went in. */
class WireMapperTest {

    private static final Instant AT = Instant.parse("2026-01-01T00:00:00.123456789Z");
    private static final ShardId SHARD = new ShardId("orders", 3);

    @Test
    void everyKindOfEventSurvivesTheRoundTrip() {
        List<ShardEvent> events = List.of(
                new ShardEvent.Enqueued("payload with ünïcode", 2, AT, AT.plusSeconds(60)),
                new ShardEvent.Enqueued("", 0, AT, null),
                new ShardEvent.Delivered(7, AT, AT.plusSeconds(30)),
                new ShardEvent.Acked(7, AT),
                new ShardEvent.Requeued(8, AT),
                new ShardEvent.DeadLettered(9, AT),
                new ShardEvent.Expired(10, AT));

        for (ShardEvent event : events) {
            LogEntry entry = new LogEntry(1234567890123L, 5, event);
            assertThat(WireMapper.fromWire(WireMapper.toWire(entry))).isEqualTo(entry);
        }
    }

    @Test
    void aMessageSurvivesWithAndWithoutItsOptionalTimes() {
        MessageView inFlight = new MessageView(new MessageId(3, 42), "a", 1, MessageState.IN_FLIGHT, 2, AT,
                AT.plusSeconds(60), AT.plusSeconds(30));
        MessageView ready = new MessageView(new MessageId(0, 0), "b", 0, MessageState.READY, 0, AT, null, null);
        MessageView dead = new MessageView(new MessageId(1, 9), "c", 2, MessageState.DEAD, 4, AT, null, null);

        for (MessageView message : List.of(inFlight, ready, dead)) {
            assertThat(WireMapper.fromWire(WireMapper.toWire(message))).isEqualTo(message);
        }
    }

    @Test
    void metricsSurviveTheRoundTrip() {
        ShardMetrics metrics = new ShardMetrics(List.of(1L, 0L, 7L), 3, Duration.ofMillis(1500), 2, 100, 90, 5, 80, 2,
                4, 1.5, 0.25);

        assertThat(WireMapper.fromWire(WireMapper.toWire(metrics))).isEqualTo(metrics);
    }

    @Test
    void fetchRequestsAndEndingsSurviveTheRoundTrip() {
        FetchRequest request = new FetchRequest(SHARD, new ReplicaId("broker-2", "incarnation"), 4, 99, 3);
        FetchRequest fromAnEmptyLog = new FetchRequest(SHARD, new ReplicaId("broker-2", "incarnation"), 0, 0, -1);

        assertThat(WireMapper.fromWire(WireMapper.toWire(request))).isEqualTo(request);
        assertThat(WireMapper.fromWire(WireMapper.toWire(fromAnEmptyLog))).isEqualTo(fromAnEmptyLog);
        for (FetchEnd end : List.of(FetchEnd.of(FetchEnd.Reason.CLOSED), FetchEnd.of(FetchEnd.Reason.NOT_LEADER),
                FetchEnd.of(FetchEnd.Reason.FENCED_EPOCH), FetchEnd.diverged(new EpochEnd(3, 17)),
                FetchEnd.diverged(new EpochEnd(-1, 0)))) {
            assertThat(WireMapper.fromWire(WireMapper.toWire(end))).isEqualTo(end);
        }
    }

    @Test
    void smallTypesSurviveTheRoundTrip() {
        assertThat(WireMapper.fromWire(WireMapper.toWire(SHARD))).isEqualTo(SHARD);
        assertThat(WireMapper.fromWire(WireMapper.toWire(AT))).isEqualTo(AT);
        assertThat(WireMapper.fromWire(WireMapper.toWire(new MessageId(3, 42)))).isEqualTo(new MessageId(3, 42));
        assertThat(WireMapper.fromWire(WireMapper.toWire(Acks.ALL))).isEqualTo(Acks.ALL);
        assertThat(WireMapper.fromWire(WireMapper.toWire(Acks.LEADER))).isEqualTo(Acks.LEADER);
    }

    @Test
    void theServicesOwnErrorsArriveAsTheSameExceptions() {
        assertArrivesAs(new NotLeaderException(SHARD), NotLeaderException.class);
        assertArrivesAs(new NotEnoughReplicasException(SHARD, 1, 2), NotEnoughReplicasException.class);
        assertArrivesAs(new CommitTimeoutException(SHARD), CommitTimeoutException.class);
        assertArrivesAs(new ShardUnavailableException(SHARD), ShardUnavailableException.class);
        assertArrivesAs(new IllegalArgumentException("unknown priority level"), IllegalArgumentException.class);
        assertArrivesAs(new CompletionException(new NotLeaderException(SHARD)), NotLeaderException.class);
    }

    @Test
    void aFailedCallMeansTheBrokerIsUnreachable() {
        assertThat(WireErrors.fromWire(Status.UNAVAILABLE.asRuntimeException(), "broker-2"))
                .isInstanceOf(BrokerUnreachableException.class).hasMessageContaining("broker-2");
        assertThat(WireErrors.fromWire(Status.DEADLINE_EXCEEDED.asRuntimeException(), "broker-2"))
                .isInstanceOf(BrokerUnreachableException.class);
        assertThat(WireErrors.fromWire(WireErrors.toWire(new IllegalStateException("a bug")), "broker-2"))
                .isInstanceOf(BrokerUnreachableException.class);
    }

    private static void assertArrivesAs(RuntimeException thrown, Class<? extends RuntimeException> expected) {
        Throwable cause = thrown instanceof CompletionException ? thrown.getCause() : thrown;
        assertThat(WireErrors.fromWire(WireErrors.toWire(thrown), "broker-2"))
                .isInstanceOf(expected).hasMessage(cause.getMessage());
    }
}
