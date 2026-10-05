package com.queue.distributedpriorityqueue.transport;

import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageState;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.ShardEvent;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.log.LogEntry;
import com.queue.distributedpriorityqueue.log.ShardLog.EpochEnd;
import com.queue.distributedpriorityqueue.replication.Acks;
import com.queue.distributedpriorityqueue.replication.FetchEnd;
import com.queue.distributedpriorityqueue.replication.FetchRequest;
import java.time.Duration;
import java.time.Instant;

/** Converts between the broker's own types and their protobuf form on the wire. */
final class WireMapper {

    private WireMapper() {
    }

    static Wire.Shard toWire(ShardId shard) {
        return Wire.Shard.newBuilder().setQueue(shard.queue()).setNumber(shard.shard()).build();
    }

    static ShardId fromWire(Wire.Shard shard) {
        return new ShardId(shard.getQueue(), shard.getNumber());
    }

    static Wire.Time toWire(Instant instant) {
        return Wire.Time.newBuilder().setEpochSecond(instant.getEpochSecond()).setNano(instant.getNano()).build();
    }

    static Instant fromWire(Wire.Time time) {
        return Instant.ofEpochSecond(time.getEpochSecond(), time.getNano());
    }

    static Wire.MessageRef toWire(MessageId id) {
        return Wire.MessageRef.newBuilder().setShard(id.shard()).setOffset(id.offset()).build();
    }

    static MessageId fromWire(Wire.MessageRef id) {
        return new MessageId(id.getShard(), id.getOffset());
    }

    static Wire.Acks toWire(Acks acks) {
        return acks == Acks.LEADER ? Wire.Acks.LEADER : Wire.Acks.ALL;
    }

    static Acks fromWire(Wire.Acks acks) {
        return acks == Wire.Acks.LEADER ? Acks.LEADER : Acks.ALL;
    }

    static Wire.QueueMessage toWire(MessageView message) {
        Wire.QueueMessage.Builder wire = Wire.QueueMessage.newBuilder()
                .setId(toWire(message.id()))
                .setPayload(message.payload())
                .setPriority(message.priority())
                .setState(Wire.MessageState.valueOf(message.state().name()))
                .setDeliveryCount(message.deliveryCount())
                .setEnqueuedAt(toWire(message.enqueuedAt()));
        if (message.expiresAt() != null) {
            wire.setExpiresAt(toWire(message.expiresAt()));
        }
        if (message.visibleAt() != null) {
            wire.setVisibleAt(toWire(message.visibleAt()));
        }
        return wire.build();
    }

    static MessageView fromWire(Wire.QueueMessage message) {
        return new MessageView(fromWire(message.getId()), message.getPayload(), message.getPriority(),
                MessageState.valueOf(message.getState().name()), message.getDeliveryCount(),
                fromWire(message.getEnqueuedAt()),
                message.hasExpiresAt() ? fromWire(message.getExpiresAt()) : null,
                message.hasVisibleAt() ? fromWire(message.getVisibleAt()) : null);
    }

    static Wire.MetricsReply toWire(ShardMetrics metrics) {
        return Wire.MetricsReply.newBuilder()
                .addAllReadyByPriority(metrics.readyByPriority())
                .setInFlight(metrics.inFlight())
                .setOldestReadyAgeNanos(metrics.oldestReadyAge().toNanos())
                .setDeadLetters(metrics.deadLetters())
                .setEnqueuedTotal(metrics.enqueuedTotal())
                .setDeliveredTotal(metrics.deliveredTotal())
                .setRedeliveredTotal(metrics.redeliveredTotal())
                .setAckedTotal(metrics.ackedTotal())
                .setDeadLetteredTotal(metrics.deadLetteredTotal())
                .setExpiredTotal(metrics.expiredTotal())
                .setEnqueuedPerSecond(metrics.enqueuedPerSecond())
                .setAckedPerSecond(metrics.ackedPerSecond())
                .build();
    }

    static ShardMetrics fromWire(Wire.MetricsReply metrics) {
        return new ShardMetrics(metrics.getReadyByPriorityList(), metrics.getInFlight(),
                Duration.ofNanos(metrics.getOldestReadyAgeNanos()), metrics.getDeadLetters(),
                metrics.getEnqueuedTotal(), metrics.getDeliveredTotal(), metrics.getRedeliveredTotal(),
                metrics.getAckedTotal(), metrics.getDeadLetteredTotal(), metrics.getExpiredTotal(),
                metrics.getEnqueuedPerSecond(), metrics.getAckedPerSecond());
    }

    static Wire.LogEntry toWire(LogEntry entry) {
        return Wire.LogEntry.newBuilder().setOffset(entry.offset()).setEpoch(entry.epoch())
                .setEvent(toWire(entry.event())).build();
    }

    static LogEntry fromWire(Wire.LogEntry entry) {
        return new LogEntry(entry.getOffset(), entry.getEpoch(), fromWire(entry.getEvent()));
    }

    static Wire.Event toWire(ShardEvent event) {
        Wire.Event.Builder wire = Wire.Event.newBuilder().setAt(toWire(event.at()));
        if (event instanceof ShardEvent.Enqueued e) {
            Wire.Enqueued.Builder enqueued = Wire.Enqueued.newBuilder().setPayload(e.payload()).setPriority(e.priority());
            if (e.expiresAt() != null) {
                enqueued.setExpiresAt(toWire(e.expiresAt()));
            }
            wire.setEnqueued(enqueued);
        } else if (event instanceof ShardEvent.Delivered e) {
            wire.setDelivered(Wire.Delivered.newBuilder().setMessageOffset(e.messageOffset())
                    .setVisibleAt(toWire(e.visibleAt())));
        } else if (event instanceof ShardEvent.Acked e) {
            wire.setAcked(e.messageOffset());
        } else if (event instanceof ShardEvent.Requeued e) {
            wire.setRequeued(e.messageOffset());
        } else if (event instanceof ShardEvent.DeadLettered e) {
            wire.setDeadLettered(e.messageOffset());
        } else if (event instanceof ShardEvent.Expired e) {
            wire.setExpired(e.messageOffset());
        } else {
            throw new IllegalArgumentException("unknown event " + event);
        }
        return wire.build();
    }

    static ShardEvent fromWire(Wire.Event event) {
        Instant at = fromWire(event.getAt());
        return switch (event.getKindCase()) {
            case ENQUEUED -> new ShardEvent.Enqueued(event.getEnqueued().getPayload(), event.getEnqueued().getPriority(),
                    at, event.getEnqueued().hasExpiresAt() ? fromWire(event.getEnqueued().getExpiresAt()) : null);
            case DELIVERED -> new ShardEvent.Delivered(event.getDelivered().getMessageOffset(), at,
                    fromWire(event.getDelivered().getVisibleAt()));
            case ACKED -> new ShardEvent.Acked(event.getAcked(), at);
            case REQUEUED -> new ShardEvent.Requeued(event.getRequeued(), at);
            case DEAD_LETTERED -> new ShardEvent.DeadLettered(event.getDeadLettered(), at);
            case EXPIRED -> new ShardEvent.Expired(event.getExpired(), at);
            case KIND_NOT_SET -> throw new IllegalArgumentException("an event of no kind");
        };
    }

    static Wire.FetchOpen toWire(FetchRequest request) {
        return Wire.FetchOpen.newBuilder()
                .setShard(toWire(request.shard()))
                .setFollowerBroker(request.follower().brokerId())
                .setFollowerIncarnation(request.follower().incarnation())
                .setEpoch(request.epoch())
                .setFetchOffset(request.fetchOffset())
                .setLastEntryEpoch(request.lastEntryEpoch())
                .build();
    }

    static FetchRequest fromWire(Wire.FetchOpen open) {
        return new FetchRequest(fromWire(open.getShard()),
                new ReplicaId(open.getFollowerBroker(), open.getFollowerIncarnation()), open.getEpoch(),
                open.getFetchOffset(), open.getLastEntryEpoch());
    }

    static Wire.End toWire(FetchEnd end) {
        Wire.End.Builder wire = Wire.End.newBuilder().setReason(Wire.End.Reason.valueOf(end.reason().name()));
        if (end.leaderEpochEnd() != null) {
            wire.setLeaderEpoch(end.leaderEpochEnd().epoch()).setLeaderEpochEnd(end.leaderEpochEnd().endOffset());
        }
        return wire.build();
    }

    static FetchEnd fromWire(Wire.End end) {
        FetchEnd.Reason reason = FetchEnd.Reason.valueOf(end.getReason().name());
        return reason == FetchEnd.Reason.DIVERGED
                ? FetchEnd.diverged(new EpochEnd(end.getLeaderEpoch(), end.getLeaderEpochEnd()))
                : FetchEnd.of(reason);
    }
}
