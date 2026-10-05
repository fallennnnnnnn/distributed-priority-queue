package com.queue.distributedpriorityqueue.core;

import com.queue.distributedpriorityqueue.core.ShardEvent.Acked;
import com.queue.distributedpriorityqueue.core.ShardEvent.DeadLettered;
import com.queue.distributedpriorityqueue.core.ShardEvent.Delivered;
import com.queue.distributedpriorityqueue.core.ShardEvent.Enqueued;
import com.queue.distributedpriorityqueue.core.ShardEvent.Expired;
import com.queue.distributedpriorityqueue.core.ShardEvent.Requeued;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The state of one shard and every rule for changing it.
 *
 * <p>It has two halves. The decision methods ({@link #enqueue}, {@link #dequeue}, {@link #ack},
 * {@link #reclaim}) only read the state and return the events that should happen; the leader
 * calls them. {@link #apply} is the only method that changes the state; the leader calls it after
 * appending an event to the log, and followers call it with the events they fetch.
 *
 * <p>A message is always in exactly one place: a ready lane, the in-flight index or the dead
 * letters. Not thread-safe: the owning replica guards it with the shard lock.
 */
public final class ShardStateMachine {

    private final int shard;
    /** Ready messages: one lane per priority rank, each ordered by enqueue offset (FIFO). */
    private final List<NavigableMap<Long, Message>> lanes;
    /** Ready and in-flight messages by enqueue offset. */
    private final Map<Long, Message> live = new HashMap<>();
    /** In-flight messages, soonest visibility deadline first. */
    private final NavigableMap<TimeKey, Message> byDeadline = new TreeMap<>();
    /** Ready messages that have a TTL, soonest expiry first. */
    private final NavigableMap<TimeKey, Message> byExpiry = new TreeMap<>();
    private final Map<Long, Message> deadLetters = new LinkedHashMap<>();

    private final RateWindow enqueueRate = new RateWindow();
    private final RateWindow ackRate = new RateWindow();
    private long enqueued;
    private long delivered;
    private long redelivered;
    private long acked;
    private long deadLettered;
    private long expired;

    public ShardStateMachine(int shard, int priorityLevels) {
        this.shard = shard;
        List<NavigableMap<Long, Message>> lanes = new ArrayList<>(priorityLevels);
        for (int rank = 0; rank < priorityLevels; rank++) {
            lanes.add(new TreeMap<>());
        }
        this.lanes = List.copyOf(lanes);
    }

    // Decisions: read the state, return what should happen.

    public Enqueued enqueue(String payload, int priority, Instant expiresAt, Instant now) {
        if (payload == null) {
            throw new IllegalArgumentException("payload is required");
        }
        if (priority < 0 || priority >= lanes.size()) {
            throw new IllegalArgumentException("priority rank " + priority + " is outside 0.." + (lanes.size() - 1));
        }
        return new Enqueued(payload, priority, now, expiresAt);
    }

    /** The first message of the highest non-empty lane. Call {@link #reclaim} first. */
    public Optional<Delivered> dequeue(Instant now, Duration visibilityTimeout) {
        for (NavigableMap<Long, Message> lane : lanes) {
            if (!lane.isEmpty()) {
                return Optional.of(new Delivered(lane.firstKey(), now, now.plus(visibilityTimeout)));
            }
        }
        return Optional.empty();
    }

    /**
     * Accepted while the message still exists and has been delivered at least once, even after
     * its visibility timeout: an ack means the work succeeded, so refusing it would only cause a
     * redelivery.
     */
    public Optional<Acked> ack(long messageOffset, Instant now) {
        Message message = live.get(messageOffset);
        return message != null && message.deliveryCount > 0
                ? Optional.of(new Acked(messageOffset, now))
                : Optional.empty();
    }

    /** What is overdue at {@code now}: in-flight messages past their deadline, ready ones past their TTL. */
    public List<ShardEvent> reclaim(Instant now, int maxRetries) {
        List<ShardEvent> events = new ArrayList<>();
        for (Message message : dueBy(byDeadline, now)) {
            if (message.hasExpiredBy(now)) {
                events.add(new Expired(message.offset(), now));
            } else if (message.deliveryCount > maxRetries) {
                events.add(new DeadLettered(message.offset(), now));
            } else {
                events.add(new Requeued(message.offset(), now));
            }
        }
        for (Message message : dueBy(byExpiry, now)) {
            events.add(new Expired(message.offset(), now));
        }
        return events;
    }

    // The one place state changes.

    public void apply(long offset, ShardEvent event) {
        if (event instanceof Enqueued e) {
            Message message = new Message(new MessageId(shard, offset), e.payload(), e.priority(), e.at(), e.expiresAt());
            live.put(offset, message);
            addToLane(message);
            enqueued++;
            enqueueRate.record(e.at());
        } else if (event instanceof Delivered e) {
            Message message = find(e, MessageState.READY);
            removeFromLane(message);
            message.state = MessageState.IN_FLIGHT;
            message.deliveryCount++;
            message.visibleAt = e.visibleAt();
            byDeadline.put(TimeKey.deadline(message), message);
            delivered++;
            if (message.deliveryCount > 1) {
                redelivered++;
            }
        } else if (event instanceof Acked e) {
            Message message = find(e, MessageState.READY, MessageState.IN_FLIGHT);
            remove(message);
            acked++;
            ackRate.record(e.at());
        } else if (event instanceof Requeued e) {
            Message message = find(e, MessageState.IN_FLIGHT);
            byDeadline.remove(TimeKey.deadline(message));
            message.state = MessageState.READY;
            message.visibleAt = null;
            addToLane(message);
        } else if (event instanceof DeadLettered e) {
            Message message = find(e, MessageState.IN_FLIGHT);
            remove(message);
            message.state = MessageState.DEAD;
            message.visibleAt = null;
            deadLetters.put(message.offset(), message);
            deadLettered++;
        } else if (event instanceof Expired e) {
            Message message = find(e, MessageState.READY, MessageState.IN_FLIGHT);
            remove(message);
            expired++;
        } else {
            throw new IllegalArgumentException("unknown event " + event);
        }
    }

    // Views.

    public Optional<MessageView> find(long messageOffset) {
        Message message = live.get(messageOffset);
        if (message == null) {
            message = deadLetters.get(messageOffset);
        }
        return Optional.ofNullable(message).map(Message::view);
    }

    /** The rank of the highest priority that has a ready message, or -1 when nothing is ready. */
    public int highestReadyRank() {
        for (int rank = 0; rank < lanes.size(); rank++) {
            if (!lanes.get(rank).isEmpty()) {
                return rank;
            }
        }
        return -1;
    }

    /** Ready and in-flight messages, oldest first. */
    public List<MessageView> liveMessages() {
        return live.values().stream()
                .sorted(Comparator.comparingLong(Message::offset))
                .map(Message::view)
                .toList();
    }

    public List<MessageView> deadLetters() {
        return deadLetters.values().stream().map(Message::view).toList();
    }

    /** Reports the state as applied. Call {@link #reclaim} first so nothing overdue is counted. */
    public ShardMetrics metrics(Instant now) {
        List<Long> readyByPriority = new ArrayList<>(lanes.size());
        Instant oldest = null;
        for (NavigableMap<Long, Message> lane : lanes) {
            readyByPriority.add((long) lane.size());
            if (!lane.isEmpty()) {
                Instant head = lane.firstEntry().getValue().enqueuedAt;
                if (oldest == null || head.isBefore(oldest)) {
                    oldest = head;
                }
            }
        }
        Duration oldestReadyAge = oldest == null || oldest.isAfter(now) ? Duration.ZERO : Duration.between(oldest, now);
        return new ShardMetrics(List.copyOf(readyByPriority), byDeadline.size(), oldestReadyAge, deadLetters.size(),
                enqueued, delivered, redelivered, acked, deadLettered, expired,
                enqueueRate.perSecond(now), ackRate.perSecond(now));
    }

    private void addToLane(Message message) {
        lanes.get(message.priority).put(message.offset(), message);
        if (message.expiresAt != null) {
            byExpiry.put(TimeKey.expiry(message), message);
        }
    }

    private void removeFromLane(Message message) {
        lanes.get(message.priority).remove(message.offset());
        if (message.expiresAt != null) {
            byExpiry.remove(TimeKey.expiry(message));
        }
    }

    /** Takes a ready or in-flight message out of every structure. */
    private void remove(Message message) {
        live.remove(message.offset());
        if (message.state == MessageState.READY) {
            removeFromLane(message);
        } else {
            byDeadline.remove(TimeKey.deadline(message));
        }
    }

    private Message find(ShardEvent event, MessageState... allowed) {
        long messageOffset = messageOffsetOf(event);
        Message message = live.get(messageOffset);
        if (message == null || !Arrays.asList(allowed).contains(message.state)) {
            throw new IllegalStateException("cannot apply " + event + " in shard " + shard + ": message is "
                    + (message == null ? "not live" : message.state));
        }
        return message;
    }

    private static long messageOffsetOf(ShardEvent event) {
        if (event instanceof Delivered e) {
            return e.messageOffset();
        } else if (event instanceof Acked e) {
            return e.messageOffset();
        } else if (event instanceof Requeued e) {
            return e.messageOffset();
        } else if (event instanceof DeadLettered e) {
            return e.messageOffset();
        } else if (event instanceof Expired e) {
            return e.messageOffset();
        }
        throw new IllegalArgumentException(event + " does not refer to an existing message");
    }

    private static List<Message> dueBy(NavigableMap<TimeKey, Message> index, Instant now) {
        return List.copyOf(index.headMap(new TimeKey(now, Long.MAX_VALUE), true).values());
    }

    /** Orders messages by a time, then by offset so that equal times stay distinct. */
    private record TimeKey(Instant time, long offset) implements Comparable<TimeKey> {

        static TimeKey deadline(Message message) {
            return new TimeKey(message.visibleAt, message.offset());
        }

        static TimeKey expiry(Message message) {
            return new TimeKey(message.expiresAt, message.offset());
        }

        @Override
        public int compareTo(TimeKey other) {
            int byTime = time.compareTo(other.time);
            return byTime != 0 ? byTime : Long.compare(offset, other.offset);
        }
    }
}
