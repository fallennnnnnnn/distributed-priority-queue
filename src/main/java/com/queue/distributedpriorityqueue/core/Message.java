package com.queue.distributedpriorityqueue.core;

import java.time.Instant;

/** A message as the state machine holds it. Only {@link ShardStateMachine} changes it. */
final class Message {

    final MessageId id;
    final String payload;
    final int priority;
    final Instant enqueuedAt;
    final Instant expiresAt;

    MessageState state = MessageState.READY;
    int deliveryCount;
    Instant visibleAt;

    Message(MessageId id, String payload, int priority, Instant enqueuedAt, Instant expiresAt) {
        this.id = id;
        this.payload = payload;
        this.priority = priority;
        this.enqueuedAt = enqueuedAt;
        this.expiresAt = expiresAt;
    }

    long offset() {
        return id.offset();
    }

    boolean hasExpiredBy(Instant now) {
        return expiresAt != null && !expiresAt.isAfter(now);
    }

    MessageView view() {
        return new MessageView(id, payload, priority, state, deliveryCount, enqueuedAt, expiresAt, visibleAt);
    }
}
