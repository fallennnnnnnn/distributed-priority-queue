package com.queue.distributedpriorityqueue.core;

import java.time.Instant;

/**
 * An immutable copy of a message, safe to hand outside the shard lock. {@code priority} is the
 * rank in the queue's priority levels; {@code expiresAt} and {@code visibleAt} may be null.
 */
public record MessageView(
        MessageId id,
        String payload,
        int priority,
        MessageState state,
        int deliveryCount,
        Instant enqueuedAt,
        Instant expiresAt,
        Instant visibleAt) {
}
