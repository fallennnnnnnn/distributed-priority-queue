package com.queue.distributedpriorityqueue.core;

import java.time.Instant;

/**
 * One state transition in a shard's log. Applying the same events in the same order always gives
 * the same state, so every time value is stamped into the event by the leader and replicas never
 * read their own clocks.
 *
 * <p>Every event except {@link Enqueued} names its message by {@code messageOffset}: the log
 * offset of that message's {@code Enqueued} event.
 */
public sealed interface ShardEvent {

    /** When the leader decided this transition. */
    Instant at();

    /** A message entered its priority lane. {@code expiresAt} is null when it has no TTL. */
    record Enqueued(String payload, int priority, Instant at, Instant expiresAt) implements ShardEvent {
    }

    /** A message was handed to a consumer and is hidden until {@code visibleAt}. */
    record Delivered(long messageOffset, Instant at, Instant visibleAt) implements ShardEvent {
    }

    /** A consumer confirmed the message; it is gone for good. */
    record Acked(long messageOffset, Instant at) implements ShardEvent {
    }

    /** The visibility timeout passed with retries left; the message is back in its lane. */
    record Requeued(long messageOffset, Instant at) implements ShardEvent {
    }

    /** The visibility timeout passed with no retries left; the message is a dead letter. */
    record DeadLettered(long messageOffset, Instant at) implements ShardEvent {
    }

    /** The message outlived its TTL without being acknowledged. */
    record Expired(long messageOffset, Instant at) implements ShardEvent {
    }
}
