package com.queue.distributedpriorityqueue.core;

import java.time.Duration;
import java.util.List;

/**
 * A consistent snapshot of one shard's numbers. {@code readyByPriority} is indexed by priority
 * rank. {@code oldestReadyAge} is zero when nothing is ready. The two rates cover the last 60 seconds.
 */
public record ShardMetrics(
        List<Long> readyByPriority,
        long inFlight,
        Duration oldestReadyAge,
        long deadLetters,
        long enqueuedTotal,
        long deliveredTotal,
        long redeliveredTotal,
        long ackedTotal,
        long deadLetteredTotal,
        long expiredTotal,
        double enqueuedPerSecond,
        double ackedPerSecond) {

    public long ready() {
        return readyByPriority.stream().mapToLong(Long::longValue).sum();
    }
}
