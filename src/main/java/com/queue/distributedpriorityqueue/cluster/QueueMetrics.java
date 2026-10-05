package com.queue.distributedpriorityqueue.cluster;

import java.util.Map;

/**
 * One queue's numbers, summed over its shards. {@code ready} maps each priority level, highest
 * first, to its ready count. The oldest message age is the largest of the shards' ages.
 */
public record QueueMetrics(
        String queue,
        Map<String, Long> ready,
        long readyTotal,
        long inFlight,
        double oldestMessageAgeSeconds,
        long deadLetters,
        long enqueuedTotal,
        long deliveredTotal,
        long redeliveredTotal,
        long ackedTotal,
        long deadLetteredTotal,
        long expiredTotal,
        double enqueuedPerSecond,
        double ackedPerSecond) {
}
