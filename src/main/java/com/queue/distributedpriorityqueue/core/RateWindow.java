package com.queue.distributedpriorityqueue.core;

import java.time.Instant;

/** Counts events in one-second buckets and reports the average rate over the last 60 seconds. */
final class RateWindow {

    static final int SECONDS = 60;

    private final long[] bucketSecond = new long[SECONDS];
    private final long[] bucketCount = new long[SECONDS];

    void record(Instant at) {
        long second = at.getEpochSecond();
        int bucket = (int) Math.floorMod(second, (long) SECONDS);
        if (second < bucketSecond[bucket]) {
            return;
        }
        if (second > bucketSecond[bucket]) {
            bucketSecond[bucket] = second;
            bucketCount[bucket] = 0;
        }
        bucketCount[bucket]++;
    }

    double perSecond(Instant now) {
        long newest = now.getEpochSecond();
        long total = 0;
        for (int bucket = 0; bucket < SECONDS; bucket++) {
            if (bucketSecond[bucket] > newest - SECONDS && bucketSecond[bucket] <= newest) {
                total += bucketCount[bucket];
            }
        }
        return total / (double) SECONDS;
    }
}
