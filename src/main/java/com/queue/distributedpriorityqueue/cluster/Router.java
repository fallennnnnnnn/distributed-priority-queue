package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.core.QueueConfig;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntUnaryOperator;

/** Chooses the shard for a request. Priority and FIFO are strict within a shard, best-effort across shards. */
final class Router {

    private final Map<String, AtomicLong> enqueueTurns = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> dequeueTurns = new ConcurrentHashMap<>();

    /**
     * Messages with the same key always land in the same shard, so they keep their order.
     * Without a key, shards take turns. {@code String.hashCode} is the same on every broker.
     */
    int shardForEnqueue(QueueConfig config, String shardKey) {
        if (shardKey != null) {
            int hash = shardKey.hashCode();
            return Math.floorMod(hash ^ (hash >>> 16), config.shards());
        }
        return nextTurn(enqueueTurns, config);
    }

    /**
     * The order in which a dequeue tries the shards: the shard holding the highest ready priority
     * first. Shards that tie take turns, so no shard is favoured. Every shard is listed, because
     * the ready hint can be a moment out of date.
     *
     * @param highestReadyRank for a shard number, the rank of its highest ready priority, or -1 if none
     */
    List<Integer> dequeueOrder(QueueConfig config, IntUnaryOperator highestReadyRank) {
        int shards = config.shards();
        int first = nextTurn(dequeueTurns, config);
        List<Integer> order = new ArrayList<>(shards);
        for (int i = 0; i < shards; i++) {
            order.add((first + i) % shards);
        }
        order.sort(Comparator.comparingInt(shard -> {
            int rank = highestReadyRank.applyAsInt(shard);
            return rank < 0 ? Integer.MAX_VALUE : rank;
        }));
        return order;
    }

    private static int nextTurn(Map<String, AtomicLong> turns, QueueConfig config) {
        long turn = turns.computeIfAbsent(config.name(), name -> new AtomicLong()).getAndIncrement();
        return (int) Math.floorMod(turn, (long) config.shards());
    }
}
