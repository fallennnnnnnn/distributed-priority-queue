package com.queue.distributedpriorityqueue.log;

import com.queue.distributedpriorityqueue.core.ShardEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A shard log held in memory. It is never trimmed, so it grows for the life of the shard.
 * Thread-safe on its own, so replication can read it without taking the shard lock.
 */
public final class InMemoryShardLog implements ShardLog {

    private final List<LogEntry> entries = new ArrayList<>();
    /** The first offset written in each epoch. */
    private final NavigableMap<Integer, Long> epochStarts = new TreeMap<>();
    private long highWatermark;

    @Override
    public synchronized long append(int epoch, ShardEvent event) {
        if (!epochStarts.isEmpty() && epoch < epochStarts.lastKey()) {
            throw new IllegalArgumentException(
                    "epoch " + epoch + " is older than the log's latest epoch " + epochStarts.lastKey());
        }
        long offset = entries.size();
        entries.add(new LogEntry(offset, epoch, event));
        epochStarts.putIfAbsent(epoch, offset);
        return offset;
    }

    @Override
    public synchronized List<LogEntry> read(long fromOffset, int maxEntries) {
        if (fromOffset < 0 || maxEntries < 0) {
            throw new IllegalArgumentException("fromOffset and maxEntries must not be negative");
        }
        if (fromOffset >= entries.size()) {
            return List.of();
        }
        int from = (int) fromOffset;
        int to = (int) Math.min(entries.size(), fromOffset + maxEntries);
        return List.copyOf(entries.subList(from, to));
    }

    @Override
    public synchronized long endOffset() {
        return entries.size();
    }

    @Override
    public synchronized void truncate(long toOffset) {
        if (toOffset < highWatermark || toOffset > entries.size()) {
            throw new IllegalArgumentException("cannot truncate to " + toOffset + ": the high watermark is "
                    + highWatermark + " and the log ends at " + entries.size());
        }
        entries.subList((int) toOffset, entries.size()).clear();
        epochStarts.values().removeIf(start -> start >= toOffset);
    }

    @Override
    public synchronized long highWatermark() {
        return highWatermark;
    }

    @Override
    public synchronized void advanceHighWatermark(long highWatermark) {
        if (highWatermark < this.highWatermark || highWatermark > entries.size()) {
            throw new IllegalArgumentException("cannot move the high watermark from " + this.highWatermark
                    + " to " + highWatermark + ": the log ends at " + entries.size());
        }
        this.highWatermark = highWatermark;
    }

    @Override
    public synchronized Optional<EpochEnd> endOfEpoch(int epoch) {
        Map.Entry<Integer, Long> known = epochStarts.floorEntry(epoch);
        if (known == null) {
            return Optional.empty();
        }
        Map.Entry<Integer, Long> next = epochStarts.higherEntry(known.getKey());
        long endOffset = next == null ? entries.size() : next.getValue();
        return Optional.of(new EpochEnd(known.getKey(), endOffset));
    }
}
