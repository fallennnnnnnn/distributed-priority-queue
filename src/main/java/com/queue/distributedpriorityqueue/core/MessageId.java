package com.queue.distributedpriorityqueue.core;

/**
 * Identifies a message within a queue: the shard it lives in and the log offset of its enqueue.
 * Written as {@code {shard}-{offset}}, so an acknowledge can be routed without a lookup.
 */
public record MessageId(int shard, long offset) {

    public MessageId {
        if (shard < 0 || offset < 0) {
            throw new IllegalArgumentException("shard and offset must not be negative: " + shard + "-" + offset);
        }
    }

    public static MessageId parse(String text) {
        int dash = text == null ? -1 : text.indexOf('-');
        if (dash <= 0) {
            throw new IllegalArgumentException("not a message ID: " + text);
        }
        try {
            return new MessageId(Integer.parseInt(text.substring(0, dash)), Long.parseLong(text.substring(dash + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a message ID: " + text);
        }
    }

    @Override
    public String toString() {
        return shard + "-" + offset;
    }
}
