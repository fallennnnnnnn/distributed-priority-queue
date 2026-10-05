package com.queue.distributedpriorityqueue.coordination;

public record ShardId(String queue, int shard) {

    @Override
    public String toString() {
        return queue + "/" + shard;
    }
}
