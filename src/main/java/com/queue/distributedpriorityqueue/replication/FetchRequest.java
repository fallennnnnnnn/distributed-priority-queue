package com.queue.distributedpriorityqueue.replication;

import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;

/**
 * What a follower says when it opens a fetch stream: who it is, which leader epoch it believes in
 * and where its log ends. {@code lastEntryEpoch} is the epoch of its last entry, or -1 if its log
 * is empty; the leader uses it to check that the two logs agree up to {@code fetchOffset}.
 */
public record FetchRequest(ShardId shard, ReplicaId follower, int epoch, long fetchOffset, int lastEntryEpoch) {
}
