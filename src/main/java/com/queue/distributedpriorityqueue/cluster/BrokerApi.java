package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.replication.Acks;
import com.queue.distributedpriorityqueue.replication.FetchListener;
import com.queue.distributedpriorityqueue.replication.FetchRequest;
import com.queue.distributedpriorityqueue.replication.FetchStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * What one broker asks of another. A broker implements it to serve the shards it leads; a
 * transport gives every other broker an object with the same interface that carries the calls
 * across.
 *
 * <p>The request methods fail with {@code NotLeaderException} when the broker asked does not lead
 * the shard. They are never passed on to a third broker.
 */
public interface BrokerApi {

    CompletableFuture<MessageId> enqueue(ShardId shard, String payload, String priority, Duration ttl, Acks acks);

    CompletableFuture<Optional<MessageView>> dequeue(ShardId shard);

    CompletableFuture<Boolean> ack(ShardId shard, MessageId messageId);

    CompletableFuture<ShardMetrics> metrics(ShardId shard);

    CompletableFuture<List<MessageView>> deadLetters(ShardId shard);

    /** For each shard this broker leads, the rank of its highest ready priority, or -1 if nothing is ready. */
    CompletableFuture<Map<ShardId, Integer>> readySummary();

    /** Opens a replication stream from this broker, as leader, to a follower. */
    FetchStream openFetch(FetchRequest request, FetchListener toFollower);
}
