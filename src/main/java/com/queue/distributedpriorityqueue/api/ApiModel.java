package com.queue.distributedpriorityqueue.api;

import com.queue.distributedpriorityqueue.cluster.QueueMetadata;
import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardState;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** The JSON bodies of the REST API. Durations are in milliseconds. */
final class ApiModel {

    private ApiModel() {
    }

    /** Every field except {@code name} is optional; a missing one takes the broker's default. */
    record CreateQueueRequest(
            String name,
            List<String> priorityLevels,
            Long visibilityTimeoutMs,
            Integer maxRetries,
            Long defaultTtlMs,
            Integer shards,
            Integer replicationFactor,
            Integer minInSyncReplicas,
            Boolean uncleanLeaderElection) {
    }

    /**
     * Only the fields present are changed. A {@code defaultTtlMs} of 0 removes the default TTL.
     * The last three are fixed at creation and are accepted only so that an attempt to change
     * them is refused instead of ignored.
     */
    record UpdateQueueRequest(
            Long visibilityTimeoutMs,
            Integer maxRetries,
            Long defaultTtlMs,
            Integer minInSyncReplicas,
            Boolean uncleanLeaderElection,
            List<String> priorityLevels,
            Integer shards,
            Integer replicationFactor) {
    }

    record QueueResponse(
            String name,
            List<String> priorityLevels,
            long visibilityTimeoutMs,
            int maxRetries,
            Long defaultTtlMs,
            int shards,
            int replicationFactor,
            int minInSyncReplicas,
            boolean uncleanLeaderElection,
            List<ShardResponse> shardStates) {

        static QueueResponse of(QueueMetadata metadata) {
            QueueConfig config = metadata.config();
            List<ShardResponse> shardStates = new ArrayList<>();
            for (int shard = 0; shard < metadata.shards().size(); shard++) {
                shardStates.add(ShardResponse.of(shard, metadata.shard(shard)));
            }
            return new QueueResponse(config.name(), config.priorityLevels(), config.visibilityTimeout().toMillis(),
                    config.maxRetries(), config.defaultTtl() == null ? null : config.defaultTtl().toMillis(),
                    config.shards(), config.replicationFactor(), config.minInSyncReplicas(),
                    config.uncleanLeaderElection(), shardStates);
        }
    }

    /** {@code leader} is null while the shard has none. Brokers are named by their IDs. */
    record ShardResponse(int shard, String leader, int epoch, List<String> replicas, List<String> isr) {

        static ShardResponse of(int shard, ShardState state) {
            return new ShardResponse(shard, state.leader() == null ? null : state.leader().brokerId(), state.epoch(),
                    state.replicas(), state.isr().stream().map(ReplicaId::brokerId).toList());
        }
    }

    /**
     * Give {@code ttlMs} or {@code expiresAt}, not both; neither means the queue's default TTL.
     * {@code acks} is {@code all} (the default) or {@code leader}.
     */
    record EnqueueRequest(String payload, String priority, Long ttlMs, Instant expiresAt, String shardKey,
            String acks) {
    }

    record EnqueueResponse(String messageId) {
    }

    /** {@code visibleUntil} is null for a dead letter. */
    record MessageResponse(
            String messageId,
            String payload,
            String priority,
            int deliveryCount,
            Instant enqueuedAt,
            Instant visibleUntil) {

        static MessageResponse of(MessageView message, QueueConfig config) {
            return new MessageResponse(message.id().toString(), message.payload(),
                    config.levelName(message.priority()), message.deliveryCount(), message.enqueuedAt(),
                    message.visibleAt());
        }
    }

    static Duration millis(Long value) {
        return value == null ? null : Duration.ofMillis(value);
    }
}
