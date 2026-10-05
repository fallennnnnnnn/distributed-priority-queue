package com.queue.distributedpriorityqueue.api;

import com.queue.distributedpriorityqueue.api.ApiModel.CreateQueueRequest;
import com.queue.distributedpriorityqueue.api.ApiModel.EnqueueRequest;
import com.queue.distributedpriorityqueue.api.ApiModel.EnqueueResponse;
import com.queue.distributedpriorityqueue.api.ApiModel.MessageResponse;
import com.queue.distributedpriorityqueue.api.ApiModel.QueueResponse;
import com.queue.distributedpriorityqueue.api.ApiModel.UpdateQueueRequest;
import com.queue.distributedpriorityqueue.cluster.BrokerSettings;
import com.queue.distributedpriorityqueue.cluster.QueueMetadata;
import com.queue.distributedpriorityqueue.cluster.QueueMetrics;
import com.queue.distributedpriorityqueue.cluster.QueueService;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.exception.MessageNotFoundException;
import com.queue.distributedpriorityqueue.replication.Acks;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/queues")
class QueueController {

    private final QueueService queues;
    private final BrokerSettings settings;
    private final Clock clock;

    QueueController(QueueService queues, BrokerSettings settings, Clock clock) {
        this.queues = queues;
        this.settings = settings;
        this.clock = clock;
    }

    @PostMapping
    ResponseEntity<QueueResponse> create(@RequestBody CreateQueueRequest request) {
        QueueConfig.Builder config = settings.queueDefaults().builder(request.name());
        if (request.priorityLevels() != null) {
            config.priorityLevels(request.priorityLevels());
        }
        if (request.visibilityTimeoutMs() != null) {
            config.visibilityTimeout(Duration.ofMillis(request.visibilityTimeoutMs()));
        }
        if (request.maxRetries() != null) {
            config.maxRetries(request.maxRetries());
        }
        if (request.defaultTtlMs() != null) {
            config.defaultTtl(Duration.ofMillis(request.defaultTtlMs()));
        }
        if (request.shards() != null) {
            config.shards(request.shards());
        }
        if (request.replicationFactor() != null) {
            config.replicationFactor(request.replicationFactor());
        }
        if (request.minInSyncReplicas() != null) {
            config.minInSyncReplicas(request.minInSyncReplicas());
        }
        if (request.uncleanLeaderElection() != null) {
            config.uncleanLeaderElection(request.uncleanLeaderElection());
        }
        QueueMetadata created = queues.createQueue(config.build());
        return ResponseEntity.created(URI.create("/queues/" + created.config().name())).body(QueueResponse.of(created));
    }

    @GetMapping("/{queue}")
    QueueResponse describe(@PathVariable String queue) {
        return QueueResponse.of(queues.describe(queue));
    }

    @PatchMapping("/{queue}")
    QueueResponse update(@PathVariable String queue, @RequestBody UpdateQueueRequest request) {
        return QueueResponse.of(queues.updateQueue(queue, config -> {
            if (request.visibilityTimeoutMs() != null) {
                config.visibilityTimeout(Duration.ofMillis(request.visibilityTimeoutMs()));
            }
            if (request.maxRetries() != null) {
                config.maxRetries(request.maxRetries());
            }
            if (request.defaultTtlMs() != null) {
                config.defaultTtl(request.defaultTtlMs() == 0 ? null : Duration.ofMillis(request.defaultTtlMs()));
            }
            if (request.minInSyncReplicas() != null) {
                config.minInSyncReplicas(request.minInSyncReplicas());
            }
            if (request.uncleanLeaderElection() != null) {
                config.uncleanLeaderElection(request.uncleanLeaderElection());
            }
            if (request.priorityLevels() != null) {
                config.priorityLevels(request.priorityLevels());
            }
            if (request.shards() != null) {
                config.shards(request.shards());
            }
            if (request.replicationFactor() != null) {
                config.replicationFactor(request.replicationFactor());
            }
            return config;
        }));
    }

    @PostMapping("/{queue}/messages")
    CompletableFuture<ResponseEntity<EnqueueResponse>> enqueue(@PathVariable String queue,
            @RequestBody EnqueueRequest request) {
        return queues.enqueue(queue, request.payload(), request.priority(), timeToLive(request), request.shardKey(),
                        acks(request))
                .thenApply(id -> ResponseEntity
                        .created(URI.create("/queues/" + queue + "/messages/" + id))
                        .body(new EnqueueResponse(id.toString())));
    }

    @PostMapping("/{queue}/messages/dequeue")
    CompletableFuture<ResponseEntity<MessageResponse>> dequeue(@PathVariable String queue,
            @RequestParam(defaultValue = "0") long waitTimeMs) {
        return queues.dequeue(queue, Duration.ofMillis(waitTimeMs)).thenApply(delivery -> delivery
                .map(message -> ResponseEntity.ok(MessageResponse.of(message, queues.describe(queue).config())))
                .orElseGet(() -> ResponseEntity.noContent().build()));
    }

    @PostMapping("/{queue}/messages/{messageId}/ack")
    CompletableFuture<ResponseEntity<Void>> ack(@PathVariable String queue, @PathVariable String messageId) {
        return queues.ack(queue, MessageId.parse(messageId)).thenApply(acked -> {
            if (!acked) {
                throw new MessageNotFoundException(queue, messageId);
            }
            return ResponseEntity.noContent().build();
        });
    }

    @GetMapping("/{queue}/metrics")
    CompletableFuture<QueueMetrics> metrics(@PathVariable String queue) {
        return queues.metrics(queue);
    }

    @GetMapping("/{queue}/dlq")
    CompletableFuture<List<MessageResponse>> deadLetters(@PathVariable String queue) {
        QueueConfig config = queues.describe(queue).config();
        return queues.deadLetters(queue).thenApply(
                deadLetters -> deadLetters.stream().map(message -> MessageResponse.of(message, config)).toList());
    }

    private static Acks acks(EnqueueRequest request) {
        if (request.acks() == null || request.acks().equalsIgnoreCase("all")) {
            return Acks.ALL;
        }
        if (request.acks().equalsIgnoreCase("leader")) {
            return Acks.LEADER;
        }
        throw new IllegalArgumentException("acks must be 'all' or 'leader'");
    }

    private Duration timeToLive(EnqueueRequest request) {
        if (request.ttlMs() != null && request.expiresAt() != null) {
            throw new IllegalArgumentException("give ttlMs or expiresAt, not both");
        }
        if (request.expiresAt() == null) {
            return ApiModel.millis(request.ttlMs());
        }
        Duration untilExpiry = Duration.between(clock.instant(), request.expiresAt());
        if (untilExpiry.isNegative() || untilExpiry.isZero()) {
            throw new IllegalArgumentException("expiresAt must be in the future");
        }
        return untilExpiry;
    }
}
