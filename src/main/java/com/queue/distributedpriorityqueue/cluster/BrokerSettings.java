package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.core.QueueConfig;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * What one broker is told at startup, bound from {@code dpq.*}. The defaults are the ones in
 * docs/api.md, so a broker started with no configuration runs as a single-broker cluster.
 */
@Validated
@ConfigurationProperties("dpq")
public record BrokerSettings(
        @DefaultValue("broker-1") @NotBlank String brokerId,
        @DefaultValue @Valid Advertised advertised,
        @DefaultValue @Valid Coordinator coordinator,
        @DefaultValue @Valid QueueDefaults queueDefaults,
        @DefaultValue("5s") @DurationMin(millis = 1) Duration failureDetectionTimeout,
        @DefaultValue("5s") @DurationMin(millis = 1) Duration replicaLagMax,
        @DefaultValue("10s") @DurationMin(millis = 1) Duration commitTimeout,
        @DefaultValue("100ms") @DurationMin(millis = 1) Duration sweeperInterval,
        @DefaultValue("200ms") @DurationMin(millis = 1) Duration summaryInterval) {

    /** The address clients and other brokers use to reach this broker. */
    public record Advertised(
            @DefaultValue("localhost") @NotBlank String host,
            @DefaultValue("8080") @Min(1) @Max(65535) int httpPort,
            @DefaultValue("9090") @Min(1) @Max(65535) int grpcPort) {
    }

    public enum CoordinatorType { MEMORY, ETCD }

    public record Coordinator(
            @DefaultValue("memory") @NotNull CoordinatorType type,
            List<String> endpoints) {

        public Coordinator {
            endpoints = endpoints == null ? List.of() : List.copyOf(endpoints);
        }

        @AssertTrue(message = "endpoints are required when type is etcd")
        public boolean isEndpointsGivenForEtcd() {
            return type != CoordinatorType.ETCD || !endpoints.isEmpty();
        }
    }

    /** Settings a new queue gets when the create request leaves them out. */
    public record QueueDefaults(
            @DefaultValue({"HIGH", "MEDIUM", "LOW"}) @Size(min = 1, max = 10) List<@NotBlank String> priorityLevels,
            @DefaultValue("30s") @DurationMin(millis = 1) Duration visibilityTimeout,
            @DefaultValue("3") @Min(0) int maxRetries,
            @DurationMin(millis = 1) Duration defaultTtl,
            @DefaultValue("1") @Min(1) int shards,
            @DefaultValue("1") @Min(1) int replicationFactor,
            @DefaultValue("1") @Min(1) int minInSyncReplicas,
            @DefaultValue("false") boolean uncleanLeaderElection) {

        public QueueDefaults {
            priorityLevels = List.copyOf(priorityLevels);
        }

        /** A new queue's settings before the create request overrides any of them. */
        public QueueConfig.Builder builder(String queueName) {
            return QueueConfig.builder(queueName)
                    .priorityLevels(priorityLevels)
                    .visibilityTimeout(visibilityTimeout)
                    .maxRetries(maxRetries)
                    .defaultTtl(defaultTtl)
                    .shards(shards)
                    .replicationFactor(replicationFactor)
                    .minInSyncReplicas(minInSyncReplicas)
                    .uncleanLeaderElection(uncleanLeaderElection);
        }

        @AssertTrue(message = "priority level names must be unique")
        public boolean isPriorityLevelsUnique() {
            return new HashSet<>(priorityLevels).size() == priorityLevels.size();
        }

        @AssertTrue(message = "min-in-sync-replicas must not exceed replication-factor")
        public boolean isMinInSyncReplicasWithinReplicationFactor() {
            return minInSyncReplicas <= replicationFactor;
        }
    }
}
