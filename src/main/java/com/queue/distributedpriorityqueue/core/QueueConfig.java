package com.queue.distributedpriorityqueue.core;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The settings of one queue. Priority levels are listed highest first; a message's priority is
 * stored as its rank in that list, 0 being the highest.
 */
public record QueueConfig(
        String name,
        List<String> priorityLevels,
        Duration visibilityTimeout,
        int maxRetries,
        Duration defaultTtl,
        int shards,
        int replicationFactor,
        int minInSyncReplicas,
        boolean uncleanLeaderElection) {

    public static final int MAX_PRIORITY_LEVELS = 10;

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
    private static final Pattern LEVEL = Pattern.compile("[A-Za-z0-9_-]{1,32}");

    public QueueConfig {
        require(name != null && NAME.matcher(name).matches(),
                "queue name must be 1 to 128 letters, digits, dots, dashes or underscores: " + name);
        require(priorityLevels != null && !priorityLevels.isEmpty() && priorityLevels.size() <= MAX_PRIORITY_LEVELS,
                "a queue needs between 1 and " + MAX_PRIORITY_LEVELS + " priority levels");
        priorityLevels = List.copyOf(priorityLevels);
        Set<String> seen = new HashSet<>();
        for (String level : priorityLevels) {
            require(LEVEL.matcher(level).matches(),
                    "a priority level must be 1 to 32 letters, digits, dashes or underscores: " + level);
            require(seen.add(level.toLowerCase(Locale.ROOT)), "priority level names must be unique: " + level);
        }
        require(visibilityTimeout != null && isPositive(visibilityTimeout), "visibilityTimeout must be positive");
        require(maxRetries >= 0, "maxRetries must not be negative");
        require(defaultTtl == null || isPositive(defaultTtl), "defaultTtl must be positive when set");
        require(shards >= 1, "shards must be at least 1");
        require(replicationFactor >= 1, "replicationFactor must be at least 1");
        require(minInSyncReplicas >= 1 && minInSyncReplicas <= replicationFactor,
                "minInSyncReplicas must be between 1 and replicationFactor");
    }

    /** The rank of a level name, ignoring case. No name means the lowest level. */
    public int rankOf(String level) {
        if (level == null) {
            return lowestRank();
        }
        for (int rank = 0; rank < priorityLevels.size(); rank++) {
            if (priorityLevels.get(rank).equalsIgnoreCase(level)) {
                return rank;
            }
        }
        throw new IllegalArgumentException(
                "unknown priority level '" + level + "' for queue " + name + "; its levels are " + priorityLevels);
    }

    public String levelName(int rank) {
        return priorityLevels.get(rank);
    }

    public int lowestRank() {
        return priorityLevels.size() - 1;
    }

    /** Starts from the defaults in README section 7: one shard and no replication. */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    public Builder toBuilder() {
        return new Builder(name)
                .priorityLevels(priorityLevels)
                .visibilityTimeout(visibilityTimeout)
                .maxRetries(maxRetries)
                .defaultTtl(defaultTtl)
                .shards(shards)
                .replicationFactor(replicationFactor)
                .minInSyncReplicas(minInSyncReplicas)
                .uncleanLeaderElection(uncleanLeaderElection);
    }

    private static boolean isPositive(Duration duration) {
        return !duration.isNegative() && !duration.isZero();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public static final class Builder {

        private final String name;
        private List<String> priorityLevels = List.of("HIGH", "MEDIUM", "LOW");
        private Duration visibilityTimeout = Duration.ofSeconds(30);
        private int maxRetries = 3;
        private Duration defaultTtl;
        private int shards = 1;
        private int replicationFactor = 1;
        private int minInSyncReplicas = 1;
        private boolean uncleanLeaderElection;

        private Builder(String name) {
            this.name = name;
        }

        public Builder priorityLevels(List<String> priorityLevels) {
            this.priorityLevels = priorityLevels;
            return this;
        }

        public Builder priorityLevels(String... priorityLevels) {
            return priorityLevels(List.of(priorityLevels));
        }

        public Builder visibilityTimeout(Duration visibilityTimeout) {
            this.visibilityTimeout = visibilityTimeout;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder defaultTtl(Duration defaultTtl) {
            this.defaultTtl = defaultTtl;
            return this;
        }

        public Builder shards(int shards) {
            this.shards = shards;
            return this;
        }

        public Builder replicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public Builder minInSyncReplicas(int minInSyncReplicas) {
            this.minInSyncReplicas = minInSyncReplicas;
            return this;
        }

        public Builder uncleanLeaderElection(boolean uncleanLeaderElection) {
            this.uncleanLeaderElection = uncleanLeaderElection;
            return this;
        }

        public QueueConfig build() {
            return new QueueConfig(name, priorityLevels, visibilityTimeout, maxRetries, defaultTtl, shards,
                    replicationFactor, minInSyncReplicas, uncleanLeaderElection);
        }
    }
}
