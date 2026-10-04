package com.queue.distributedpriorityqueue.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class QueueConfigTest {

    @Test
    void defaultsAreOneShardAndNoReplication() {
        QueueConfig config = QueueConfig.builder("orders").build();

        assertThat(config.name()).isEqualTo("orders");
        assertThat(config.priorityLevels()).containsExactly("HIGH", "MEDIUM", "LOW");
        assertThat(config.visibilityTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.maxRetries()).isEqualTo(3);
        assertThat(config.defaultTtl()).isNull();
        assertThat(config.shards()).isEqualTo(1);
        assertThat(config.replicationFactor()).isEqualTo(1);
        assertThat(config.minInSyncReplicas()).isEqualTo(1);
        assertThat(config.uncleanLeaderElection()).isFalse();
    }

    @Test
    void toBuilderCopiesEverySetting() {
        QueueConfig config = QueueConfig.builder("orders")
                .priorityLevels("urgent", "normal")
                .visibilityTimeout(Duration.ofSeconds(5))
                .maxRetries(7)
                .defaultTtl(Duration.ofMinutes(1))
                .shards(4)
                .replicationFactor(3)
                .minInSyncReplicas(2)
                .uncleanLeaderElection(true)
                .build();

        assertThat(config.toBuilder().build()).isEqualTo(config);
    }

    @Test
    void ranksLevelsHighestFirst() {
        QueueConfig config = QueueConfig.builder("orders").build();

        assertThat(config.rankOf("HIGH")).isZero();
        assertThat(config.rankOf("MEDIUM")).isEqualTo(1);
        assertThat(config.rankOf("LOW")).isEqualTo(2);
        assertThat(config.levelName(1)).isEqualTo("MEDIUM");
    }

    @Test
    void levelNamesIgnoreCase() {
        assertThat(QueueConfig.builder("orders").build().rankOf("high")).isZero();
    }

    @Test
    void noLevelMeansTheLowest() {
        assertThat(QueueConfig.builder("orders").build().rankOf(null)).isEqualTo(2);
    }

    @Test
    void unknownLevelIsRejected() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> QueueConfig.builder("orders").build().rankOf("URGENT"))
                .withMessageContaining("URGENT");
    }

    @Test
    void acceptsOneToTenLevels() {
        assertThat(QueueConfig.builder("q").priorityLevels("only").build().lowestRank()).isZero();
        assertThat(QueueConfig.builder("q").priorityLevels(levels(10)).build().lowestRank()).isEqualTo(9);
    }

    @Test
    void rejectsBadPriorityLevels() {
        assertInvalid(QueueConfig.builder("q").priorityLevels(List.of()));
        assertInvalid(QueueConfig.builder("q").priorityLevels(levels(11)));
        assertInvalid(QueueConfig.builder("q").priorityLevels("HIGH", "LOW", "high"));
        assertInvalid(QueueConfig.builder("q").priorityLevels("HIGH", " "));
        assertInvalid(QueueConfig.builder("q").priorityLevels("has space"));
    }

    @Test
    void rejectsBadNames() {
        assertInvalid(QueueConfig.builder(null));
        assertInvalid(QueueConfig.builder(""));
        assertInvalid(QueueConfig.builder("has space"));
        assertInvalid(QueueConfig.builder("a/b"));
        assertInvalid(QueueConfig.builder("-leading-dash"));
    }

    @Test
    void rejectsBadNumbersAndDurations() {
        assertInvalid(QueueConfig.builder("q").visibilityTimeout(Duration.ZERO));
        assertInvalid(QueueConfig.builder("q").visibilityTimeout(null));
        assertInvalid(QueueConfig.builder("q").maxRetries(-1));
        assertInvalid(QueueConfig.builder("q").defaultTtl(Duration.ofSeconds(-1)));
        assertInvalid(QueueConfig.builder("q").shards(0));
        assertInvalid(QueueConfig.builder("q").replicationFactor(0));
        assertInvalid(QueueConfig.builder("q").minInSyncReplicas(0));
        assertInvalid(QueueConfig.builder("q").replicationFactor(2).minInSyncReplicas(3));
    }

    @Test
    void acceptsZeroRetries() {
        assertThat(QueueConfig.builder("q").maxRetries(0).build().maxRetries()).isZero();
    }

    private static List<String> levels(int count) {
        return IntStream.range(0, count).mapToObj(i -> "p" + i).toList();
    }

    private static void assertInvalid(QueueConfig.Builder builder) {
        assertThatIllegalArgumentException().isThrownBy(builder::build);
    }
}
