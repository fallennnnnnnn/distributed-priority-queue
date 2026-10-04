package com.queue.distributedpriorityqueue.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.queue.distributedpriorityqueue.core.ShardEvent;
import com.queue.distributedpriorityqueue.log.ShardLog.EpochEnd;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class InMemoryShardLogTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final InMemoryShardLog log = new InMemoryShardLog();

    @Test
    void startsEmpty() {
        assertThat(log.endOffset()).isZero();
        assertThat(log.highWatermark()).isZero();
        assertThat(log.read(0, 10)).isEmpty();
        assertThat(log.endOfEpoch(0)).isEmpty();
    }

    @Test
    void appendAssignsConsecutiveOffsets() {
        assertThat(log.append(0, event("a"))).isZero();
        assertThat(log.append(0, event("b"))).isEqualTo(1);
        assertThat(log.append(2, event("c"))).isEqualTo(2);

        assertThat(log.endOffset()).isEqualTo(3);
        assertThat(log.read(0, 10)).containsExactly(
                new LogEntry(0, 0, event("a")), new LogEntry(1, 0, event("b")), new LogEntry(2, 2, event("c")));
    }

    @Test
    void readsARangeFromAnOffset() {
        append(0, "a", "b", "c", "d");

        assertThat(log.read(1, 2)).extracting(LogEntry::offset).containsExactly(1L, 2L);
        assertThat(log.read(3, 10)).extracting(LogEntry::offset).containsExactly(3L);
        assertThat(log.read(4, 10)).isEmpty();
        assertThat(log.read(99, 10)).isEmpty();
        assertThat(log.read(0, 0)).isEmpty();
    }

    @Test
    void epochsCannotGoBackwards() {
        log.append(3, event("a"));

        assertThatIllegalArgumentException().isThrownBy(() -> log.append(2, event("b")));
        assertThat(log.endOffset()).isEqualTo(1);
    }

    @Test
    void highWatermarkMovesForwardUpToTheEnd() {
        append(0, "a", "b", "c");

        log.advanceHighWatermark(2);
        log.advanceHighWatermark(2);
        log.advanceHighWatermark(3);

        assertThat(log.highWatermark()).isEqualTo(3);
        assertThatIllegalArgumentException().isThrownBy(() -> log.advanceHighWatermark(2));
        assertThatIllegalArgumentException().isThrownBy(() -> log.advanceHighWatermark(4));
    }

    @Test
    void truncateDropsTheTail() {
        append(0, "a", "b", "c", "d");

        log.truncate(2);

        assertThat(log.endOffset()).isEqualTo(2);
        assertThat(log.read(0, 10)).extracting(LogEntry::offset).containsExactly(0L, 1L);
        assertThat(log.append(1, event("e"))).isEqualTo(2);
    }

    @Test
    void truncateCannotDropCommittedEntries() {
        append(0, "a", "b", "c");
        log.advanceHighWatermark(2);

        assertThatIllegalArgumentException().isThrownBy(() -> log.truncate(1));
        assertThatIllegalArgumentException().isThrownBy(() -> log.truncate(4));
        log.truncate(2);
        assertThat(log.endOffset()).isEqualTo(2);
    }

    @Test
    void endOfEpochIsWhereTheNextEpochStarts() {
        append(1, "a", "b");
        append(2, "c");
        append(5, "d", "e");

        assertThat(log.endOfEpoch(1)).contains(new EpochEnd(1, 2));
        assertThat(log.endOfEpoch(2)).contains(new EpochEnd(2, 3));
        assertThat(log.endOfEpoch(5)).contains(new EpochEnd(5, 5));
    }

    @Test
    void endOfEpochFallsBackToTheClosestEarlierEpoch() {
        append(1, "a", "b");
        append(5, "c");

        assertThat(log.endOfEpoch(3)).contains(new EpochEnd(1, 2));
        assertThat(log.endOfEpoch(9)).contains(new EpochEnd(5, 3));
        assertThat(log.endOfEpoch(0)).isEmpty();
    }

    @Test
    void truncateForgetsEpochsItRemoved() {
        append(1, "a", "b");
        append(2, "c", "d");

        log.truncate(2);

        assertThat(log.endOfEpoch(2)).contains(new EpochEnd(1, 2));
        log.truncate(1);
        assertThat(log.endOfEpoch(1)).contains(new EpochEnd(1, 1));
    }

    private void append(int epoch, String... payloads) {
        for (String payload : payloads) {
            log.append(epoch, event(payload));
        }
    }

    private static ShardEvent event(String payload) {
        return new ShardEvent.Enqueued(payload, 0, NOW, null);
    }
}
