package com.queue.distributedpriorityqueue.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.queue.distributedpriorityqueue.testsupport.ManualClock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A follower becomes equal to its leader by applying the leader's events. This drives one state
 * machine the way a leader does, then replays the recorded events into a fresh one.
 */
class ShardStateMachineReplayTest {

    private static final int LEVELS = 3;
    private static final Duration VISIBILITY = Duration.ofSeconds(30);
    private static final int MAX_RETRIES = 1;

    private final ManualClock clock = new ManualClock();
    private final ShardStateMachine leader = new ShardStateMachine(0, LEVELS);
    private final List<ShardEvent> log = new ArrayList<>();

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 42, 2026})
    void replayingTheEventsReproducesTheState(long seed) {
        runRandomWorkload(new Random(seed), 2_000);

        ShardStateMachine replayed = new ShardStateMachine(0, LEVELS);
        for (int offset = 0; offset < log.size(); offset++) {
            replayed.apply(offset, log.get(offset));
        }

        Instant now = clock.instant();
        assertThat(replayed.liveMessages()).isEqualTo(leader.liveMessages());
        assertThat(replayed.deadLetters()).isEqualTo(leader.deadLetters());
        assertThat(replayed.metrics(now)).isEqualTo(leader.metrics(now));
        assertThat(replayed.reclaim(now, MAX_RETRIES)).isEqualTo(leader.reclaim(now, MAX_RETRIES));
        assertThat(replayed.dequeue(now, VISIBILITY)).isEqualTo(leader.dequeue(now, VISIBILITY));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 42, 2026})
    void theWorkloadReachesEveryKindOfTransition(long seed) {
        runRandomWorkload(new Random(seed), 2_000);

        ShardMetrics metrics = leader.metrics(clock.instant());
        assertThat(metrics.ackedTotal()).isPositive();
        assertThat(metrics.redeliveredTotal()).isPositive();
        assertThat(metrics.deadLetteredTotal()).isPositive();
        assertThat(metrics.expiredTotal()).isPositive();
        assertThat(metrics.enqueuedTotal()).isEqualTo(metrics.ready() + metrics.inFlight()
                + metrics.ackedTotal() + metrics.deadLetteredTotal() + metrics.expiredTotal());
    }

    private void runRandomWorkload(Random random, int steps) {
        List<Long> delivered = new ArrayList<>();
        for (int step = 0; step < steps; step++) {
            Instant now = clock.instant();
            switch (random.nextInt(10)) {
                case 0, 1, 2, 3 -> {
                    Instant expiresAt = random.nextInt(4) == 0 ? now.plusSeconds(1 + random.nextInt(90)) : null;
                    record(leader.enqueue("payload-" + step, random.nextInt(LEVELS), expiresAt, now));
                }
                case 4, 5, 6 -> {
                    reclaim(now);
                    leader.dequeue(now, VISIBILITY).ifPresent(event -> {
                        record(event);
                        delivered.add(event.messageOffset());
                    });
                }
                case 7 -> {
                    if (!delivered.isEmpty()) {
                        long messageOffset = delivered.remove(random.nextInt(delivered.size()));
                        leader.ack(messageOffset, now).ifPresent(this::record);
                    }
                }
                case 8 -> reclaim(now);
                default -> clock.advance(Duration.ofSeconds(random.nextInt(20)));
            }
        }
    }

    private void reclaim(Instant now) {
        leader.reclaim(now, MAX_RETRIES).forEach(this::record);
    }

    private void record(ShardEvent event) {
        leader.apply(log.size(), event);
        log.add(event);
    }
}
