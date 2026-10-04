package com.queue.distributedpriorityqueue.testsupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class ManualClockTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void standsStillUntilMoved() {
        ManualClock clock = new ManualClock(START);

        assertThat(clock.instant()).isEqualTo(START);
        assertThat(clock.instant()).isEqualTo(START);
    }

    @Test
    void advanceMovesTimeForward() {
        ManualClock clock = new ManualClock(START);

        clock.advance(Duration.ofSeconds(30));
        clock.advance(Duration.ofMillis(500));

        assertThat(clock.instant()).isEqualTo(START.plusMillis(30_500));
        assertThat(clock.millis()).isEqualTo(START.toEpochMilli() + 30_500);
    }

    @Test
    void setJumpsToAnInstant() {
        ManualClock clock = new ManualClock(START);

        clock.set(START.minusSeconds(60));

        assertThat(clock.instant()).isEqualTo(START.minusSeconds(60));
    }

    @Test
    void advanceRejectsNegativeDuration() {
        ManualClock clock = new ManualClock(START);

        assertThatIllegalArgumentException().isThrownBy(() -> clock.advance(Duration.ofSeconds(-1)));
        assertThat(clock.instant()).isEqualTo(START);
    }

    @Test
    void zonedViewSharesTheSameTime() {
        ManualClock clock = new ManualClock(START);
        Clock tokyo = clock.withZone(ZoneId.of("Asia/Tokyo"));

        clock.advance(Duration.ofHours(1));

        assertThat(tokyo.instant()).isEqualTo(START.plusSeconds(3600));
        assertThat(tokyo.getZone()).isEqualTo(ZoneId.of("Asia/Tokyo"));
    }
}
