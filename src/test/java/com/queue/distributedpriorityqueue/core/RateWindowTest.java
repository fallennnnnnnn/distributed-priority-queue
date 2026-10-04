package com.queue.distributedpriorityqueue.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class RateWindowTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    private final RateWindow window = new RateWindow();

    @Test
    void isZeroWithNoEvents() {
        assertThat(window.perSecond(START)).isZero();
    }

    @Test
    void averagesOverSixtySeconds() {
        for (int i = 0; i < 30; i++) {
            window.record(START.plusMillis(i));
        }

        assertThat(window.perSecond(START)).isEqualTo(0.5);
        assertThat(window.perSecond(START.plusSeconds(59))).isEqualTo(0.5);
    }

    @Test
    void forgetsEventsOlderThanSixtySeconds() {
        window.record(START);
        window.record(START.plusSeconds(30));

        assertThat(window.perSecond(START.plusSeconds(60))).isEqualTo(1 / 60.0);
        assertThat(window.perSecond(START.plusSeconds(90))).isZero();
    }

    @Test
    void reusesABucketForALaterSecond() {
        window.record(START);
        window.record(START.plusSeconds(60));
        window.record(START.plusSeconds(60));

        assertThat(window.perSecond(START.plusSeconds(60))).isEqualTo(2 / 60.0);
    }

    @Test
    void ignoresAnEventOlderThanWhatItsBucketHolds() {
        window.record(START.plusSeconds(60));
        window.record(START);

        assertThat(window.perSecond(START.plusSeconds(60))).isEqualTo(1 / 60.0);
    }
}
