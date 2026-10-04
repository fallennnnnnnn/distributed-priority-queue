package com.queue.distributedpriorityqueue.testsupport;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

/** A clock that only moves when a test moves it, so timeout and TTL tests need no sleeps. */
public final class ManualClock extends Clock {

    private final AtomicReference<Instant> now;
    private final ZoneId zone;

    public ManualClock() {
        this(Instant.parse("2026-01-01T00:00:00Z"));
    }

    public ManualClock(Instant start) {
        this(new AtomicReference<>(start), ZoneOffset.UTC);
    }

    private ManualClock(AtomicReference<Instant> now, ZoneId zone) {
        this.now = now;
        this.zone = zone;
    }

    public void advance(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("cannot advance by a negative duration: " + duration);
        }
        now.updateAndGet(instant -> instant.plus(duration));
    }

    public void set(Instant instant) {
        now.set(instant);
    }

    @Override
    public Instant instant() {
        return now.get();
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new ManualClock(now, zone);
    }
}
