// SPDX-License-Identifier: Apache-2.0

package com.phoenix.testsupport;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** Deterministic, manually advanced clock for lifecycle/timeout tests. */
public final class MutableClock extends Clock {

    private volatile Instant now;

    public MutableClock() {
        this(Instant.parse("2026-01-01T00:00:00Z"));
    }

    public MutableClock(Instant start) {
        this.now = start;
    }

    public void advance(Duration duration) {
        now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return now;
    }
}
