// SPDX-License-Identifier: Apache-2.0

package com.phoenix.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.phoenix.dispatch.PendingTaskQueue;

/** Invalid retry limits are rejected at startup instead of misbehaving later. */
class RecoveryConfigurationTest {

    @Test
    void maxAttemptsMustBeAtLeastOne() {
        assertThrows(IllegalArgumentException.class, () -> new TaskRecoveryManager(0));
        assertThrows(IllegalArgumentException.class, () -> new TaskRecoveryManager(-3));
        assertEquals(1, new TaskRecoveryManager(1).maxAttempts());
    }

    @Test
    void theWaitingWindowMustBePositive() {
        Clock clock = Clock.systemUTC();

        assertThrows(IllegalArgumentException.class,
                () -> new PendingTaskQueue(clock, Duration.ZERO, 3));
        assertThrows(IllegalArgumentException.class,
                () -> new PendingTaskQueue(clock, Duration.ofSeconds(-1), 3));
        assertThrows(IllegalArgumentException.class,
                () -> new PendingTaskQueue(clock, null, 3));
    }

    @Test
    void theRecoveryBudgetMustBeAtLeastOne() {
        assertThrows(IllegalArgumentException.class,
                () -> new PendingTaskQueue(Clock.systemUTC(), Duration.ofSeconds(60), 0));
    }
}
