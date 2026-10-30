// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.phoenix.model.TaskFailureReason;

class TaskTimeoutPolicyTest {

    private static TaskTimeoutPolicy policy() {
        return new TaskTimeoutPolicy(
                Duration.ofSeconds(60),
                Duration.ofSeconds(12),
                Duration.ofSeconds(15)
        );
    }

    @Test
    void exposesConfiguredWindows() {

        TaskTimeoutPolicy policy = policy();

        assertEquals(Duration.ofSeconds(60), policy.deliveryTimeout());
        assertEquals(Duration.ofSeconds(12), policy.executionTimeout());
        assertEquals(Duration.ofSeconds(15), policy.contactLiveness());
    }

    @Test
    void recentContactMeansBacklogNotUnreachable() {

        assertEquals(
                TaskFailureReason.DELIVERY_TIMEOUT,
                policy().classifyUndelivered(
                        Optional.of(Duration.ofSeconds(2))
                )
        );
    }

    @Test
    void contactExactlyAtTheLivenessLimitStillCountsAsInContact() {

        assertEquals(
                TaskFailureReason.DELIVERY_TIMEOUT,
                policy().classifyUndelivered(
                        Optional.of(Duration.ofSeconds(15))
                )
        );
    }

    @Test
    void staleContactMeansUnreachable() {

        assertEquals(
                TaskFailureReason.NODE_UNREACHABLE,
                policy().classifyUndelivered(
                        Optional.of(Duration.ofSeconds(16))
                )
        );
    }

    @Test
    void noContactEverMeansUnreachable() {

        assertEquals(
                TaskFailureReason.NODE_UNREACHABLE,
                policy().classifyUndelivered(Optional.empty())
        );
    }

    @Test
    void rejectsNonPositiveWindows() {

        assertThrows(
                IllegalArgumentException.class,
                () -> new TaskTimeoutPolicy(
                        Duration.ZERO,
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1))
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new TaskTimeoutPolicy(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(-1),
                        Duration.ofSeconds(1))
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new TaskTimeoutPolicy(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1),
                        null)
        );
    }
}
