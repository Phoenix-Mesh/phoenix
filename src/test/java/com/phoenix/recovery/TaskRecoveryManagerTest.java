// SPDX-License-Identifier: Apache-2.0

package com.phoenix.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;

class TaskRecoveryManagerTest {

    @Test
    void successIsNotRetryable() {

        TaskResult result =
                new TaskResult(
                        "task-001",
                        "SUCCESS",
                        "done"
                );

        assertEquals(
                TaskFailureReason.NONE,
                result.failureReason()
        );

        assertFalse(
                result.isRetryable()
        );
    }

    @Test
    void executionErrorIsNotAutomaticallyRetryable() {

        TaskResult result =
                new TaskResult(
                        "task-001",
                        "FAILED",
                        "invalid task",
                        TaskFailureReason.EXECUTION_ERROR
                );

        assertFalse(
                result.isRetryable()
        );
    }

    @Test
    void timeoutIsRetryable() {

        TaskResult result =
                new TaskResult(
                        "task-001",
                        "FAILED",
                        "timed out",
                        TaskFailureReason.TIMEOUT
                );

        assertTrue(
                result.isRetryable()
        );
    }

    @Test
    void unreachableNodeIsRetryable() {

        TaskResult result =
                new TaskResult(
                        "task-001",
                        "FAILED",
                        "connection refused",
                        TaskFailureReason.NODE_UNREACHABLE
                );

        assertTrue(
                result.isRetryable()
        );
    }

    @Test
    void transportErrorIsRetryable() {

        TaskResult result =
                new TaskResult(
                        "task-001",
                        "FAILED",
                        "connection reset",
                        TaskFailureReason.TRANSPORT_ERROR
                );

        assertTrue(
                result.isRetryable()
        );
    }

    @Test
    void offlineNodeIsRetryable() {

        TaskResult result =
                new TaskResult(
                        "task-001",
                        "FAILED",
                        "node offline",
                        TaskFailureReason.NODE_OFFLINE
                );

        assertTrue(
                result.isRetryable()
        );
    }
}
