// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * Issue #28: retry and node-health semantics for every failure reason.
 *
 * <p>The explicit sets double as a guard: adding a new TaskFailureReason
 * without classifying it here fails the coverage test.
 */
class TaskResultSemanticsTest {

    private static final Set<TaskFailureReason> RETRYABLE =
            EnumSet.of(
                    TaskFailureReason.TIMEOUT,
                    TaskFailureReason.DELIVERY_TIMEOUT,
                    TaskFailureReason.EXECUTION_TIMEOUT,
                    TaskFailureReason.NODE_UNREACHABLE,
                    TaskFailureReason.TRANSPORT_ERROR,
                    TaskFailureReason.TRANSPORT_UNAVAILABLE,
                    TaskFailureReason.NODE_OFFLINE
            );

    private static final Set<TaskFailureReason> NOT_RETRYABLE =
            EnumSet.of(
                    TaskFailureReason.NONE,
                    TaskFailureReason.EXECUTION_ERROR,
                    TaskFailureReason.NO_ELIGIBLE_NODE
            );

    /**
     * Retryable failures that are evidence against the node.
     *
     * <p>NODE_OFFLINE is deliberately absent: it is derived from node
     * health (the node was already declared OFFLINE), so it must not be fed
     * back into node health. TRANSPORT_UNAVAILABLE is absent because it is a
     * failure of Core's own transport, not of the node.
     */
    private static final Set<TaskFailureReason> IMPLICATE_NODE =
            EnumSet.of(
                    TaskFailureReason.TIMEOUT,
                    TaskFailureReason.EXECUTION_TIMEOUT,
                    TaskFailureReason.NODE_UNREACHABLE,
                    TaskFailureReason.TRANSPORT_ERROR
            );

    private static TaskResult resultWith(TaskFailureReason reason) {
        return new TaskResult("task-001", "FAILED", "x", reason);
    }

    @Test
    void everyFailureReasonIsClassifiedForRetry() {

        Set<TaskFailureReason> covered = EnumSet.copyOf(RETRYABLE);
        covered.addAll(NOT_RETRYABLE);

        assertEquals(
                EnumSet.allOf(TaskFailureReason.class),
                covered
        );
    }

    @Test
    void retryabilityMatchesTheClassification() {

        for (TaskFailureReason reason : RETRYABLE) {
            assertTrue(
                    resultWith(reason).isRetryable(),
                    reason + " should be retryable"
            );
        }

        for (TaskFailureReason reason : NOT_RETRYABLE) {
            assertFalse(
                    resultWith(reason).isRetryable(),
                    reason + " should not be retryable"
            );
        }
    }

    @Test
    void deliveryTimeoutIsRetryableButDoesNotImplicateTheNode() {

        TaskResult result =
                resultWith(TaskFailureReason.DELIVERY_TIMEOUT);

        assertTrue(result.isRetryable());
        assertFalse(result.implicatesNodeHealth());
    }

    @Test
    void nodeOfflineIsRetryableButDoesNotImplicateTheNode() {

        TaskResult result =
                resultWith(TaskFailureReason.NODE_OFFLINE);

        assertTrue(result.isRetryable());
        assertFalse(result.implicatesNodeHealth());
    }

    @Test
    void transportUnavailableIsRetryableButDoesNotImplicateTheNode() {

        TaskResult result =
                resultWith(TaskFailureReason.TRANSPORT_UNAVAILABLE);

        assertTrue(result.isRetryable());
        assertFalse(result.implicatesNodeHealth());
    }

    @Test
    void everyOtherRetryableFailureStillImplicatesTheNode() {

        for (TaskFailureReason reason : RETRYABLE) {

            if (reason == TaskFailureReason.DELIVERY_TIMEOUT
                    || reason == TaskFailureReason.NODE_OFFLINE
                    || reason == TaskFailureReason.TRANSPORT_UNAVAILABLE) {
                continue;
            }

            assertTrue(
                    resultWith(reason).implicatesNodeHealth(),
                    reason + " must keep degrading the node (#28 must "
                            + "not weaken unresponsive-node detection)"
            );
        }
    }

    @Test
    void nodeHealthSetIsExactlyTheDocumentedOne() {

        for (TaskFailureReason reason : TaskFailureReason.values()) {
            assertEquals(
                    IMPLICATE_NODE.contains(reason),
                    resultWith(reason).implicatesNodeHealth(),
                    "unexpected node-health semantics for " + reason
            );
        }
    }

    @Test
    void backwardCompatibleConstructorStillClassifiesSuccessAndFailure() {

        assertEquals(
                TaskFailureReason.NONE,
                new TaskResult("t", "SUCCESS", "ok").failureReason()
        );

        assertEquals(
                TaskFailureReason.EXECUTION_ERROR,
                new TaskResult("t", "FAILED", "boom").failureReason()
        );
    }
}
