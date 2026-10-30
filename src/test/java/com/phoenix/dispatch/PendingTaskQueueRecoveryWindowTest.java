// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.testsupport.MutableClock;

/**
 * The waiting window must mean "time without an eligible node".
 */
class PendingTaskQueueRecoveryWindowTest {

    private final MutableClock clock = new MutableClock();
    private final PendingTaskQueue queue =
            new PendingTaskQueue(clock, Duration.ofSeconds(60), 3);

    private static TaskDescriptor task(String id) {
        return new TaskDescriptor(id, "Monte-Carlo-Pi", 1, 128);
    }

    private PendingTaskQueue.Pending live(String id) {
        return queue.get(id);
    }

    @Test
    void failedAttemptStartsAFreshWindowAndKeepsItsPlaceInLine() {

        queue.enqueue(task("task-001"));
        queue.enqueue(task("task-002"));

        clock.advance(Duration.ofSeconds(50));
        queue.claim("task-001");              // attempt runs ...
        clock.advance(Duration.ofSeconds(50));// ... for 50s, then fails

        // The snapshot copy taken before the attempt is stale:
        assertTrue(queue.isExpired(live("task-001")));

        queue.enqueue(task("task-001"), TaskFailureReason.NODE_OFFLINE);

        assertFalse(queue.isExpired("task-001"));
        assertEquals(1, live("task-001").failedCycles());
        assertEquals(TaskFailureReason.NODE_OFFLINE,
                live("task-001").lastFailure());
        assertFalse(live("task-001").inFlight());
        assertEquals(
                List.of("task-001", "task-002"),
                queue.snapshot().stream().map(p -> p.task().id()).toList());
    }

    @Test
    void windowStillExpiresAfterTheFreshWindowRunsOut() {

        queue.enqueue(task("task-001"));
        queue.claim("task-001");
        queue.enqueue(task("task-001"), TaskFailureReason.NODE_OFFLINE);

        clock.advance(Duration.ofSeconds(61));

        assertTrue(queue.isExpired("task-001"));
    }

    @Test
    void plainReEvaluationNeverResetsTheWindow() {

        queue.enqueue(task("task-001"));

        clock.advance(Duration.ofSeconds(40));

        // The recovery tick finds no eligible node, again and again.
        for (int i = 0; i < 5; i++) {
            queue.claim("task-001");
            queue.enqueue(task("task-001"));
            clock.advance(Duration.ofSeconds(5));
        }

        assertTrue(queue.isExpired("task-001"),
                "an unplaceable task must still expire");
        assertEquals(0, live("task-001").failedCycles());
    }

    @Test
    void claimAndReleaseAreReflectedWithoutMovingTheTask() {

        queue.enqueue(task("task-001"));
        queue.enqueue(task("task-002"));

        assertTrue(queue.claim("task-001"));
        assertTrue(live("task-001").inFlight());

        queue.release("task-001");

        assertFalse(live("task-001").inFlight());
        assertEquals("task-001", queue.peek().task().id());
    }

    @Test
    void aPlainReEvaluationEndsTheClaimWithoutTouchingTheWindow() {

        queue.enqueue(task("task-001"));
        Object windowStart = live("task-001").waitingSince();

        queue.claim("task-001");
        assertTrue(live("task-001").inFlight());

        queue.enqueue(task("task-001"));      // evaluated, no node eligible

        assertFalse(live("task-001").inFlight());
        assertEquals(windowStart, live("task-001").waitingSince());
    }

    @Test
    void claimingAnUnknownTaskFails() {
        assertFalse(queue.claim("ghost"));
        assertFalse(queue.isExpired("ghost"));
        assertNull(queue.get("ghost"));
    }

    @Test
    void recoveryBudgetIsExhaustedAfterTheConfiguredCycles() {

        queue.enqueue(task("task-001"));

        for (int i = 1; i <= 3; i++) {
            assertFalse(queue.isExhausted(live("task-001")),
                    "cycle " + i + " should still have budget");
            queue.claim("task-001");
            queue.enqueue(task("task-001"), TaskFailureReason.NODE_OFFLINE);
        }

        assertTrue(queue.isExhausted(live("task-001")));
    }

    @Test
    void aFreshTaskThatEntersWaitingAfterAFailureCountsThatFailure() {

        queue.enqueue(task("task-001"), TaskFailureReason.NODE_UNREACHABLE);

        assertEquals(1, live("task-001").failedCycles());
        assertEquals(TaskFailureReason.NODE_UNREACHABLE,
                live("task-001").lastFailure());
    }

    @Test
    void duplicateEnqueueStillDoesNotChangePosition() {

        queue.enqueue(task("task-001"));
        queue.enqueue(task("task-002"));
        queue.enqueue(task("task-001"));

        assertEquals(
                List.of("task-001", "task-002"),
                queue.snapshot().stream().map(p -> p.task().id()).toList());
    }
}
