// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.phoenix.core.AttemptFencing;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.model.TaskLifecycleState;

/** Correlation of node reports with pending attempts, at the edges. */
class MqttAttemptsTest {

    private static final String DEVICE = "dev-1";
    private static final String NODE = "node-1";

    private final TaskLifecycleService lifecycle = new TaskLifecycleService();
    private final MqttAttempts attempts = new MqttAttempts(lifecycle);

    @Test
    void aResultThatLosesTheRaceWithAbandonmentIsNotHandedToTheWaiter() {

        TaskLifecycleService.Attempt attemptA =
                lifecycle.accepted("task-a", NODE, AttemptFencing.ENFORCED);
        TaskLifecycleService.Attempt attemptB =
                lifecycle.accepted("task-b", NODE, AttemptFencing.ENFORCED);

        MqttAttempts.Pending pendingA = attempts.track(DEVICE, attemptA);
        MqttAttempts.Pending pendingB = attempts.track(DEVICE, attemptB);

        // Abandonment records every attempt in the ledger first and only then
        // marks them one by one. While the first is being marked, the second
        // is already ABANDONED in the ledger but does not know it yet; a
        // result for it arrives exactly then. Whichever is marked first, the
        // other one is in that window.
        AtomicReference<Boolean> acceptedA = new AtomicReference<>();
        AtomicReference<Boolean> acceptedB = new AtomicReference<>();

        attemptA.onAbandon(() -> acceptedB.set(attempts.resulted(DEVICE, success(attemptB))));
        attemptB.onAbandon(() -> acceptedA.set(attempts.resulted(DEVICE, success(attemptA))));

        lifecycle.abandonAttemptsOn(NODE);

        // Exactly one of the two results hit the window, and it was refused.
        assertTrue(Boolean.FALSE.equals(acceptedA.get()) || Boolean.FALSE.equals(acceptedB.get()));
        assertFalse(Boolean.TRUE.equals(acceptedA.get()));
        assertFalse(Boolean.TRUE.equals(acceptedB.get()));

        assertTrue(pendingA.result.isCompletedExceptionally(),
                "task-a must end abandoned, not with a result");
        assertTrue(pendingB.result.isCompletedExceptionally(),
                "task-b must end abandoned, not with a result");

        assertEquals(TaskLifecycleState.ABANDONED, lifecycle.snapshot("task-a").orElseThrow().state());
        assertEquals(TaskLifecycleState.ABANDONED, lifecycle.snapshot("task-b").orElseThrow().state());
    }

    @Test
    void aResultTheLedgerAcceptsReachesTheWaiterAfterTheLedgerHasIt() {

        TaskLifecycleService.Attempt attempt =
                lifecycle.accepted("task-a", NODE, AttemptFencing.ENFORCED);
        MqttAttempts.Pending pending = attempts.track(DEVICE, attempt);

        assertTrue(attempts.resulted(DEVICE, success(attempt)));

        assertTrue(pending.result.isDone() && !pending.result.isCompletedExceptionally());
        assertEquals(TaskLifecycleState.COMPLETED, lifecycle.snapshot("task-a").orElseThrow().state());
    }

    @Test
    void aDuplicateResultChangesNothing() {

        TaskLifecycleService.Attempt attempt =
                lifecycle.accepted("task-a", NODE, AttemptFencing.ENFORCED);
        attempts.track(DEVICE, attempt);

        assertTrue(attempts.resulted(DEVICE, success(attempt)));
        assertFalse(attempts.resulted(DEVICE, success(attempt)));
        assertEquals(TaskLifecycleState.COMPLETED, lifecycle.snapshot("task-a").orElseThrow().state());
    }

    private static MqttWire.Result success(TaskLifecycleService.Attempt attempt) {
        MqttWire.Result result = new MqttWire.Result();
        result.taskId = attempt.taskId();
        result.attemptId = attempt.id();
        result.status = "SUCCESS";
        result.outputData = "ok";
        return result;
    }
}
