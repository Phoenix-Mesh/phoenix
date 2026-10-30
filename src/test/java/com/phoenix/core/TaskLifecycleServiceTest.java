// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.phoenix.model.TaskLifecycleState;
import com.phoenix.testsupport.MutableClock;

class TaskLifecycleServiceTest {

    private final MutableClock clock = new MutableClock();
    private final TaskLifecycleService lifecycle =
            new TaskLifecycleService(clock);

    private final java.util.Map<String, TaskLifecycleService.Attempt> handles =
            new java.util.HashMap<>();

    private TaskLifecycleService.Attempt accept(String taskId, String nodeId) {
        TaskLifecycleService.Attempt attempt =
                lifecycle.accepted(taskId, nodeId, AttemptFencing.LEGACY);
        handles.put(taskId, attempt);
        return attempt;
    }

    private TaskLifecycleService.Attempt h(String taskId) {
        return handles.get(taskId);
    }

    private TaskLifecycleService.Snapshot snap(String id) {
        return lifecycle.snapshot(id).orElseThrow();
    }

    @Test
    void unknownTaskHasNoSnapshot() {
        assertTrue(lifecycle.snapshot("nope").isEmpty());
    }

    @Test
    void acceptedTaskHasNotReachedTheNode() {

        accept("task-001", "n1");

        TaskLifecycleService.Snapshot snapshot = snap("task-001");

        assertEquals(TaskLifecycleState.ACCEPTED, snapshot.state());
        assertFalse(snapshot.reachedNode());
        assertFalse(snapshot.startAcknowledged());
        assertNotNull(snapshot.acceptedAt());
    }

    @Test
    void walksTheFullLifecycleWithTimestamps() {

        accept("task-001", "n1");

        clock.advance(Duration.ofSeconds(3));
        lifecycle.delivered(h("task-001"));

        clock.advance(Duration.ofSeconds(1));
        lifecycle.started(h("task-001"));

        clock.advance(Duration.ofSeconds(2));
        lifecycle.completed(h("task-001"));

        TaskLifecycleService.Snapshot snapshot = snap("task-001");

        assertEquals(TaskLifecycleState.COMPLETED, snapshot.state());
        assertTrue(snapshot.reachedNode());
        assertTrue(snapshot.startAcknowledged());
        assertEquals(
                Duration.ofSeconds(3),
                Duration.between(
                        snapshot.acceptedAt(),
                        snapshot.deliveredAt())
        );
        assertEquals(
                Duration.ofSeconds(6),
                Duration.between(
                        snapshot.acceptedAt(),
                        snapshot.finishedAt())
        );
    }

    @Test
    void startedImpliesDelivered() {

        accept("task-001", "n1");
        lifecycle.started(h("task-001"));

        TaskLifecycleService.Snapshot snapshot = snap("task-001");

        assertEquals(TaskLifecycleState.STARTED, snapshot.state());
        assertTrue(snapshot.reachedNode());
    }

    @Test
    void completedWithoutAnyAckStillImpliesDelivered() {

        // Push transport: no separate delivered/started signal may arrive.
        accept("task-001", "n1");
        lifecycle.completed(h("task-001"));

        assertTrue(snap("task-001").reachedNode());
        assertFalse(snap("task-001").startAcknowledged());
    }

    @Test
    void failedBeforeDeliveryDoesNotClaimDelivery() {

        accept("task-001", "n1");
        lifecycle.failed(h("task-001"));

        TaskLifecycleService.Snapshot snapshot = snap("task-001");

        assertEquals(TaskLifecycleState.FAILED, snapshot.state());
        assertFalse(snapshot.reachedNode());
        assertNull(snapshot.deliveredAt());
    }

    @Test
    void transitionsNeverMoveBackwards() {

        accept("task-001", "n1");
        lifecycle.started(h("task-001"));
        lifecycle.delivered(h("task-001"));   // late duplicate
        accept("task-002", "n1");    // unrelated task

        assertEquals(
                TaskLifecycleState.STARTED,
                snap("task-001").state()
        );
    }

    @Test
    void terminalStateIsFinal() {

        accept("task-001", "n1");
        lifecycle.failed(h("task-001"));
        lifecycle.completed(h("task-001"));   // late result must not resurrect it

        assertEquals(
                TaskLifecycleState.FAILED,
                snap("task-001").state()
        );
    }

    @Test
    void reportsFromAForgottenAttemptAreIgnored() {

        TaskLifecycleService.Attempt attempt = accept("ghost", "n1");
        lifecycle.forget("ghost");

        lifecycle.delivered(attempt);
        lifecycle.completed(attempt);

        assertTrue(lifecycle.snapshot("ghost").isEmpty());
    }

    @Test
    void acceptingAgainStartsAFreshAttempt() {

        accept("task-001", "n1");
        lifecycle.delivered(h("task-001"));
        lifecycle.failed(h("task-001"));

        // Recovery re-places the same task.
        accept("task-001", "n1");

        TaskLifecycleService.Snapshot snapshot = snap("task-001");

        assertEquals(TaskLifecycleState.ACCEPTED, snapshot.state());
        assertFalse(snapshot.reachedNode());
    }

    @Test
    void forgetRemovesTheTask() {

        accept("task-001", "n1");
        lifecycle.forget("task-001");

        assertTrue(lifecycle.snapshot("task-001").isEmpty());
    }
}
