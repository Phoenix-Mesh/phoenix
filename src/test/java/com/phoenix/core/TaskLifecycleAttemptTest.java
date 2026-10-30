// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.phoenix.model.TaskLifecycleState;

/**
 * Attempt identity and abandonment in the transport-neutral ledger.
 */
class TaskLifecycleAttemptTest {

    private final TaskLifecycleService lifecycle =
            new TaskLifecycleService();

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
    void attemptsAreNumberedPerTask() {

        assertEquals(1, accept("task-001", "n1").number());
        assertEquals(1, accept("task-002", "n1").number());

        lifecycle.failed(h("task-001"));

        assertEquals(2, accept("task-001", "n2").number());
        assertEquals(2, snap("task-001").attempt());
        assertEquals("n2", snap("task-001").nodeId());
    }

    @Test
    void anAttemptMustBeBoundToANode() {

        assertThrows(
                NullPointerException.class,
                () -> lifecycle.accepted("task-001", null, AttemptFencing.LEGACY));

        assertThrows(
                NullPointerException.class,
                () -> lifecycle.accepted(null, "n1", AttemptFencing.LEGACY));

        assertThrows(
                NullPointerException.class,
                () -> lifecycle.abandonAttemptsOn(null));

        assertTrue(lifecycle.snapshot("task-001").isEmpty());
    }

    @Test
    void abandonOnlyTouchesNonTerminalAttemptsOnThatNode() {

        accept("queued-on-n1", "n1");

        accept("delivered-on-n1", "n1");
        lifecycle.delivered(h("delivered-on-n1"));

        accept("done-on-n1", "n1");
        lifecycle.completed(h("done-on-n1"));

        accept("failed-on-n1", "n1");
        lifecycle.failed(h("failed-on-n1"));

        accept("queued-on-n2", "n2");

        List<TaskLifecycleService.Snapshot> abandoned =
                lifecycle.abandonAttemptsOn("n1");

        assertEquals(
                java.util.Set.of("queued-on-n1", "delivered-on-n1"),
                abandoned.stream()
                        .map(TaskLifecycleService.Snapshot::taskId)
                        .collect(java.util.stream.Collectors.toSet())
        );

        assertEquals(TaskLifecycleState.ABANDONED, snap("queued-on-n1").state());
        assertEquals(TaskLifecycleState.ABANDONED, snap("delivered-on-n1").state());
        assertEquals(TaskLifecycleState.COMPLETED, snap("done-on-n1").state());
        assertEquals(TaskLifecycleState.FAILED, snap("failed-on-n1").state());
        assertEquals(TaskLifecycleState.ACCEPTED, snap("queued-on-n2").state());
    }

    @Test
    void abandonedAttemptDoesNotClaimDeliveryItNeverHad() {

        accept("task-001", "n1");
        lifecycle.abandonAttemptsOn("n1");

        assertFalse(snap("task-001").reachedNode());
    }

    @Test
    void abandonedAttemptPreservesWhatWasObserved() {

        accept("task-001", "n1");
        lifecycle.delivered(h("task-001"));
        lifecycle.started(h("task-001"));
        lifecycle.abandonAttemptsOn("n1");

        TaskLifecycleService.Snapshot snapshot = snap("task-001");

        assertTrue(snapshot.reachedNode());
        assertTrue(snapshot.startAcknowledged());
    }

    @Test
    void abandonedIsTerminalSoLateReportsAreIgnored() {

        accept("task-001", "n1");
        lifecycle.abandonAttemptsOn("n1");

        lifecycle.delivered(h("task-001"));
        lifecycle.completed(h("task-001"));
        lifecycle.failed(h("task-001"));

        assertEquals(TaskLifecycleState.ABANDONED, snap("task-001").state());
    }

    @Test
    void abandonRunsHooksOnceAndFlagsTheAttemptFirst() {

        TaskLifecycleService.Attempt attempt =
                accept("task-001", "n1");

        AtomicInteger runs = new AtomicInteger();
        boolean[] flagSeenByHook = new boolean[1];

        attempt.onAbandon(() -> {
            runs.incrementAndGet();
            flagSeenByHook[0] = attempt.isAbandoned();
        });

        assertFalse(attempt.isAbandoned());

        lifecycle.abandonAttemptsOn("n1");
        lifecycle.abandonAttemptsOn("n1");

        assertEquals(1, runs.get());
        assertTrue(flagSeenByHook[0]);
        assertTrue(attempt.isAbandoned());
    }

    @Test
    void hookRegisteredAfterAbandonmentRunsImmediately() {

        TaskLifecycleService.Attempt attempt =
                accept("task-001", "n1");

        lifecycle.abandonAttemptsOn("n1");

        AtomicInteger runs = new AtomicInteger();
        attempt.onAbandon(runs::incrementAndGet);

        assertEquals(1, runs.get());
    }

    @Test
    void aFailingHookDoesNotPreventOtherHooks() {

        TaskLifecycleService.Attempt attempt =
                accept("task-001", "n1");

        AtomicInteger runs = new AtomicInteger();

        attempt.onAbandon(() -> {
            throw new IllegalStateException("boom");
        });
        attempt.onAbandon(runs::incrementAndGet);

        lifecycle.abandonAttemptsOn("n1");

        assertEquals(1, runs.get());
    }

    @Test
    void reportsFromAReplacedAttemptCannotCorruptTheNewOne() {

        TaskLifecycleService.Attempt first =
                accept("task-001", "n1");
        lifecycle.failed(first);

        TaskLifecycleService.Attempt second =
                accept("task-001", "n2");

        // A stale reporter still holding attempt 1:
        lifecycle.delivered(first);
        lifecycle.completed(first);
        lifecycle.failed(first);

        assertEquals(TaskLifecycleState.ACCEPTED, snap("task-001").state());
        assertEquals(2, snap("task-001").attempt());

        // The current attempt's reports still work.
        lifecycle.delivered(second);
        assertEquals(TaskLifecycleState.DELIVERED, snap("task-001").state());
    }

    @Test
    void acceptingAgainReleasesAnAttemptThatNeverFinished() {

        TaskLifecycleService.Attempt first =
                accept("task-001", "n1");

        AtomicInteger runs = new AtomicInteger();
        first.onAbandon(runs::incrementAndGet);

        accept("task-001", "n2");

        assertTrue(first.isAbandoned());
        assertEquals(1, runs.get());
    }

    @Test
    void currentAttemptReturnsTheLatestHandle() {

        accept("task-001", "n1");
        TaskLifecycleService.Attempt second =
                accept("task-001", "n2");

        assertSame(second, lifecycle.currentAttempt("task-001").orElseThrow());
        assertTrue(lifecycle.currentAttempt("ghost").isEmpty());
    }

    // ------------------------------------------------------------------
    // Issue #36: opaque, globally unique attempt ids
    // ------------------------------------------------------------------

    @Test
    void everyAttemptGetsADistinctOpaqueId() {

        TaskLifecycleService.Attempt a = accept("task-001", "n1");
        TaskLifecycleService.Attempt b = accept("task-001", "n1");
        TaskLifecycleService.Attempt c = accept("task-002", "n1");

        assertNotEquals(a.id(), b.id());
        assertNotEquals(a.id(), c.id());
        assertNotEquals(b.id(), c.id());
    }

    @Test
    void theSnapshotExposesTheCurrentAttemptId() {

        accept("task-001", "n1");
        TaskLifecycleService.Attempt second = accept("task-001", "n2");

        assertEquals(second.id(), snap("task-001").attemptId());
        assertEquals(2, snap("task-001").attempt());
    }

    @Test
    void twoCoreRunsReusingTheSameTaskIdNeverShareAnAttemptId() {

        // A restarted Core numbers tasks from task-001 again and every task
        // starts at attempt 1; the id is the only thing that tells the runs
        // apart.
        String before = new TaskLifecycleService()
                .accepted("task-001", "n1", AttemptFencing.LEGACY).id();
        String after = new TaskLifecycleService()
                .accepted("task-001", "n1", AttemptFencing.LEGACY).id();

        assertNotEquals(before, after);
    }

    @Test
    void anInjectedSupplierDecidesTheIds() {

        java.util.concurrent.atomic.AtomicInteger counter =
                new java.util.concurrent.atomic.AtomicInteger();

        TaskLifecycleService deterministic =
                new TaskLifecycleService(
                        new com.phoenix.testsupport.MutableClock(),
                        () -> "id-" + counter.incrementAndGet());

        assertEquals("id-1", deterministic.accepted("t", "n1", AttemptFencing.LEGACY).id());
        assertEquals("id-2", deterministic.accepted("t", "n1", AttemptFencing.LEGACY).id());
    }

    @Test
    void shortIdIsAPrefixForLogsOnly() {

        TaskLifecycleService deterministic =
                new TaskLifecycleService(
                        new com.phoenix.testsupport.MutableClock(),
                        () -> "0123456789abcdef");

        TaskLifecycleService.Attempt attempt =
                deterministic.accepted("t", "n1", AttemptFencing.LEGACY);

        assertEquals("01234567", attempt.shortId());
        assertEquals("0123456789abcdef", attempt.id());
    }

    @Test
    void anAttemptIdSurvivesPruningAsANewUniqueId() {

        String first = accept("task-001", "n1").id();
        lifecycle.failed(h("task-001"));

        // Push the ledger past its prune threshold with finished tasks.
        for (int i = 0; i < 4200; i++) {
            TaskLifecycleService.Attempt filler =
                    lifecycle.accepted("filler-" + i, "n1", AttemptFencing.LEGACY);
            lifecycle.failed(filler);
        }

        TaskLifecycleService.Attempt again = accept("task-001", "n1");

        assertNotEquals(first, again.id());
    }

    @Test
    void aSupplierThatReturnsNullIsRejectedAtAccept() {

        TaskLifecycleService broken =
                new TaskLifecycleService(
                        new com.phoenix.testsupport.MutableClock(),
                        () -> null);

        assertThrows(
                NullPointerException.class,
                () -> broken.accepted("t", "n1", AttemptFencing.LEGACY));
    }
}
