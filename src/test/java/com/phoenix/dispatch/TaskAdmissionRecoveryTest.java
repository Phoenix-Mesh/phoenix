// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.phoenix.adapter.IHardwareAdapter;
import com.phoenix.core.AdapterRegistry;
import com.phoenix.core.DeviceRegistry;
import com.phoenix.core.PlacementReservationService;
import com.phoenix.model.HardwareClass;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.NodeStatus;
import com.phoenix.model.ResourceStatus;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;
import com.phoenix.recovery.TaskRecoveryManager;
import com.phoenix.scheduler.GreedyCostScheduler;
import com.phoenix.scheduler.WeightedPlacementCostModel;
import com.phoenix.testsupport.MutableClock;
import com.phoenix.tui.TaskJournal;

/**
 * Regression tests for the recovery window, built from the traces in
 * the issue: a task that had just failed on a node was "WAITING" and then
 * "gave up" in the same second; and one long attempt made other pending
 * tasks "expire" without ever being evaluated.
 *
 * <p>Time is a {@link MutableClock}. An adapter that "takes a long time"
 * simply advances it, so the tests are deterministic apart from the 2 s
 * recovery tick.
 */
class TaskAdmissionRecoveryTest {

    private final MutableClock clock = new MutableClock();

    private static final class Fixture {
        final DeviceRegistry registry = new DeviceRegistry();
        final TaskJournal journal = new TaskJournal();
        final PendingTaskQueue pending;
        final TaskAdmission admission;
        final PlacementReservationService reservations;

        Fixture(
                MutableClock clock,
                int maxFailedCycles,
                Map<String, IHardwareAdapter> adapters) {

            pending = new PendingTaskQueue(
                    clock, Duration.ofSeconds(60), maxFailedCycles);

            GreedyCostScheduler scheduler =
                    new GreedyCostScheduler(new WeightedPlacementCostModel());
            reservations =
                    new PlacementReservationService(registry, scheduler);

            admission = new TaskAdmission(
                    pending,
                    new TaskDispatcher(
                            registry,
                            new AdapterRegistry(adapters),
                            journal,
                            new TaskRecoveryManager(),
                            pending,
                            reservations),
                    journal);
        }

        void addNode(String id, String adapter, double cpu) {
            registry.register(
                    new NodeInfo(id, "node-" + id, HardwareClass.LINUX,
                            "x86_64", 2, 4096, adapter, Set.of()),
                    ResourceStatus.of(cpu, 4096, 0, 0, 1.0, 1.0));
        }

        void pend(String id, int cpu) {
            TaskDescriptor task =
                    new TaskDescriptor(id, "Monte-Carlo-Pi", cpu, 128);
            journal.submitted(task);
            pending.enqueue(task);
        }

        String status(String id) {
            return journal.recent(20).stream()
                    .filter(t -> t.taskId().equals(id))
                    .findFirst().orElseThrow().status();
        }

        boolean await(String id, String status, long millis)
                throws InterruptedException {
            long deadline = System.currentTimeMillis() + millis;
            while (System.currentTimeMillis() < deadline) {
                if (status(id).equals(status)) {
                    return true;
                }
                Thread.sleep(50);
            }
            return status(id).equals(status);
        }
    }

    private static IHardwareAdapter adapter(
            Function<TaskDescriptor, TaskResult> behaviour) {

        return new IHardwareAdapter() {
            @Override
            public TaskResult executeTask(
                    TaskDescriptor task, NodeExecutionTarget target) {
                return behaviour.apply(task);
            }

            @Override
            public void reportResult(TaskResult result) {
            }
        };
    }

    private static TaskResult offline(TaskDescriptor task) {
        return new TaskResult(task.id(), "FAILED", "node went offline",
                TaskFailureReason.NODE_OFFLINE);
    }

    @Test
    void aTaskWhoseAttemptJustFailedIsRetriedNotGivenUp() throws Exception {

        AtomicInteger calls = new AtomicInteger();

        Fixture f = new Fixture(clock, 3, Map.of("stub", adapter(task -> {
            if (calls.incrementAndGet() == 1) {
                // The first attempt spends longer than the whole waiting
                // window at the node (like the 60 s delivery wait) ...
                clock.advance(Duration.ofSeconds(61));
                return offline(task);          // ... then the node is OFFLINE
            }
            return new TaskResult(task.id(), "SUCCESS", "ok");
        })));

        f.addNode("A", "stub", 10.0);
        f.pend("task-006", 1);
        f.admission.start();

        try {
            assertTrue(f.await("task-006", "SUCCESS", 10_000),
                    "the task must be recovered, not given up; status="
                            + f.status("task-006"));
            assertEquals(2, calls.get());
            assertEquals(0, f.pending.size());
            // NODE_OFFLINE must not degrade a node that is already known.
            assertEquals(NodeStatus.ONLINE,
                    f.registry.get("A").orElseThrow().state());
        } finally {
            f.admission.stop();
        }
    }

    @Test
    void aLongAttemptDoesNotMakeOtherPendingTasksExpireUnevaluated()
            throws Exception {

        Fixture f = new Fixture(clock, 3, Map.of(
                "slow", adapter(task -> {
                    clock.advance(Duration.ofSeconds(61));   // long attempt
                    return new TaskResult(task.id(), "SUCCESS", "slow-ok");
                }),
                "fast", adapter(task ->
                        new TaskResult(task.id(), "SUCCESS", "fast-ok"))));

        f.addNode("A", "slow", 5.0);
        f.addNode("B", "fast", 80.0);

        f.pend("task-1", 1);
        f.pend("task-2", 1);
        f.admission.start();

        try {
            assertTrue(f.await("task-1", "SUCCESS", 10_000));
            assertTrue(f.await("task-2", "SUCCESS", 10_000),
                    "task-2 was eligible the whole time; status="
                            + f.status("task-2"));
        } finally {
            f.admission.stop();
        }
    }

    @Test
    void aTaskThatReallyHasNoEligibleNodeStillExpires() throws Exception {

        Fixture f = new Fixture(clock, 3,
                Map.of("stub", adapter(task ->
                        new TaskResult(task.id(), "SUCCESS", "ok"))));

        f.addNode("A", "stub", 10.0);
        f.pend("task-stuck", 999);          // needs more CPU than any node has

        clock.advance(Duration.ofSeconds(61));
        f.admission.start();

        try {
            assertTrue(f.await("task-stuck", "FAILED", 8_000));
            assertEquals(0, f.pending.size());
            assertEquals("NO_ELIGIBLE_NODE",
                    f.journal.recent(5).get(0).failureReason());
        } finally {
            f.admission.stop();
        }
    }

    @Test
    void aTaskThatKeepsFailingIsEventuallyGivenUp() throws Exception {

        AtomicInteger calls = new AtomicInteger();

        Fixture f = new Fixture(clock, 2, Map.of("stub", adapter(task -> {
            calls.incrementAndGet();
            return offline(task);
        })));

        f.addNode("A", "stub", 10.0);
        f.pend("task-flaky", 1);
        f.admission.start();

        try {
            assertTrue(f.await("task-flaky", "FAILED", 10_000));
            assertEquals(2, calls.get(),
                    "exactly the configured recovery budget");
            assertEquals(0, f.pending.size());
            assertEquals("NODE_OFFLINE",
                    f.journal.recent(5).get(0).failureReason());
        } finally {
            f.admission.stop();
        }
    }

    @Test
    void anUnplaceableTaskReEvaluatedEveryTickKeepsItsOriginalWindow()
            throws Exception {

        Fixture f = new Fixture(clock, 3,
                Map.of("stub", adapter(task ->
                        new TaskResult(task.id(), "SUCCESS", "ok"))));

        f.addNode("A", "stub", 10.0);
        f.pend("task-stuck", 999);
        Object windowStart = f.pending.get("task-stuck").waitingSince();

        f.admission.start();

        try {
            Thread.sleep(4_500);             // two or more recovery ticks

            assertEquals(1, f.pending.size());
            assertEquals(windowStart,
                    f.pending.get("task-stuck").waitingSince(),
                    "re-evaluation must not reset the waiting window");
            assertEquals(0, f.pending.get("task-stuck").failedCycles());
        } finally {
            f.admission.stop();
        }
    }
}
