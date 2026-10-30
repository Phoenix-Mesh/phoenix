// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.phoenix.adapter.IHardwareAdapter;
import com.phoenix.core.AdapterRegistry;
import com.phoenix.core.DeviceRegistry;
import com.phoenix.core.PlacementReservationService;
import com.phoenix.model.HardwareClass;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.ResourceStatus;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;
import com.phoenix.recovery.TaskRecoveryManager;
import com.phoenix.scheduler.GreedyCostScheduler;
import com.phoenix.scheduler.WeightedPlacementCostModel;
import com.phoenix.tui.TaskJournal;

/**
 * Regression tests for the #33 dispatcher recovery boundary.
 *
 * <p>A placement cycle may exhaust every eligible node with retryable
 * failures. That does not make the logical task terminal: the task must
 * return to the common pending path, where the recovery budget bounds future
 * cycles.
 */
public class TaskDispatcherExhaustedAttemptsTest {

    private final List<String> attempted = new ArrayList<>();

    private IHardwareAdapter alwaysFails(TaskFailureReason reason) {
        return new IHardwareAdapter() {
            @Override
            public TaskResult executeTask(
                    TaskDescriptor task, NodeExecutionTarget target) {

                attempted.add(target.nodeId());

                return new TaskResult(
                        task.id(),
                        "FAILED",
                        "scripted",
                        reason
                );
            }

            @Override
            public void reportResult(TaskResult result) {
            }
        };
    }

    private TaskDispatcher dispatcher(
            DeviceRegistry registry,
            PendingTaskQueue pending,
            TaskJournal journal,
            PlacementReservationService[] reservationsOut,
            IHardwareAdapter adapter) {

        GreedyCostScheduler scheduler =
                new GreedyCostScheduler(
                        new WeightedPlacementCostModel()
                );

        PlacementReservationService reservations =
                new PlacementReservationService(
                        registry,
                        scheduler
                );

        reservationsOut[0] = reservations;

        return new TaskDispatcher(
                registry,
                new AdapterRegistry(
                        Map.of("stub", adapter)
                ),
                journal,
                new TaskRecoveryManager(),
                pending,
                reservations
        );
    }

    private DeviceRegistry threeNodes() {

        DeviceRegistry registry = new DeviceRegistry();

        for (String id : List.of("n1", "n2", "n3")) {

            registry.register(
                    new NodeInfo(
                            id,
                            "node-" + id,
                            HardwareClass.LINUX,
                            "x86_64",
                            2,
                            4096,
                            "stub",
                            Set.of()
                    ),
                    ResourceStatus.of(
                            10.0,
                            4096,
                            0,
                            0,
                            1.0,
                            1.0
                    )
            );
        }

        return registry;
    }

    @Test
    void exhaustingEveryAttemptRetryablyReturnsTheTaskToWaiting() {

        DeviceRegistry registry = threeNodes();

        PendingTaskQueue pending =
                new PendingTaskQueue();

        TaskJournal journal =
                new TaskJournal();

        PlacementReservationService[] reservations =
                new PlacementReservationService[1];

        TaskDispatcher dispatcher =
                dispatcher(
                        registry,
                        pending,
                        journal,
                        reservations,
                        alwaysFails(
                                TaskFailureReason.NODE_OFFLINE
                        )
                );

        TaskDescriptor task =
                new TaskDescriptor(
                        "task-001",
                        "Monte-Carlo-Pi",
                        1,
                        128
                );

        journal.submitted(task);

        TaskDispatcher.Outcome outcome =
                dispatcher.replace(task);

        assertInstanceOf(
                TaskDispatcher.Outcome.Waiting.class,
                outcome
        );

        assertEquals(
                3,
                attempted.size(),
                "all three placement attempts were used"
        );

        /*
         * The exact node ordering is a scheduler detail. What matters here
         * is that the complete eligible set was consumed during this cycle.
         */
        assertEquals(
                3,
                Set.copyOf(attempted).size(),
                "each eligible node was attempted once in this cycle"
        );

        PendingTaskQueue.Pending waiting =
                pending.get("task-001");

        assertEquals(
                1,
                pending.size()
        );

        assertEquals(
                1,
                waiting.failedCycles()
        );

        assertEquals(
                TaskFailureReason.NODE_OFFLINE,
                waiting.lastFailure()
        );

        assertTrue(
                !pending.isExpired("task-001")
        );

        assertEquals(
                "WAITING",
                journal.recent(5).get(0).status()
        );

        for (String id : List.of("n1", "n2", "n3")) {

            assertEquals(
                    0,
                    reservations[0].reservationCount(id)
            );
        }
    }

    @Test
    void aTaskThatKeepsExhaustingAttemptsIsBoundedByTheRecoveryBudget() {

        DeviceRegistry registry = threeNodes();

        PendingTaskQueue pending =
                new PendingTaskQueue(
                        java.time.Clock.systemUTC(),
                        java.time.Duration.ofSeconds(60),
                        2
                );

        TaskJournal journal =
                new TaskJournal();

        PlacementReservationService[] reservations =
                new PlacementReservationService[1];

        TaskDispatcher dispatcher =
                dispatcher(
                        registry,
                        pending,
                        journal,
                        reservations,
                        alwaysFails(
                                TaskFailureReason.NODE_OFFLINE
                        )
                );

        TaskDescriptor task =
                new TaskDescriptor(
                        "task-001",
                        "Monte-Carlo-Pi",
                        1,
                        128
                );

        journal.submitted(task);

        /*
         * Recovery cycle #1.
         */
        TaskDispatcher.Outcome first =
                dispatcher.replace(task);

        assertInstanceOf(
                TaskDispatcher.Outcome.Waiting.class,
                first
        );

        assertEquals(
                1,
                pending.get("task-001").failedCycles()
        );

        assertTrue(
                !pending.isExhausted(
                        pending.get("task-001")
                )
        );

        /*
         * The admission layer claims the task before starting another
         * placement cycle. Reproduce that boundary here.
         */
        pending.claim("task-001");

        /*
         * Recovery cycle #2.
         *
         * Even though this reaches the configured recovery budget, the
         * dispatcher itself still returns WAITING. TaskAdmission is
         * responsible for deciding what happens once the budget is exhausted.
         */
        TaskDispatcher.Outcome second =
                dispatcher.replace(task);

        assertInstanceOf(
                TaskDispatcher.Outcome.Waiting.class,
                second,
                "exhausting another retryable cycle is still recoverable "
                        + "at the dispatcher boundary"
        );

        PendingTaskQueue.Pending waiting =
                pending.get("task-001");

        assertEquals(
                2,
                waiting.failedCycles()
        );

        assertEquals(
                TaskFailureReason.NODE_OFFLINE,
                waiting.lastFailure()
        );

        assertTrue(
                pending.isExhausted(waiting),
                "two exhausted cycles use up a budget of two"
        );

        assertEquals(
                6,
                attempted.size(),
                "two cycles each attempted all three eligible nodes"
        );

        assertEquals(
                "WAITING",
                journal.recent(5).get(0).status()
        );

        /*
         * No reservation may leak across either failed recovery cycle.
         */
        for (String id : List.of("n1", "n2", "n3")) {

            assertEquals(
                    0,
                    reservations[0].reservationCount(id)
            );
        }
    }
}
