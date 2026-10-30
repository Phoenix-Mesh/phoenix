// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import com.phoenix.model.TaskResult;
import com.phoenix.recovery.TaskRecoveryManager;
import com.phoenix.scheduler.GreedyCostScheduler;
import com.phoenix.scheduler.WeightedPlacementCostModel;
import com.phoenix.tui.TaskJournal;

/**
 * Verifies Phoenix's FIFO-preferred, non-blocking recovery policy.
 *
 * Older pending tasks are considered first, but an older task that is
 * currently ineligible must not block younger tasks that are independently
 * eligible.
 *
 * Example:
 *
 *   T1 -> ineligible
 *   T2 -> eligible
 *   T3 -> eligible
 *
 * Expected:
 *
 *   T1 remains pending
 *   T2 completes
 *   T3 completes
 */
class PendingTaskFifoPolicyTest {

    @Test
    void ineligibleOlderTaskDoesNotBlockEligibleYoungerTasks() {

        DeviceRegistry registry = new DeviceRegistry();

        registry.register(
                new NodeInfo(
                        "n1",
                        "stub-node",
                        HardwareClass.LINUX,
                        "x86_64",
                        4,
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

        AdapterRegistry adapterRegistry =
                new AdapterRegistry(
                        Map.of(
                                "stub",
                                new AlwaysSucceedsAdapter()
                        )
                );

        GreedyCostScheduler scheduler =
                new GreedyCostScheduler(
                        new WeightedPlacementCostModel()
                );

        TaskJournal journal = new TaskJournal();

        TaskRecoveryManager recoveryManager =
                new TaskRecoveryManager();

        PlacementReservationService reservationService =
                new PlacementReservationService(
                        registry,
                        scheduler
                );

        PendingTaskQueue pendingQueue =
                new PendingTaskQueue();

        TaskDispatcher dispatcher =
                new TaskDispatcher(
                        registry,
                        adapterRegistry,
                        journal,
                        recoveryManager,
                        pendingQueue,
                        reservationService
                );

        /*
         * T1 requires more CPU cores than the only node has,
         * so T1 is permanently ineligible for this test.
         */
        TaskDescriptor t1 =
                new TaskDescriptor(
                        "task-001",
                        "Monte-Carlo-Pi",
                        999,
                        512
                );

        /*
         * T2 and T3 are both compatible with the four-core node.
         */
        TaskDescriptor t2 =
                new TaskDescriptor(
                        "task-002",
                        "Monte-Carlo-Pi",
                        1,
                        512
                );

        TaskDescriptor t3 =
                new TaskDescriptor(
                        "task-003",
                        "Monte-Carlo-Pi",
                        1,
                        512
                );

        /*
         * Insert in known FIFO order.
         */
        pendingQueue.enqueue(t1);
        pendingQueue.enqueue(t2);
        pendingQueue.enqueue(t3);

        /*
         * Take a FIFO snapshot, exactly as the recovery scheduler does.
         *
         * T1 is attempted first and remains pending.
         * T2 succeeds and is removed.
         * T3 succeeds and is removed.
         */
        for (PendingTaskQueue.Pending pending :
                pendingQueue.snapshot()) {

            TaskDispatcher.Outcome outcome =
                    dispatcher.replace(pending.task());

            if (outcome instanceof TaskDispatcher.Outcome.Completed) {

                pendingQueue.remove(
                        pending.task().id()
                );
            }
        }

        /*
         * Only T1 should remain pending.
         */
        assertEquals(
                1,
                pendingQueue.size()
        );

        PendingTaskQueue.Pending remaining =
                pendingQueue.snapshot().get(0);

        assertEquals(
                "task-001",
                remaining.task().id()
        );

        /*
         * T2 and T3 must have been removed after successful recovery.
         */
        assertTrue(
                pendingQueue.snapshot()
                        .stream()
                        .noneMatch(
                                p -> p.task().id().equals("task-002")
                        )
        );

        assertTrue(
                pendingQueue.snapshot()
                        .stream()
                        .noneMatch(
                                p -> p.task().id().equals("task-003")
                        )
        );
    }

    /** Adapter that completes every task immediately. */
    private static final class AlwaysSucceedsAdapter
            implements IHardwareAdapter {

        @Override
        public TaskResult executeTask(
                TaskDescriptor task,
                NodeExecutionTarget target) {

            return new TaskResult(
                    task.id(),
                    "SUCCESS",
                    "ok"
            );
        }

        @Override
        public void reportResult(
                TaskResult result) {
        }
    }
}
