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

class TaskDispatcherReservationTest {

    @Test
    void successfulExecutionReleasesPlacementReservation() {

        DeviceRegistry registry =
                new DeviceRegistry();

        registry.register(
                new NodeInfo(
                        "n1",
                        "stub-node",
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

        IHardwareAdapter adapter =
                new IHardwareAdapter() {

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
                };

        AdapterRegistry adapterRegistry =
                new AdapterRegistry(
                        Map.of(
                                "stub",
                                adapter
                        )
                );

        GreedyCostScheduler scheduler =
                new GreedyCostScheduler(
                        new WeightedPlacementCostModel()
                );

        TaskJournal journal =
                new TaskJournal();

        TaskRecoveryManager recoveryManager =
                new TaskRecoveryManager();

        PendingTaskQueue pendingQueue =
                new PendingTaskQueue();

        PlacementReservationService reservations =
                new PlacementReservationService(
                        registry,
                        scheduler
                );

        TaskDispatcher dispatcher =
                new TaskDispatcher(
                        registry,
                        adapterRegistry,
                        journal,
                        recoveryManager,
                        pendingQueue,
                        reservations
                );

        TaskDescriptor task =
                new TaskDescriptor(
                        "task-001",
                        "Monte-Carlo-Pi",
                        2,
                        512
                );

        TaskDispatcher.Outcome outcome =
                dispatcher.replace(task);

        assertTrue(
                outcome instanceof TaskDispatcher.Outcome.Completed
        );

        /*
         * The task consumed the full declared CPU capacity while executing.
         * Once execution returned, the reservation must be gone.
         */
        assertEquals(
                0,
                reservations.reservedCpuCores("n1")
        );

        assertEquals(
                0,
                reservations.reservationCount("n1")
        );
    }
}
