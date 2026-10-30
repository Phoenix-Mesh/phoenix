// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import com.phoenix.model.NodeStatus;
import com.phoenix.model.ResourceStatus;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskResult;
import com.phoenix.recovery.TaskRecoveryManager;
import com.phoenix.scheduler.GreedyCostScheduler;
import com.phoenix.scheduler.WeightedPlacementCostModel;
import com.phoenix.tui.TaskJournal;

/**
 * A node registered with an adapter type Core cannot resolve must fail its
 * own attempt; the task moves on and the reservation is released.
 */
class TaskDispatcherUnknownAdapterTest {

    @Test
    void anUnknownAdapterFailsTheAttemptAndTheTaskMovesToAnotherNode() {

        DeviceRegistry registry = new DeviceRegistry();

        // The misconfigured node is the cheaper one, so it is tried first.
        registry.register(
                new NodeInfo("bad", "bad-node", HardwareClass.LINUX, "x86_64",
                        2, 4096, "no-such-adapter", Set.of()),
                ResourceStatus.of(5.0, 4096, 0, 0, 1.0, 1.0));
        registry.register(
                new NodeInfo("good", "good-node", HardwareClass.LINUX, "x86_64",
                        2, 4096, "stub", Set.of()),
                ResourceStatus.of(80.0, 4096, 0, 0, 1.0, 1.0));

        IHardwareAdapter stub = new IHardwareAdapter() {
            @Override
            public TaskResult executeTask(TaskDescriptor task, NodeExecutionTarget target) {
                return new TaskResult(task.id(), "SUCCESS", "ok");
            }

            @Override
            public void reportResult(TaskResult result) {
            }
        };

        GreedyCostScheduler scheduler =
                new GreedyCostScheduler(new WeightedPlacementCostModel());
        PlacementReservationService reservations =
                new PlacementReservationService(registry, scheduler);

        TaskDispatcher dispatcher = new TaskDispatcher(
                registry,
                new AdapterRegistry(Map.of("stub", stub)),
                new TaskJournal(),
                new TaskRecoveryManager(),
                new PendingTaskQueue(),
                reservations);

        TaskDispatcher.Outcome.Completed completed = assertInstanceOf(
                TaskDispatcher.Outcome.Completed.class,
                dispatcher.replace(new TaskDescriptor("task-001", "Monte-Carlo-Pi", 1, 128)));

        assertTrue(completed.result().result().isSuccess());
        assertEquals("good", completed.result().nodeId());
        assertEquals(NodeStatus.DEGRADED, registry.get("bad").orElseThrow().state());
        assertEquals(0, reservations.reservationCount("bad"));
    }
}
