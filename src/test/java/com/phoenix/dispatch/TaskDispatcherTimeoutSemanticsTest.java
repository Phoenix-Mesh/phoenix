// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
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
import com.phoenix.model.NodeStatus;
import com.phoenix.model.ResourceStatus;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;
import com.phoenix.recovery.TaskRecoveryManager;
import com.phoenix.scheduler.GreedyCostScheduler;
import com.phoenix.scheduler.WeightedPlacementCostModel;
import com.phoenix.tui.TaskJournal;

/**
 * Issue #28: the dispatcher must keep re-placing every retryable failure,
 * but may only degrade a node when the failure implicates the node.
 */
class TaskDispatcherTimeoutSemanticsTest {

    /** Returns scripted outcomes in call order and records which node ran. */
    private static final class ScriptedAdapter
            implements IHardwareAdapter {

        private final Deque<TaskFailureReason> script;
        final List<String> attemptedNodeIds = new ArrayList<>();

        ScriptedAdapter(TaskFailureReason... script) {
            this.script = new ArrayDeque<>(List.of(script));
        }

        @Override
        public TaskResult executeTask(
                TaskDescriptor task,
                NodeExecutionTarget target) {

            attemptedNodeIds.add(target.nodeId());

            TaskFailureReason next = script.pop();

            return next == TaskFailureReason.NONE
                    ? new TaskResult(task.id(), "SUCCESS", "ok")
                    : new TaskResult(
                            task.id(),
                            "FAILED",
                            "scripted " + next,
                            next);
        }

        @Override
        public void reportResult(TaskResult result) {
        }
    }

    private static final class Fixture {
        final DeviceRegistry registry = new DeviceRegistry();
        final PendingTaskQueue pendingQueue = new PendingTaskQueue();
        final PlacementReservationService reservations;
        final TaskDispatcher dispatcher;
        final ScriptedAdapter adapter;

        Fixture(int nodes, TaskFailureReason... script) {

            for (int i = 1; i <= nodes; i++) {
                registry.register(
                        new NodeInfo(
                                "n" + i,
                                "node-" + i,
                                HardwareClass.LINUX,
                                "x86_64",
                                2,
                                4096,
                                "stub",
                                Set.of()),
                        ResourceStatus.of(10.0, 4096, 0, 0, 1.0, 1.0)
                );
            }

            adapter = new ScriptedAdapter(script);

            GreedyCostScheduler scheduler =
                    new GreedyCostScheduler(
                            new WeightedPlacementCostModel());

            reservations =
                    new PlacementReservationService(registry, scheduler);

            dispatcher =
                    new TaskDispatcher(
                            registry,
                            new AdapterRegistry(Map.of("stub", adapter)),
                            new TaskJournal(),
                            new TaskRecoveryManager(),
                            pendingQueue,
                            reservations);
        }

        NodeStatus stateOf(String nodeId) {
            return registry.get(nodeId).orElseThrow().state();
        }
    }

    private static TaskDescriptor task() {
        return new TaskDescriptor("task-001", "Monte-Carlo-Pi", 1, 128);
    }

    @Test
    void deliveryTimeoutReplacesTheTaskButDoesNotDegradeTheNode() {

        Fixture f = new Fixture(
                2,
                TaskFailureReason.DELIVERY_TIMEOUT,
                TaskFailureReason.NONE
        );

        TaskDispatcher.Outcome outcome = f.dispatcher.replace(task());

        TaskDispatcher.Outcome.Completed completed =
                assertInstanceOf(
                        TaskDispatcher.Outcome.Completed.class,
                        outcome);

        assertTrue(completed.result().result().isSuccess());

        String first = f.adapter.attemptedNodeIds.get(0);
        String second = f.adapter.attemptedNodeIds.get(1);

        // Re-placed on the other node...
        assertNotEquals(first, second);

        // ...and the backlogged node is NOT penalised.
        assertEquals(NodeStatus.ONLINE, f.stateOf(first));

        // Reservations from both attempts were released.
        assertEquals(0, f.reservations.reservationCount(first));
        assertEquals(0, f.reservations.reservationCount(second));
    }

    @Test
    void failuresThatImplicateTheNodeStillDegradeIt() {

        for (TaskFailureReason reason : List.of(
                TaskFailureReason.TIMEOUT,
                TaskFailureReason.EXECUTION_TIMEOUT,
                TaskFailureReason.NODE_UNREACHABLE,
                TaskFailureReason.TRANSPORT_ERROR)) {

            Fixture f = new Fixture(
                    2,
                    reason,
                    TaskFailureReason.NONE
            );

            TaskDispatcher.Outcome outcome =
                    f.dispatcher.replace(task());

            assertInstanceOf(
                    TaskDispatcher.Outcome.Completed.class,
                    outcome);

            String first = f.adapter.attemptedNodeIds.get(0);

            assertEquals(
                    NodeStatus.DEGRADED,
                    f.stateOf(first),
                    reason + " must still degrade the node"
            );

            assertNotEquals(
                    first,
                    f.adapter.attemptedNodeIds.get(1)
            );
        }
    }

    @Test
    void deliveryTimeoutOnTheOnlyNodeWaitsAndLeavesTheNodeSchedulable() {

        Fixture f = new Fixture(
                1,
                TaskFailureReason.DELIVERY_TIMEOUT,
                TaskFailureReason.NONE
        );

        TaskDispatcher.Outcome first = f.dispatcher.replace(task());

        assertInstanceOf(TaskDispatcher.Outcome.Waiting.class, first);
        assertEquals(1, f.pendingQueue.size());
        assertEquals(NodeStatus.ONLINE, f.stateOf("n1"));
        assertEquals(0, f.reservations.reservationCount("n1"));

        // Because the node was not degraded, the very next recovery pass
        // can place the waiting task on it again and complete.
        TaskDispatcher.Outcome retry = f.dispatcher.replace(task());

        TaskDispatcher.Outcome.Completed completed =
                assertInstanceOf(
                        TaskDispatcher.Outcome.Completed.class,
                        retry);

        assertTrue(completed.result().result().isSuccess());
    }

    @Test
    void unreachableOnTheOnlyNodeWaitsAndTheNodeIsNoLongerSchedulable() {

        Fixture f = new Fixture(
                1,
                TaskFailureReason.NODE_UNREACHABLE
        );

        TaskDispatcher.Outcome outcome = f.dispatcher.replace(task());

        assertInstanceOf(TaskDispatcher.Outcome.Waiting.class, outcome);
        assertEquals(NodeStatus.DEGRADED, f.stateOf("n1"));
        assertFalse(f.registry.get("n1").orElseThrow().isSchedulable());
    }
}
