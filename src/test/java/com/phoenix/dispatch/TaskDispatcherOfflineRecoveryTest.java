// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.phoenix.adapter.IHardwareAdapter;
import com.phoenix.core.AdapterRegistry;
import com.phoenix.core.DeviceRegistry;
import com.phoenix.core.PlacementReservationService;
import com.phoenix.core.TaskLifecycleService;
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
 * A task whose node goes OFFLINE mid-attempt is re-placed through the
 * existing dispatcher path: reservation released, no node re-penalised,
 * retried on an eligible node with a fresh attempt.
 */
class TaskDispatcherOfflineRecoveryTest {

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void stop() {
        executor.shutdownNow();
    }

    /**
     * Behaves like a blocking transport: starts an attempt bound to the node
     * and blocks until it completes or Core abandons it.
     */
    private static final class BlockingTransportAdapter
            implements IHardwareAdapter {

        final TaskLifecycleService lifecycle;
        final CountDownLatch started = new CountDownLatch(1);
        final List<String> attempted = new CopyOnWriteArrayList<>();

        BlockingTransportAdapter(TaskLifecycleService lifecycle) {
            this.lifecycle = lifecycle;
        }

        @Override
        public TaskResult executeTask(
                TaskDescriptor task,
                NodeExecutionTarget target) {

            attempted.add(target.nodeId());

            TaskLifecycleService.Attempt attempt =
                    lifecycle.accepted(
                            task.id(),
                            target.nodeId(),
                            com.phoenix.core.AttemptFencing.LEGACY);
            CountDownLatch released = new CountDownLatch(1);
            attempt.onAbandon(released::countDown);
            started.countDown();

            try {
                released.await(30, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }

            return new TaskResult(
                    task.id(), "FAILED", "node went offline",
                    TaskFailureReason.NODE_OFFLINE);
        }

        @Override
        public void reportResult(TaskResult result) {
        }
    }

    private static final class Recording implements IHardwareAdapter {

        final List<String> attempted = new CopyOnWriteArrayList<>();

        @Override
        public TaskResult executeTask(
                TaskDescriptor task,
                NodeExecutionTarget target) {

            attempted.add(target.nodeId());
            return new TaskResult(task.id(), "SUCCESS", "recovered");
        }

        @Override
        public void reportResult(TaskResult result) {
        }
    }

    private static NodeInfo node(String id, String adapter) {
        return new NodeInfo(id, "node-" + id, HardwareClass.LINUX,
                "x86_64", 2, 4096, adapter, Set.of());
    }

    private static TaskDescriptor task() {
        return new TaskDescriptor("task-001", "Monte-Carlo-Pi", 1, 128);
    }

    @Test
    void tasksOnAnOfflineNodeMoveToAnEligibleNodeWithAFreshAttempt()
            throws Exception {

        DeviceRegistry registry = new DeviceRegistry();
        TaskLifecycleService lifecycle = new TaskLifecycleService();

        // A is cheaper, so it is chosen first; B is the alternative.
        registry.register(node("A", "blocking"),
                ResourceStatus.of(5.0, 4096, 0, 0, 1.0, 1.0));
        registry.register(node("B", "recording"),
                ResourceStatus.of(80.0, 4096, 0, 0, 1.0, 1.0));

        BlockingTransportAdapter blocking =
                new BlockingTransportAdapter(lifecycle);
        Recording recording = new Recording();

        GreedyCostScheduler scheduler =
                new GreedyCostScheduler(new WeightedPlacementCostModel());
        PlacementReservationService reservations =
                new PlacementReservationService(registry, scheduler);
        PendingTaskQueue pending = new PendingTaskQueue();

        TaskDispatcher dispatcher = new TaskDispatcher(
                registry,
                new AdapterRegistry(Map.of(
                        "blocking", blocking, "recording", recording)),
                new TaskJournal(),
                new TaskRecoveryManager(),
                pending,
                reservations);

        Future<TaskDispatcher.Outcome> outcome =
                executor.submit(() -> dispatcher.replace(task()));

        assertTrue(blocking.started.await(3, TimeUnit.SECONDS));
        assertEquals(1, reservations.reservationCount("A"));

        // What NodeHealthMonitor + the reconciler do on OFFLINE:
        registry.markOffline("A");
        lifecycle.abandonAttemptsOn("A");

        TaskDispatcher.Outcome.Completed completed =
                assertInstanceOf(
                        TaskDispatcher.Outcome.Completed.class,
                        outcome.get(3, TimeUnit.SECONDS));

        assertTrue(completed.result().result().isSuccess());
        assertEquals(List.of("A"), blocking.attempted);
        assertEquals(List.of("B"), recording.attempted);

        // The offline node is not re-penalised and the reservation is gone.
        assertEquals(NodeStatus.OFFLINE,
                registry.get("A").orElseThrow().state());
        assertEquals(0, reservations.reservationCount("A"));
        assertEquals(0, reservations.reservationCount("B"));
        assertEquals(0, pending.size());
    }


    @Test
    void allRetryablePlacementAttemptsReturnToWaitingInsteadOfBecomingTerminal()
            throws Exception {

        DeviceRegistry registry = new DeviceRegistry();

        IHardwareAdapter failing = new IHardwareAdapter() {
            @Override
            public TaskResult executeTask(
                    TaskDescriptor task,
                    NodeExecutionTarget target) {
                return new TaskResult(
                        task.id(),
                        "FAILED",
                        "node went offline",
                        TaskFailureReason.NODE_OFFLINE
                );
            }

            @Override
            public void reportResult(TaskResult result) {
            }
        };

        registry.register(
                node("A", "failing"),
                ResourceStatus.of(5.0, 4096, 0, 0, 1.0, 1.0));
        registry.register(
                node("B", "failing"),
                ResourceStatus.of(20.0, 4096, 0, 0, 1.0, 1.0));
        registry.register(
                node("C", "failing"),
                ResourceStatus.of(40.0, 4096, 0, 0, 1.0, 1.0));

        GreedyCostScheduler scheduler =
                new GreedyCostScheduler(new WeightedPlacementCostModel());
        PendingTaskQueue pending = new PendingTaskQueue();
        PlacementReservationService reservations =
                new PlacementReservationService(registry, scheduler);

        TaskDispatcher dispatcher = new TaskDispatcher(
                registry,
                new AdapterRegistry(Map.of("failing", failing)),
                new TaskJournal(),
                new TaskRecoveryManager(),
                pending,
                reservations);

        TaskDispatcher.Outcome outcome =
                dispatcher.replace(task());

        assertInstanceOf(
                TaskDispatcher.Outcome.Waiting.class,
                outcome);

        assertEquals(1, pending.size());

        PendingTaskQueue.Pending waiting =
                pending.get("task-001");

        assertEquals(1, waiting.failedCycles());
        assertEquals(
                TaskFailureReason.NODE_OFFLINE,
                waiting.lastFailure());
    }

    @Test
    void withNoOtherNodeTheTaskWaitsWithAFreshRecoveryWindow()
            throws Exception {

        DeviceRegistry registry = new DeviceRegistry();
        TaskLifecycleService lifecycle = new TaskLifecycleService();

        registry.register(node("A", "blocking"),
                ResourceStatus.of(5.0, 4096, 0, 0, 1.0, 1.0));

        BlockingTransportAdapter blocking =
                new BlockingTransportAdapter(lifecycle);

        GreedyCostScheduler scheduler =
                new GreedyCostScheduler(new WeightedPlacementCostModel());
        PlacementReservationService reservations =
                new PlacementReservationService(registry, scheduler);
        PendingTaskQueue pending = new PendingTaskQueue();

        TaskDispatcher dispatcher = new TaskDispatcher(
                registry,
                new AdapterRegistry(Map.of("blocking", blocking)),
                new TaskJournal(),
                new TaskRecoveryManager(),
                pending,
                reservations);

        Future<TaskDispatcher.Outcome> outcome =
                executor.submit(() -> dispatcher.replace(task()));

        assertTrue(blocking.started.await(3, TimeUnit.SECONDS));

        registry.markOffline("A");
        lifecycle.abandonAttemptsOn("A");

        assertInstanceOf(
                TaskDispatcher.Outcome.Waiting.class,
                outcome.get(3, TimeUnit.SECONDS));

        // Waiting for a node, reservation released, window freshly started.
        assertEquals(1, pending.size());
        PendingTaskQueue.Pending waiting = pending.get("task-001");
        assertEquals(1, waiting.failedCycles());
        assertEquals(TaskFailureReason.NODE_OFFLINE, waiting.lastFailure());
        assertTrue(!pending.isExpired("task-001"));
        assertEquals(0, reservations.reservationCount("A"));
    }
}
