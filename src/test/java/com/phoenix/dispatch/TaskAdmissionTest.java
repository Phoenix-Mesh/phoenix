// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

class TaskAdmissionTest {

    @Test
    void olderEligiblePendingTaskIsPreferredOverFreshTask()
            throws Exception {

        Fixture fixture =
                newFixture();

        try {

            TaskDescriptor oldTask =
                    new TaskDescriptor(
                            "task-old",
                            "Monte-Carlo-Pi",
                            1,
                            512
                    );

            TaskDescriptor freshTask =
                    new TaskDescriptor(
                            "task-fresh",
                            "Monte-Carlo-Pi",
                            1,
                            512
                    );

            fixture.pendingQueue()
                    .enqueue(oldTask);

            fixture.admission()
                    .start();

            fixture.admission()
                    .submitFresh(freshTask)
                    .get(
                            2,
                            TimeUnit.SECONDS
                    );

            assertEquals(
                    List.of(
                            "task-old",
                            "task-fresh"
                    ),
                    fixture.executionOrder()
            );

        } finally {

            fixture.admission()
                    .stop();
        }
    }

    @Test
    void olderIneligiblePendingTaskDoesNotBlockFreshEligibleTask()
            throws Exception {

        Fixture fixture =
                newFixture();

        try {

            TaskDescriptor stuckTask =
                    new TaskDescriptor(
                            "task-stuck",
                            "Monte-Carlo-Pi",
                            999,
                            512
                    );

            TaskDescriptor freshTask =
                    new TaskDescriptor(
                            "task-fresh",
                            "Monte-Carlo-Pi",
                            1,
                            512
                    );

            fixture.pendingQueue()
                    .enqueue(stuckTask);

            fixture.admission()
                    .start();

            fixture.admission()
                    .submitFresh(freshTask)
                    .get(
                            2,
                            TimeUnit.SECONDS
                    );

            assertEquals(
                    List.of("task-fresh"),
                    fixture.executionOrder()
            );

            assertEquals(
                    1,
                    fixture.pendingQueue()
                            .size()
            );

            assertEquals(
                    "task-stuck",
                    fixture.pendingQueue()
                            .snapshot()
                            .get(0)
                            .task()
                            .id()
            );

        } finally {

            fixture.admission()
                    .stop();
        }
    }

    @Test
    void multipleFreshSubmissionsExecuteConcurrentlyNotSerializedByAdmission()
            throws Exception {

        ConcurrencyFixture fixture =
                newConcurrencyFixture();

        CountDownLatch slowStarted =
                new CountDownLatch(1);

        CountDownLatch releaseSlow =
                new CountDownLatch(1);

        try {

            fixture.blockingBehavior()
                    .put(
                            "task-slow",
                            () -> {

                                slowStarted.countDown();

                                try {

                                    releaseSlow.await(
                                            5,
                                            TimeUnit.SECONDS
                                    );

                                } catch (InterruptedException exception) {

                                    Thread.currentThread()
                                            .interrupt();
                                }
                            }
                    );

            TaskDescriptor slow =
                    new TaskDescriptor(
                            "task-slow",
                            "Monte-Carlo-Pi",
                            1,
                            512
                    );

            TaskDescriptor fast =
                    new TaskDescriptor(
                            "task-fast",
                            "Monte-Carlo-Pi",
                            1,
                            512
                    );

            fixture.admission()
                    .start();

            var slowFuture =
                    fixture.admission()
                            .submitFresh(slow);

            assertTrue(
                    slowStarted.await(
                            2,
                            TimeUnit.SECONDS
                    ),
                    "slow task execution should have started"
            );

            /*
             * The slow execution is deliberately blocked.
             *
             * If TaskAdmission accidentally executes tasks on the
             * single ordering thread, task-fast would be unable to
             * complete here.
             */
            var fastFuture =
                    fixture.admission()
                            .submitFresh(fast);

            assertTrue(
                    fastFuture
                            .get(
                                    2,
                                    TimeUnit.SECONDS
                            )
                            instanceof TaskDispatcher.Outcome.Completed,
                    "fast task should complete without waiting for slow task"
            );

            releaseSlow.countDown();

            assertTrue(
                    slowFuture
                            .get(
                                    3,
                                    TimeUnit.SECONDS
                            )
                            instanceof TaskDispatcher.Outcome.Completed
            );

        } finally {

            releaseSlow.countDown();

            fixture.admission()
                    .stop();
        }
    }

    private Fixture newFixture() {

        DeviceRegistry registry =
                new DeviceRegistry();

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

        List<String> executionOrder =
                java.util.Collections.synchronizedList(
                        new ArrayList<>()
                );

        IHardwareAdapter adapter =
                new IHardwareAdapter() {

                    @Override
                    public TaskResult executeTask(
                            TaskDescriptor task,
                            NodeExecutionTarget target) {

                        executionOrder.add(
                                task.id()
                        );

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

        PlacementReservationService reservationService =
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
                        reservationService
                );

        TaskAdmission admission =
                new TaskAdmission(
                        pendingQueue,
                        dispatcher,
                        journal
                );

        return new Fixture(
                admission,
                pendingQueue,
                executionOrder
        );
    }

    private ConcurrencyFixture newConcurrencyFixture() {

        Map<String, Runnable> blockingBehavior =
                new ConcurrentHashMap<>();

        DeviceRegistry registry =
                new DeviceRegistry();

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

        IHardwareAdapter adapter =
                new IHardwareAdapter() {

                    @Override
                    public TaskResult executeTask(
                            TaskDescriptor task,
                            NodeExecutionTarget target) {

                        Runnable behavior =
                                blockingBehavior.get(
                                        task.id()
                                );

                        if (behavior != null) {
                            behavior.run();
                        }

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

        PlacementReservationService reservationService =
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
                        reservationService
                );

        TaskAdmission admission =
                new TaskAdmission(
                        pendingQueue,
                        dispatcher,
                        journal
                );

        return new ConcurrencyFixture(
                admission,
                blockingBehavior
        );
    }

    private record Fixture(
            TaskAdmission admission,
            PendingTaskQueue pendingQueue,
            List<String> executionOrder
    ) {
    }

    private record ConcurrencyFixture(
            TaskAdmission admission,
            Map<String, Runnable> blockingBehavior
    ) {
    }
}
