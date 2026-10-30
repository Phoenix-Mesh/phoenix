// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

import com.phoenix.model.HardwareClass;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.ResourceStatus;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.scheduler.GreedyCostScheduler;
import com.phoenix.scheduler.TaskScheduler;
import com.phoenix.scheduler.WeightedPlacementCostModel;

class PlacementReservationServiceTest {

    private PlacementReservationService service(
            DeviceRegistry registry) {

        TaskScheduler scheduler =
                new GreedyCostScheduler(
                        new WeightedPlacementCostModel()
                );

        return new PlacementReservationService(
                registry,
                scheduler
        );
    }

    private void registerNode(
            DeviceRegistry registry,
            String nodeId,
            int cpuCores,
            long memoryMb,
            long availableMemoryMb) {

        registerNode(
                registry,
                nodeId,
                cpuCores,
                memoryMb,
                availableMemoryMb,
                10.0
        );
    }

    private void registerNode(
            DeviceRegistry registry,
            String nodeId,
            int cpuCores,
            long memoryMb,
            long availableMemoryMb,
            double cpuUsagePercent) {

        registry.register(
                new NodeInfo(
                        nodeId,
                        nodeId,
                        HardwareClass.LINUX,
                        "x86_64",
                        cpuCores,
                        memoryMb,
                        "linux",
                        Set.of()
                ),
                ResourceStatus.of(
                        cpuUsagePercent,
                        availableMemoryMb,
                        0,
                        0,
                        1.0,
                        1.0
                )
        );
    }

    @Test
    void reservesRequestedCapacityAndReleasesIt() {

        DeviceRegistry registry =
                new DeviceRegistry();

        registerNode(
                registry,
                "n1",
                4,
                4096,
                4096
        );

        PlacementReservationService reservations =
                service(registry);

        TaskDescriptor task =
                new TaskDescriptor(
                        "task-001",
                        "Monte-Carlo-Pi",
                        2,
                        1024
                );

        Optional<PlacementReservationService.Reservation>
                result =
                reservations.selectAndReserve(
                        task,
                        Set.of()
                );

        assertTrue(result.isPresent());

        assertEquals(
                2,
                reservations.reservedCpuCores("n1")
        );

        assertEquals(
                1024,
                reservations.reservedMemoryMb("n1")
        );

        result.get().release();

        assertEquals(
                0,
                reservations.reservedCpuCores("n1")
        );

        assertEquals(
                0,
                reservations.reservedMemoryMb("n1")
        );

        assertEquals(
                0,
                reservations.reservationCount("n1")
        );
    }

    @Test
    void completedReservationMakesCapacityImmediatelyAvailableToNextPlacement() {

        DeviceRegistry registry =
                new DeviceRegistry();

        registerNode(
                registry,
                "n1",
                2,
                2048,
                2048
        );

        PlacementReservationService reservations =
                service(registry);

        TaskDescriptor first =
                new TaskDescriptor(
                        "task-A",
                        "Monte-Carlo-Pi",
                        1,
                        512
                );

        TaskDescriptor second =
                new TaskDescriptor(
                        "task-B",
                        "Monte-Carlo-Pi",
                        1,
                        512
                );

        TaskDescriptor third =
                new TaskDescriptor(
                        "task-C",
                        "Monte-Carlo-Pi",
                        1,
                        512
                );

        var reservationA =
                reservations.selectAndReserve(
                        first,
                        Set.of()
                );

        var reservationB =
                reservations.selectAndReserve(
                        second,
                        Set.of()
                );

        assertTrue(reservationA.isPresent());
        assertTrue(reservationB.isPresent());

        /*
         * All two CPU cores are already committed.
         */
        assertEquals(
                2,
                reservations.reservedCpuCores("n1")
        );

        var reservationCWhileFull =
                reservations.selectAndReserve(
                        third,
                        Set.of()
                );

        assertFalse(
                reservationCWhileFull.isPresent()
        );

        /*
         * Task A finishes.
         *
         * Its reservation disappears immediately. No heartbeat is required
         * for Phoenix's own reservation state to change.
         */
        reservationA.get().release();

        assertEquals(
                1,
                reservations.reservedCpuCores("n1")
        );

        /*
         * Task C can now reserve the capacity previously committed to A,
         * while B remains reserved.
         */
        var reservationCAfterRelease =
                reservations.selectAndReserve(
                        third,
                        Set.of()
                );

        assertTrue(
                reservationCAfterRelease.isPresent()
        );

        assertEquals(
                2,
                reservations.reservedCpuCores("n1")
        );

        reservationB.get().release();
        reservationCAfterRelease.get().release();

        assertEquals(
                0,
                reservations.reservedCpuCores("n1")
        );
    }

    @Test
    void reservationPreventsMemoryOvercommit() {

        DeviceRegistry registry =
                new DeviceRegistry();

        registerNode(
                registry,
                "n1",
                4,
                4096,
                1024
        );

        PlacementReservationService reservations =
                service(registry);

        TaskDescriptor first =
                new TaskDescriptor(
                        "task-A",
                        "Monte-Carlo-Pi",
                        1,
                        512
                );

        TaskDescriptor second =
                new TaskDescriptor(
                        "task-B",
                        "Monte-Carlo-Pi",
                        1,
                        1024
                );

        var reservationA =
                reservations.selectAndReserve(
                        first,
                        Set.of()
                );

        assertTrue(
                reservationA.isPresent()
        );

        /*
         * Current observed RAM = 1024 MB.
         * A has committed 512 MB.
         * Only 512 MB remains reservable.
         */
        var reservationB =
                reservations.selectAndReserve(
                        second,
                        Set.of()
                );

        assertTrue(
                reservationB.isEmpty()
        );

        reservationA.get().release();
    }

    @Test
    void saturatedNodeIsSkippedForAnotherEligibleNode() {

        DeviceRegistry registry =
                new DeviceRegistry();

        registerNode(
                registry,
                "n1",
                2,
                4096,
                4096,
                10.0
        );

        registerNode(
                registry,
                "n2",
                4,
                4096,
                4096,
                75.0
        );

        PlacementReservationService reservations =
                service(registry);

        TaskDescriptor first =
                new TaskDescriptor(
                        "task-A",
                        "Monte-Carlo-Pi",
                        2,
                        512
                );

        TaskDescriptor second =
                new TaskDescriptor(
                        "task-B",
                        "Monte-Carlo-Pi",
                        2,
                        512
                );

        var reservationA =
                reservations.selectAndReserve(
                        first,
                        Set.of()
                );

        assertTrue(
                reservationA.isPresent()
        );

        assertEquals(
                "n1",
                reservationA.get().nodeId()
        );

        var reservationB =
                reservations.selectAndReserve(
                        second,
                        Set.of()
                );

        assertTrue(
                reservationB.isPresent()
        );

        assertEquals(
                "n2",
                reservationB.get().nodeId()
        );

        reservationA.get().release();
        reservationB.get().release();
    }

    @Test
    void concurrentReservationsCannotOvercommitSameNode()
            throws Exception {

        DeviceRegistry registry =
                new DeviceRegistry();

        registerNode(
                registry,
                "n1",
                2,
                4096,
                4096
        );

        PlacementReservationService reservations =
                service(registry);

        ExecutorService executor =
                Executors.newFixedThreadPool(4);

        List<PlacementReservationService.Reservation>
                successfulReservations =
                Collections.synchronizedList(
                        new ArrayList<>()
                );

        try {

            List<Callable<Boolean>> attempts =
                    new ArrayList<>();

            for (int i = 0; i < 4; i++) {

                int taskNumber = i;

                attempts.add(() -> {

                    TaskDescriptor task =
                            new TaskDescriptor(
                                    "task-" + taskNumber,
                                    "Monte-Carlo-Pi",
                                    1,
                                    512
                            );

                    Optional<PlacementReservationService.Reservation>
                            result =
                            reservations.selectAndReserve(
                                    task,
                                    Set.of()
                            );

                    if (result.isPresent()) {
                        successfulReservations.add(
                                result.get()
                        );
                        return true;
                    }

                    return false;
                });
            }

            List<Future<Boolean>> futures =
                    executor.invokeAll(
                            attempts
                    );

            long successfulCount =
                    futures.stream()
                            .filter(future -> {
                                try {
                                    return future.get();
                                } catch (Exception exception) {
                                    throw new RuntimeException(
                                            exception
                                    );
                                }
                            })
                            .count();

            /*
             * Two 1-core reservations fit on a 2-core node.
             * The other two must be rejected.
             */
            assertEquals(
                    2,
                    successfulCount
            );

            assertEquals(
                    2,
                    reservations.reservedCpuCores("n1")
            );

            assertEquals(
                    2,
                    reservations.reservationCount("n1")
            );

        } finally {

            successfulReservations.forEach(
                    PlacementReservationService.Reservation::release
            );

            executor.shutdownNow();
        }

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
