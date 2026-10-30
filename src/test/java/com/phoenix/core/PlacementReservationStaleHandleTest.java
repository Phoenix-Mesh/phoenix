// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.phoenix.model.HardwareClass;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.ResourceStatus;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.scheduler.GreedyCostScheduler;
import com.phoenix.scheduler.WeightedPlacementCostModel;

/**
 * An attempt that already released its reservation can never release, or
 * otherwise disturb, the reservation of the attempt that replaced it.
 *
 * <p>This holds because every {@code Reservation} handle releases at most
 * once and a replacement is only acquired after the previous attempt's handle
 * has been closed. It does <em>not</em> rely on attempt-level ownership inside
 * the service: allocations are keyed by (node, task).
 */
class PlacementReservationStaleHandleTest {

    @Test
    void aStaleHandleReleasedAgainDoesNotFreeTheReplacementsReservation() {

        DeviceRegistry registry = new DeviceRegistry();
        registry.register(
                new NodeInfo("n1", "n1", HardwareClass.LINUX, "x86_64",
                        8, 8192, "linux", Set.of()),
                ResourceStatus.of(10.0, 8192, 0, 0, 1.0, 1.0));

        PlacementReservationService service =
                new PlacementReservationService(
                        registry,
                        new GreedyCostScheduler(
                                new WeightedPlacementCostModel()));

        TaskDescriptor task =
                new TaskDescriptor("task-001", "Monte-Carlo-Pi", 2, 512);

        var abandoned = service.selectAndReserve(task, Set.of()).orElseThrow();
        abandoned.release();                          // attempt 1 ends

        var replacement = service.selectAndReserve(task, Set.of()).orElseThrow();

        abandoned.release();                          // stale handle again
        abandoned.close();

        assertEquals(2, service.reservedCpuCores("n1"));
        assertEquals(512, service.reservedMemoryMb("n1"));
        assertEquals(1, service.reservationCount("n1"));

        replacement.release();
        assertEquals(0, service.reservationCount("n1"));
    }
}
