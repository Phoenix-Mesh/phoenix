// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.scheduler.TaskScheduler;

/**
 * Core-owned reservation state for task placement.
 *
 * <p>DeviceRegistry represents observed node state reported by the node,
 * while this service represents capacity already committed by Phoenix to
 * tasks whose execution attempts are still in flight.</p>
 *
 * <p>Reservations are deliberately kept separate from ResourceStatus.
 * ResourceStatus remains node-reported telemetry; reservation state represents
 * Phoenix's own scheduling decisions.</p>
 *
 * <p>CPU cores and memory are the two reservation dimensions because they are
 * already explicit hard task requirements in TaskRequirements.
 * Phoenix does not introduce a synthetic "task slot" capacity model here.</p>
 *
 * <p>Selection, capacity validation, and reservation acquisition occur inside
 * one synchronized operation. This prevents concurrent placement attempts
 * from both observing the same unreserved capacity and claiming it.</p>
 */
@Singleton
public class PlacementReservationService {

    private final DeviceRegistry registry;
    private final TaskScheduler scheduler;

    /**
     * nodeId -> taskId -> allocation
     *
     * <p>This state belongs to Phoenix Core, not to the node's telemetry.</p>
     */
    private final Map<String, Map<String, Allocation>> reservations =
            new HashMap<>();

    @Inject
    public PlacementReservationService(
            DeviceRegistry registry,
            TaskScheduler scheduler) {

        this.registry = registry;
        this.scheduler = scheduler;
    }

    /**
     * Selects an eligible node and atomically reserves the task's requested
     * CPU and memory capacity on that node.
     *
     * <p>The scheduler continues to own Phoenix's placement policy and
     * weighted-cost calculation. Reservation state acts as an additional
     * hard capacity guard before a node reaches the scheduler.</p>
     *
     * <p>A fresh registry snapshot is obtained for every placement attempt.
     * The selected node is also re-read immediately before reservation so
     * that a concurrent telemetry update can invalidate the stale snapshot
     * before Phoenix commits the reservation.</p>
     *
     * @param task task whose requirements must be reserved
     * @param excludedNodeIds nodes already attempted for this task
     * @return a reservation handle if placement and reservation succeed
     */
    public synchronized Optional<Reservation> selectAndReserve(
            TaskDescriptor task,
            Set<String> excludedNodeIds) {

        Set<String> rejectedNodeIds =
                new HashSet<>(excludedNodeIds);

        while (true) {

            List<NodeRecord> candidates =
                    registry.listSchedulable()
                            .stream()
                            .filter(node ->
                                    !rejectedNodeIds.contains(
                                            node.nodeId()
                                    )
                            )
                            .filter(node ->
                                    hasCapacityForReservation(
                                            node,
                                            task
                                    )
                            )
                            .toList();

            if (candidates.isEmpty()) {
                return Optional.empty();
            }

            Optional<NodeRecord> selected =
                    scheduler.selectNode(
                            task,
                            candidates
                    );

            if (selected.isEmpty()) {
                return Optional.empty();
            }

            String nodeId =
                    selected.get().nodeId();

            /*
             * Re-read the node after the scheduler has selected it.
             *
             * The registry can receive a newer telemetry snapshot while
             * the scheduler is evaluating candidates. Do not commit a
             * reservation against stale resource information.
             */
            Optional<NodeRecord> current =
                    registry.get(nodeId);

            if (current.isEmpty()
                    || !current.get().isSchedulable()
                    || !hasCapacityForReservation(
                            current.get(),
                            task)) {

                rejectedNodeIds.add(nodeId);
                continue;
            }

            NodeRecord node =
                    current.get();

            /*
             * The service is synchronized, so no other reservation can
             * change between the capacity check above and this insertion.
             */
            Allocation allocation =
                    new Allocation(
                            task.id(),
                            nodeId,
                            task.requirements().cpuCores(),
                            task.requirements().memoryMb()
                    );

            reservations
                    .computeIfAbsent(
                            nodeId,
                            ignored -> new HashMap<>()
                    )
                    .put(
                            task.id(),
                            allocation
                    );

            System.out.printf(
                    "[Reservation] Acquired task=%s node=%s "
                            + "cpu=%d memory=%dMB "
                            + "reservedCpu=%d reservedMemory=%dMB%n",
                    task.id(),
                    node.info().displayName(),
                    allocation.cpuCores(),
                    allocation.memoryMb(),
                    reservedCpuCores(nodeId),
                    reservedMemoryMb(nodeId)
            );

            return Optional.of(
                    new Reservation(
                            this,
                            node,
                            allocation
                    )
            );
        }
    }

    /**
     * Checks whether a task can still reserve its declared CPU and memory
     * requirements on the node after accounting for existing Core-owned
     * reservations.
     *
     * <p>CPU uses the node's declared core capacity because Phoenix does not
     * infer allocatable CPU cores from instantaneous CPU utilization.</p>
     *
     * <p>Memory uses both:
     * <ul>
     *     <li>the node's current observed available memory, and</li>
     *     <li>its declared total memory.</li>
     * </ul>
     *
     * Existing Phoenix reservations are subtracted from both views.
     * This prevents a task from reserving memory that has already been
     * committed by another in-flight task.</p>
     *
     * <p>Existing reservations are deliberately not written into
     * ResourceStatus.</p>
     */
    private boolean hasCapacityForReservation(
            NodeRecord node,
            TaskDescriptor task) {

        AllocationTotals totals =
                totalsFor(node.nodeId());

        /*
         * CPU capacity is based on the node's declared core capacity minus
         * Phoenix-owned reservations.
         */
        int availableCpuCores =
                Math.max(
                        0,
                        node.info().cpuCores()
                                - totals.cpuCores()
                );

        /*
         * Declared memory capacity remaining after Phoenix reservations.
         */
        long declaredUnreservedMemory =
                Math.max(
                        0L,
                        node.info().memoryMb()
                                - totals.memoryMb()
                );

        /*
         * Observed memory remaining after Phoenix reservations.
         *
         * This is the important reservation-aware correction:
         *
         *     observed available RAM
         *         - Phoenix reservations
         *
         * Without this subtraction, two concurrent tasks can both rely on
         * the same currently-available RAM reported by the node.
         */
        long observedUnreservedMemory =
                Math.max(
                        0L,
                        node.status().availableRamMb()
                                - totals.memoryMb()
                );

        /*
         * Never allow the reservation to exceed either:
         *
         *   1. the current observed memory that remains available, or
         *   2. the node's declared memory capacity that remains unreserved.
         */
        long availableMemoryMb =
                Math.min(
                        observedUnreservedMemory,
                        declaredUnreservedMemory
                );

        return availableCpuCores
                >= task.requirements().cpuCores()
                && availableMemoryMb
                >= task.requirements().memoryMb();
    }

    private AllocationTotals totalsFor(String nodeId) {

        Map<String, Allocation> nodeReservations =
                reservations.get(nodeId);

        if (nodeReservations == null) {
            return new AllocationTotals(
                    0,
                    0L
            );
        }

        int cpuCores =
                nodeReservations.values()
                        .stream()
                        .mapToInt(
                                Allocation::cpuCores
                        )
                        .sum();

        long memoryMb =
                nodeReservations.values()
                        .stream()
                        .mapToLong(
                                Allocation::memoryMb
                        )
                        .sum();

        return new AllocationTotals(
                cpuCores,
                memoryMb
        );
    }

    /**
     * Releases a reservation.
     *
     * <p>Release is idempotent. This allows callers to safely release through
     * a terminal execution path without worrying about accidental duplicate
     * cleanup.</p>
     */
    private synchronized void release(
            Reservation reservation) {

        String nodeId =
                reservation.nodeId();

        Map<String, Allocation> nodeReservations =
                reservations.get(nodeId);

        if (nodeReservations == null) {
            return;
        }

        Allocation removed =
                nodeReservations.remove(
                        reservation.taskId()
                );

        if (removed != null) {

            System.out.printf(
                    "[Reservation] Released task=%s node=%s "
                            + "cpu=%d memory=%dMB "
                            + "reservedCpu=%d reservedMemory=%dMB%n",
                    removed.taskId(),
                    reservation.node().info().displayName(),
                    removed.cpuCores(),
                    removed.memoryMb(),
                    reservedCpuCores(nodeId),
                    reservedMemoryMb(nodeId)
            );
        }

        if (nodeReservations.isEmpty()) {
            reservations.remove(nodeId);
        }
    }

    /**
     * Returns the total CPU cores currently reserved on a node.
     *
     * <p>This is Core-owned reservation state, not the node's observed
     * CPU utilization.</p>
     */
    public synchronized int reservedCpuCores(
            String nodeId) {

        return totalsFor(nodeId).cpuCores();
    }

    /**
     * Returns the total memory currently reserved on a node.
     */
    public synchronized long reservedMemoryMb(
            String nodeId) {

        return totalsFor(nodeId).memoryMb();
    }

    /**
     * Returns the number of tasks currently holding a Core reservation on
     * the node.
     *
     * <p>This is intentionally not a replacement for
     * ResourceStatus.activeTasks().</p>
     */
    public synchronized int reservationCount(
            String nodeId) {

        Map<String, Allocation> nodeReservations =
                reservations.get(nodeId);

        return nodeReservations == null
                ? 0
                : nodeReservations.size();
    }

    private record Allocation(
            String taskId,
            String nodeId,
            int cpuCores,
            long memoryMb
    ) {}

    private record AllocationTotals(
            int cpuCores,
            long memoryMb
    ) {}

    /**
     * Handle representing one successful placement reservation.
     *
     * <p>The dispatcher owns this handle for the duration of the current
     * execution attempt. Closing it releases the Core-owned reservation.</p>
     */
    public static final class Reservation
            implements AutoCloseable {

        private final PlacementReservationService owner;
        private final NodeRecord node;
        private final Allocation allocation;

        private final AtomicBoolean released =
                new AtomicBoolean(false);

        private Reservation(
                PlacementReservationService owner,
                NodeRecord node,
                Allocation allocation) {

            this.owner = owner;
            this.node = node;
            this.allocation = allocation;
        }

        public NodeRecord node() {
            return node;
        }

        public String nodeId() {
            return allocation.nodeId();
        }

        public String taskId() {
            return allocation.taskId();
        }

        public void release() {

            if (released.compareAndSet(false, true)) {
                owner.release(this);
            }
        }

        @Override
        public void close() {
            release();
        }
    }
}
