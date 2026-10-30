// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.adapter.IHardwareAdapter;
import com.phoenix.core.AdapterRegistry;
import com.phoenix.core.DeviceRegistry;
import com.phoenix.core.PlacementReservationService;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.NodeStatus;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;
import com.phoenix.recovery.TaskRecoveryManager;
import com.phoenix.tui.TaskJournal;

@Singleton
public class TaskDispatcher {

    private final DeviceRegistry registry;
    private final AdapterRegistry adapterRegistry;
    private final TaskJournal taskJournal;
    private final TaskRecoveryManager recoveryManager;
    private final PendingTaskQueue pendingQueue;
    private final PlacementReservationService reservationService;

    @Inject
    public TaskDispatcher(
            DeviceRegistry registry,
            AdapterRegistry adapterRegistry,
            TaskJournal taskJournal,
            TaskRecoveryManager recoveryManager,
            PendingTaskQueue pendingQueue,
            PlacementReservationService reservationService) {

        this.registry = registry;
        this.adapterRegistry = adapterRegistry;
        this.taskJournal = taskJournal;
        this.recoveryManager = recoveryManager;
        this.pendingQueue = pendingQueue;
        this.reservationService = reservationService;
    }

    /**
     * Runs one placement cycle for a task, fresh or recovered.
     *
     * <p>Up to {@link TaskRecoveryManager#maxAttempts()} nodes are tried.
     * The cycle either completes the task or returns it to
     * {@link PendingTaskQueue}; waiting is normal control flow, not an
     * exception. Every path goes through {@link TaskAdmission}, so fresh and
     * recovered tasks share the same ordering and reservation semantics.
     */
    public Outcome replace(TaskDescriptor task) {
        return place(task);
    }

    private Outcome place(TaskDescriptor task) {

        Set<String> excludedNodeIds =
                new HashSet<>();

        DispatchResult lastResult =
                null;

        for (int attempt = 1;
             attempt <= recoveryManager.maxAttempts();
             attempt++) {

            /*
             * Selection and reservation are one Core-side operation.
             *
             * The reservation service:
             *   1. obtains current node snapshots,
             *   2. removes nodes without reservable capacity,
             *   3. invokes the existing scheduler,
             *   4. re-reads the selected node,
             *   5. commits the reservation atomically.
             */
            Optional<PlacementReservationService.Reservation>
                    reserved =
                    reservationService.selectAndReserve(
                            task,
                            excludedNodeIds
                    );

            if (reserved.isEmpty()) {

                if (lastResult != null
                        && recoveryManager.isRetryable(
                                lastResult.result())) {

                    taskJournal.waiting(
                            task.id(),
                            lastResult.result()
                                    .failureReason()
                                    .name()
                    );

                    /*
                     * An attempt ran and failed retryably: the task returns
                     * to waiting with a FRESH waiting window and keeps its
                     * FIFO position. The time spent at the node that failed
                     * is not time spent without an eligible node.
                     */
                    pendingQueue.enqueue(
                            task,
                            lastResult.result().failureReason()
                    );

                    System.out.printf(
                            "[Recovery] %s WAITING: "
                                    + "no eligible alternative "
                                    + "after %d attempt(s)%n",
                            task.id(),
                            attempt - 1
                    );

                    return new Outcome.Waiting(
                            task.id()
                    );
                }

                taskJournal.waiting(
                        task.id(),
                        TaskFailureReason.NO_ELIGIBLE_NODE.name()
                );

                pendingQueue.enqueue(task);

                System.out.printf(
                        "[Dispatcher] %s WAITING: "
                                + "no eligible node "
                                + "(will retry for %ds)%n",
                        task.id(),
                        pendingQueue.maxWait().toSeconds()
                );

                return new Outcome.Waiting(
                        task.id()
                );
            }

            /*
             * The reservation remains held for the entire current
             * execution attempt.
             *
             * try-with-resources guarantees release on:
             *   - success
             *   - retryable failure
             *   - non-retryable failure
             *   - adapter exception
             *   - timeout result
             *   - interruption/other RuntimeException
             *
             * The adapter call is synchronous and the reservation covers
             * both the delivery and the execution phase of the attempt.
             */
            try (PlacementReservationService.Reservation reservation =
                         reserved.get()) {

                NodeRecord node =
                        reservation.node();

                String nodeId =
                        node.nodeId();

                String nodeName =
                        node.info().displayName();

                taskJournal.assigned(
                        task.id(),
                        nodeName
                );

                NodeExecutionTarget target =
                        new NodeExecutionTarget(
                                nodeId,
                                nodeName,
                                node.info().adapterType()
                        );

                if (attempt == 1) {

                    System.out.printf(
                            "[Dispatcher] Placing on adapter '%s' "
                                    + "for node %s (id=%s)%n",
                            target.adapterType(),
                            target.displayName(),
                            target.nodeId()
                    );

                } else {

                    System.out.printf(
                            "[Recovery] Retry %d/%d: "
                                    + "reassigning %s to %s (id=%s)%n",
                            attempt,
                            recoveryManager.maxAttempts(),
                            task.id(),
                            target.displayName(),
                            target.nodeId()
                    );
                }

                IHardwareAdapter adapter = null;
                TaskResult result;

                try {

                    /*
                     * Resolved inside the guard: a node that registered with
                     * an adapter type Core does not know must fail this
                     * attempt, not escape the placement cycle.
                     */
                    adapter =
                            adapterRegistry.resolve(
                                    target.adapterType()
                            );

                    result =
                            adapter.executeTask(
                                    task,
                                    target
                            );

                } catch (Exception exception) {

                    result =
                            new TaskResult(
                                    task.id(),
                                    "FAILED",
                                    "Adapter execution failed: "
                                            + exception.getMessage(),
                                    TaskFailureReason.TRANSPORT_ERROR
                            );
                }

                if (adapter != null) {
                    adapter.reportResult(result);
                }

                DispatchResult dispatchResult =
                        new DispatchResult(
                                nodeId,
                                nodeName,
                                result
                        );

                lastResult =
                        dispatchResult;

                if (result.isSuccess()) {

                    pendingQueue.remove(
                            task.id()
                    );

                    taskJournal.completed(
                            task.id(),
                            result
                    );

                    System.out.printf(
                            "[Dispatcher] Task completed: status=%s%n",
                            result.status()
                    );

                    return new Outcome.Completed(
                            dispatchResult
                    );
                }

                System.out.printf(
                        "[Recovery] Task %s failed on %s reason=%s%n",
                        task.id(),
                        nodeName,
                        result.failureReason()
                );

                if (!recoveryManager.isRetryable(result)) {

                    taskJournal.completed(
                            task.id(),
                            result
                    );

                    return new Outcome.Completed(
                            dispatchResult
                    );
                }

                /*
                 * Reservation has not been released yet here.
                 *
                 * It is released automatically when this try-with-resources
                 * block exits, before the next recovery attempt can begin.
                 */
                excludedNodeIds.add(
                        nodeId
                );

                /*
                 * Issue #28: re-placement and node-health penalty are
                 * separate decisions.
                 *
                 * Every retryable failure re-places the task, but only
                 * failures that implicate the node degrade it. A
                 * DELIVERY_TIMEOUT means the task sat in the transport
                 * queue of a node that is still in contact; that is
                 * ordinary backlog, not evidence the node has failed.
                 * Genuinely unreachable or silent nodes (NODE_UNREACHABLE,
                 * EXECUTION_TIMEOUT, TIMEOUT, ...) are still degraded
                 * exactly as before.
                 */
                if (result.implicatesNodeHealth()) {

                    registry.get(nodeId).ifPresent(current -> {

                        if (current.state()
                                == NodeStatus.ONLINE) {

                            System.out.printf(
                                    "[Recovery] Node %s failed "
                                            + "task %s -> DEGRADED%n",
                                    nodeName,
                                    task.id()
                            );

                            registry.markDegraded(
                                    nodeId
                            );
                        }
                    });

                } else {

                    System.out.printf(
                            "[Recovery] Task %s attempt on %s ended "
                                    + "(reason=%s); node health unaffected%n",
                            task.id(),
                            nodeName,
                            result.failureReason()
                    );
                }

                if (attempt
                        < recoveryManager.maxAttempts()) {

                    taskJournal.retrying(
                            task.id(),
                            nodeName,
                            attempt + 1,
                            result.failureReason().name()
                    );
                }
            }
        }

        if (lastResult != null) {

            if (recoveryManager.isRetryable(lastResult.result())) {
                TaskFailureReason failure =
                        lastResult.result().failureReason();

                taskJournal.waiting(
                        task.id(),
                        failure.name()
                );

                /*
                 * Every placement attempt in this call failed retryably.
                 * The logical task must return to the common pending path,
                 * not be reported as terminal merely because this one
                 * placement cycle exhausted its alternative nodes.
                 *
                 * A non-null failure starts a fresh waiting window and
                 * consumes one recovery cycle, while preserving FIFO
                 * position in PendingTaskQueue.
                 */
                pendingQueue.enqueue(task, failure);

                System.out.printf(
                        "[Recovery] %s WAITING: exhausted %d retryable "
                                + "placement attempt(s); last failure=%s%n",
                        task.id(),
                        recoveryManager.maxAttempts(),
                        failure
                );

                return new Outcome.Waiting(
                        task.id()
                );
            }

            taskJournal.completed(
                    task.id(),
                    lastResult.result()
            );

            System.out.printf(
                    "[Recovery] Task %s exhausted %d attempt(s) with "
                            + "non-retryable failure%n",
                    task.id(),
                    recoveryManager.maxAttempts()
            );

            return new Outcome.Completed(
                    lastResult
            );
        }

        taskJournal.waiting(
                task.id(),
                TaskFailureReason.NO_ELIGIBLE_NODE.name()
        );

        pendingQueue.enqueue(task);

        return new Outcome.Waiting(
                task.id()
        );
    }

    public sealed interface Outcome
            permits Outcome.Completed, Outcome.Waiting {

        record Completed(
                DispatchResult result
        ) implements Outcome {}

        record Waiting(
                String taskId
        ) implements Outcome {}
    }
}
