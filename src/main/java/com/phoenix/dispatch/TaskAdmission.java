// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;
import com.phoenix.tui.TaskJournal;

/**
 * Common admission boundary for fresh and recovered tasks.
 *
 * <p>Fresh submissions and recovery attempts both pass through the same
 * ordering boundary before task execution is started.
 *
 * <p>The ordering thread and execution pool deliberately have different
 * responsibilities:
 *
 * <ul>
 *     <li>The ordering thread decides which work gets admitted next.</li>
 *     <li>The execution pool performs the potentially blocking device
 *         interaction.</li>
 * </ul>
 *
 * <p>This distinction is important because {@link TaskDispatcher#replace}
 * performs real device execution synchronously. Running it directly on the
 * single admission thread would serialize all fresh task execution.
 *
 * <p>Therefore:
 *
 * <pre>
 * admission/order decision
 *          |
 *          v
 * execution pool
 *          |
 *          v
 * dispatcher.replace()
 *          |
 *          v
 * real device
 * </pre>
 *
 * <p>Admission policy for recovery remains FIFO-preferred but non-blocking:
 * older pending work is considered first, while an older ineligible task
 * does not prevent younger eligible pending work from being considered.
 *
 * <p>Node reservation and device-side queue/timeout semantics are deliberately
 * outside this class and are tracked as separate issues.
 */
@Singleton
public class TaskAdmission {

    private static final long RECOVERY_INTERVAL_SECONDS = 2;

    private final PendingTaskQueue pendingQueue;
    private final TaskDispatcher dispatcher;
    private final TaskJournal taskJournal;

    /**
     * Single ordering boundary for fresh and recovery admission.
     */
    private ExecutorService orderingThread;

    /**
     * Performs actual potentially blocking task execution.
     */
    private ExecutorService executionPool;

    /**
     * Produces periodic recovery-admission attempts.
     */
    private ScheduledExecutorService recoveryTicker;

    @Inject
    public TaskAdmission(
            PendingTaskQueue pendingQueue,
            TaskDispatcher dispatcher,
            TaskJournal taskJournal) {

        this.pendingQueue = pendingQueue;
        this.dispatcher = dispatcher;
        this.taskJournal = taskJournal;
    }

    /**
     * Starts task admission and recovery processing.
     *
     * <p>Calling start repeatedly is safe.
     */
    public synchronized void start() {

        if (orderingThread != null
                && !orderingThread.isShutdown()) {
            return;
        }

        orderingThread =
                Executors.newSingleThreadExecutor(
                        namedDaemon("phoenix-task-admission")
                );

        executionPool =
                Executors.newCachedThreadPool(
                        namedDaemon("phoenix-task-execution")
                );

        recoveryTicker =
                Executors.newSingleThreadScheduledExecutor(
                        namedDaemon("phoenix-recovery-ticker")
                );

        recoveryTicker.scheduleAtFixedRate(
                this::scheduleRecoveryDrain,
                RECOVERY_INTERVAL_SECONDS,
                RECOVERY_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );
    }

    /**
     * Stops admission, recovery scheduling, and task execution workers.
     *
     * <p>Calling stop repeatedly is safe.
     */
    public synchronized void stop() {

        if (recoveryTicker != null) {
            recoveryTicker.shutdownNow();
            recoveryTicker = null;
        }

        if (orderingThread != null) {
            orderingThread.shutdownNow();
            orderingThread = null;
        }

        if (executionPool != null) {
            executionPool.shutdownNow();
            executionPool = null;
        }
    }

    /**
     * Entry point for newly submitted tasks.
     *
     * <p>Fresh tasks no longer call TaskDispatcher.dispatch() directly.
     * Instead, they enter the common admission boundary.
     *
     * <p>The ordering thread first gives existing pending work its scheduling
     * opportunity. The fresh task's actual execution is then handed to the
     * execution pool so that one slow device cannot serialize later fresh
     * submissions.
     */
    public CompletableFuture<TaskDispatcher.Outcome> submitFresh(
            TaskDescriptor task) {

        taskJournal.submitted(task);

        System.out.printf(
                "[Admission] Task submitted: id=%s type=%s%n",
                task.id(),
                task.type()
        );

        CompletableFuture<TaskDispatcher.Outcome> future =
                new CompletableFuture<>();

        ExecutorService ordering;
        ExecutorService execution;

        synchronized (this) {
            ordering = orderingThread;
            execution = executionPool;
        }

        if (ordering == null
                || ordering.isShutdown()
                || execution == null
                || execution.isShutdown()) {

            future.completeExceptionally(
                    new IllegalStateException(
                            "Task admission is not running"
                    )
            );

            return future;
        }

        try {

            ordering.execute(() -> {

                /*
                 * Older eligible pending work gets its opportunity before
                 * this fresh task starts execution.
                 *
                 * An older ineligible task does not block younger pending
                 * work because drainPending() continues through the snapshot.
                 */
                drainPending();

                try {

                    /*
                     * IMPORTANT:
                     *
                     * Do NOT call dispatcher.replace(task) here.
                     *
                     * replace() waits for the real device. Doing that on
                     * the ordering thread would serialize fresh execution.
                     */
                    execution.execute(() -> {

                        try {

                            future.complete(
                                    dispatcher.replace(task)
                            );

                        } catch (Exception exception) {

                            future.completeExceptionally(
                                    exception
                            );
                        }
                    });

                } catch (RejectedExecutionException exception) {

                    future.completeExceptionally(exception);
                }
            });

        } catch (RejectedExecutionException exception) {

            future.completeExceptionally(exception);
        }

        return future;
    }

    /**
     * Adds a recovery-drain operation to the same ordering boundary used
     * by fresh submissions.
     */
    private void scheduleRecoveryDrain() {

        ExecutorService ordering;

        synchronized (this) {
            ordering = orderingThread;
        }

        if (ordering == null
                || ordering.isShutdown()) {
            return;
        }

        try {

            ordering.execute(
                    this::drainPending
            );

        } catch (RejectedExecutionException ignored) {
            /*
             * Shutdown race. The lifecycle owner is already stopping the
             * admission system, so there is nothing left to recover here.
             */
        }
    }

    /**
     * Attempts all currently pending tasks in FIFO-preferred order.
     *
     * <p>The queue snapshot preserves insertion order.
     *
     * <p>An ineligible task returns WAITING and remains in the queue, while
     * later pending tasks are still considered.
     */
    private void drainPending() {

        for (PendingTaskQueue.Pending pending :
                pendingQueue.snapshot()) {

            String taskId =
                    pending.task().id();

            /*
             * A task that keeps failing attempts must not bounce between
             * nodes forever. Each failed attempt starts a fresh waiting
             * window, so the failure-driven recoveries are budgeted
             * separately.
             */
            if (pendingQueue.isExhausted(pending)) {

                pendingQueue.remove(taskId);

                taskJournal.completed(
                        taskId,
                        new TaskResult(
                                taskId,
                                "FAILED",
                                "Recovery exhausted after "
                                        + pending.failedCycles()
                                        + " failed attempt cycle(s); last failure: "
                                        + pending.lastFailure(),
                                pending.lastFailure() == null
                                        ? TaskFailureReason.NO_ELIGIBLE_NODE
                                        : pending.lastFailure()
                        )
                );

                System.out.printf(
                        "[Recovery] %s gave up: %d failed attempt cycle(s), "
                                + "last failure %s%n",
                        taskId,
                        pending.failedCycles(),
                        pending.lastFailure()
                );

                continue;
            }

            /*
             * The entry stays in the queue (it keeps its FIFO place) but is
             * marked as being attempted, so its waiting clock is not
             * mistaken for time spent without an eligible node.
             */
            if (!pendingQueue.claim(taskId)) {
                continue;
            }

            try {

                /*
                 * Expiry is judged only AFTER a fresh placement attempt.
                 * Recovery execution is synchronous on the admission thread,
                 * so a long attempt earlier in this pass can make younger
                 * pending tasks "expire" without ever having been evaluated.
                 * Declaring "no eligible node" without looking would be
                 * false whenever a node is in fact eligible.
                 *
                 * Recovery execution remains synchronous here.
                 *
                 * This preserves the existing FIFO-preferred recovery
                 * semantics: older pending work gets the first placement
                 * opportunity before younger pending work.
                 */
                TaskDispatcher.Outcome outcome =
                        dispatcher.replace(
                                pending.task()
                        );

                if (outcome
                        instanceof TaskDispatcher.Outcome.Completed completed) {

                    pendingQueue.remove(taskId);

                    System.out.printf(
                            "[Recovery] %s recovered -> %s on %s%n",
                            taskId,
                            completed.result()
                                    .result()
                                    .status(),
                            completed.result()
                                    .nodeDisplayName()
                    );

                } else if (pendingQueue.isExpired(taskId)) {

                    /*
                     * Still no eligible node after a real evaluation, and
                     * the (live) waiting window has run out. Using the live
                     * entry matters: an attempt that just failed has already
                     * started a fresh window.
                     */
                    pendingQueue.remove(taskId);

                    taskJournal.completed(
                            taskId,
                            new TaskResult(
                                    taskId,
                                    "FAILED",
                                    "No eligible node within "
                                            + pendingQueue.maxWait().toSeconds()
                                            + "s",
                                    TaskFailureReason.NO_ELIGIBLE_NODE
                            )
                    );

                    System.out.printf(
                            "[Recovery] %s gave up: no eligible node within %ds%n",
                            taskId,
                            pendingQueue.maxWait().toSeconds()
                    );
                }

                /*
                 * WAITING means the task remains in PendingTaskQueue.
                 *
                 * We deliberately continue to the next pending task.
                 * Therefore an older ineligible task cannot create
                 * head-of-line blocking.
                 */

            } catch (Exception exception) {

                System.out.printf(
                        "[Recovery] Re-placement of %s failed: %s%n",
                        taskId,
                        exception.getMessage()
                );

                /*
                 * Keep the task pending. The next recovery tick can try it
                 * again. Release the claim so it is not stuck "in flight".
                 */
                pendingQueue.release(taskId);
            }
        }
    }

    /**
     * Creates daemon threads with useful names for diagnostics.
     */
    private static ThreadFactory namedDaemon(
            String name) {

        return runnable -> {

            Thread thread =
                    new Thread(
                            runnable,
                            name
                    );

            thread.setDaemon(true);

            return thread;
        };
    }
}
