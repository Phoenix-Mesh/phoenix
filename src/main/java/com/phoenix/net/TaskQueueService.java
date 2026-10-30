// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeoutException;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.core.AttemptAbandonedException;
import com.phoenix.core.AttemptFencing;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskResult;

/**
 * Per-device transport queues for pull-based node agents.
 *
 * <p>This is deliberately not Phoenix's logical work queue. A logical task
 * can leave this queue when a node polls; if execution later fails, recovery
 * is handled by PendingTaskQueue/TaskDispatcher.
 *
 * <p>Issue #28: this class is where the HTTP pull transport can actually
 * <em>observe</em> task lifecycle, so it reports into the transport-neutral
 * {@link TaskLifecycleService}:
 *
 * <pre>
 * submit()          -> ACCEPTED   (queued; has not reached the node)
 * poll() hands task -> DELIVERED  (the node took it)
 * markStarted()     -> STARTED    (optional node acknowledgement)
 * completeResult()  -> COMPLETED
 * </pre>
 *
 * <p>It also remembers when each device last polled, which is the HTTP
 * transport's evidence that a node is still in contact even while a task is
 * waiting to be delivered.
 *
 * <p>In-flight reconciliation: every attempt registers a cancellation hook
 * with its {@link TaskLifecycleService.Attempt}. If Core abandons the attempt
 * because the node went OFFLINE, the hook withdraws the task from the device
 * queue (so a reconnecting node can never pull it) and wakes the thread
 * waiting in {@link #awaitDelivery} / {@link #awaitResult} with an
 * {@link AttemptAbandonedException}. Any later result for the attempt is
 * rejected as stale.
 */
@Singleton
public class TaskQueueService {

    private final Map<String, LinkedBlockingQueue<TaskDescriptor>>
            queuesByDeviceId = new ConcurrentHashMap<>();

    private final Map<String, PendingTask>
            pendingTasks = new ConcurrentHashMap<>();

    private final Map<String, Instant>
            lastPollByDeviceId = new ConcurrentHashMap<>();

    private final TaskLifecycleService lifecycle;
    private final AtomicLong unfencedResults = new AtomicLong();
    private final Clock clock;

    @Inject
    public TaskQueueService(TaskLifecycleService lifecycle) {
        this(lifecycle, Clock.systemUTC());
    }

    /** Test seam: deterministic time. */
    public TaskQueueService(
            TaskLifecycleService lifecycle,
            Clock clock) {

        this.lifecycle = lifecycle;
        this.clock = clock;
    }

    /**
     * Queues a task for a device and starts a new attempt bound to
     * {@code nodeId}.
     *
     * @param fencing how strictly reports for this attempt are fenced; the
     *        caller decides it from what the node announced, and there is
     *        deliberately no default
     */
    public TaskLifecycleService.Attempt submit(
            String nodeId,
            String deviceId,
            TaskDescriptor task,
            AttemptFencing fencing) {

        TaskLifecycleService.Attempt attempt =
                lifecycle.accepted(task.id(), nodeId, fencing);

        // Install the pending state before making the task visible to the agent.
        PendingTask pending = new PendingTask(attempt);
        pendingTasks.put(task.id(), pending);

        attempt.onAbandon(() -> abort(deviceId, pending));

        LinkedBlockingQueue<TaskDescriptor> queue =
                queuesByDeviceId
                        .computeIfAbsent(
                                deviceId,
                                ignored -> new LinkedBlockingQueue<>()
                        );

        /*
         * The node can transition to OFFLINE between accepted() and
         * registering the abandon hook. If that happened, onAbandon() has
         * already fired before this method reaches the queue insertion.
         * Never publish an already-abandoned attempt to the device queue.
         *
         * The post-offer check closes the remaining race where OFFLINE is
         * observed immediately after the first check. If the hook wins, it
         * will also withdraw the entry; if the hook ran before the offer,
         * this check removes the just-added stale entry.
         */
        if (!attempt.isAbandoned()) {
            queue.offer(task);

            if (attempt.isAbandoned()) {
                queue.removeIf(queued -> queued.id().equals(task.id()));
            }
        }

        System.out.printf(
                "[TaskQueueService] queued %s for device %s%s%n",
                task.id(),
                deviceId,
                attempt.isAbandoned()
                        ? " (attempt already abandoned)"
                        : ""
        );

        return attempt;
    }

    /**
     * Records that a device is in contact. Called for every poll, including
     * polls that receive no work (and polls from a node the Core currently
     * refuses to hand work to): a polling node is a reachable node.
     */
    public void recordPoll(String deviceId) {
        lastPollByDeviceId.put(deviceId, clock.instant());
    }

    /** Time since this device last polled; empty if it never has. */
    public Optional<Duration> timeSinceLastPoll(String deviceId) {
        Instant last = lastPollByDeviceId.get(deviceId);

        return last == null
                ? Optional.empty()
                : Optional.of(Duration.between(last, clock.instant()));
    }

    public Optional<TaskDescriptor> poll(
            String deviceId) {

        return pollDelivery(deviceId).map(Delivery::task);
    }

    /**
     * Hands the next deliverable task to a polling device, together with the
     * attempt number the node may echo back.
     *
     * <p>A queued task whose attempt no longer exists is skipped rather than
     * delivered: the node would only execute work whose result Core is going
     * to reject.
     */
    public Optional<Delivery> pollDelivery(
            String deviceId) {

        recordPoll(deviceId);

        LinkedBlockingQueue<TaskDescriptor> queue =
                queuesByDeviceId.get(deviceId);

        if (queue == null) {
            return Optional.empty();
        }

        TaskDescriptor task;

        while ((task = queue.poll()) != null) {

            PendingTask pending = pendingTasks.get(task.id());

            if (pending == null
                    || pending.attempt.isAbandoned()) {
                continue;
            }

            pending.delivered.complete(null);
            lifecycle.delivered(pending.attempt);

            return Optional.of(
                    new Delivery(
                            task,
                            pending.attempt.id(),
                            pending.attempt.number(),
                            pending.attempt.fencing())
            );
        }

        return Optional.empty();
    }

    /**
     * Optional node acknowledgement that execution began. Only meaningful for
     * a task that has already been delivered in its current attempt.
     */
    public boolean markStarted(String taskId) {
        return markStarted(taskId, null);
    }

    /**
     * @param attemptId the attempt id echoed by the node, or {@code null}
     *        if the node does not echo it; any other value that is not the
     *        current attempt's id is rejected as stale
     */
    public boolean markStarted(
            String taskId,
            String attemptId) {

        PendingTask pending = pendingTasks.get(taskId);

        if (pending == null
                || !pending.delivered.isDone()
                || pending.delivered.isCompletedExceptionally()
                || pending.result.isDone()
                || isStaleAttempt(pending, attemptId, false)) {

            return false;
        }

        lifecycle.started(pending.attempt);
        return true;
    }

    /**
     * Remove a task if it is still waiting in a pull-node transport queue.
     * If the node has already pulled it, this returns false and the in-flight
     * execution is allowed to finish harmlessly.
     *
     * <p>Removal is atomic with respect to {@link #poll(String)}: exactly one
     * of them obtains the task, so a {@code true} result guarantees the node
     * never received it.
     */
    public boolean cancelQueuedTask(
            String deviceId,
            String taskId) {

        LinkedBlockingQueue<TaskDescriptor> queue =
                queuesByDeviceId.get(deviceId);

        if (queue == null) {
            return false;
        }

        boolean removed = queue.removeIf(
                task -> task.id().equals(taskId)
        );

        if (removed) {
            pendingTasks.remove(taskId);
            System.out.printf(
                    "[TaskQueueService] removed stale queued task %s from %s%n",
                    taskId,
                    deviceId
            );
        }

        return removed;
    }

    public boolean completeResult(
            TaskResult result) {

        return completeResult(result, null);
    }

    /**
     * Accepts a node-reported result for the current attempt of a task.
     *
     * <p>A result is rejected as stale if there is no pending attempt, the
     * attempt already has a result (or was abandoned), the node echoed an
     * attempt id that is not the current one, or the current attempt has
     * not been delivered to any node yet. The last case matters after
     * recovery: a late result from a previous attempt on another node must
     * not complete a re-queued attempt that no node has even seen.
     *
     * @param attemptId the attempt id echoed by the node, or {@code null}
     *        if the node does not echo it (a legacy agent)
     */
    public boolean completeResult(
            TaskResult result,
            String attemptId) {

        PendingTask pending =
                pendingTasks.get(result.taskId());

        if (pending == null || pending.result.isDone()) {
            System.out.printf(
                    "[TaskQueueService] dropping stale result for %s%n",
                    result.taskId()
            );
            return false;
        }

        if (isStaleAttempt(pending, attemptId, true)) {
            return false;
        }

        if (!pending.delivered.isDone()) {
            System.out.printf(
                    "[TaskQueueService] dropping result for %s: "
                            + "current attempt was never delivered%n",
                    result.taskId()
            );
            return false;
        }

        if (!pending.result.complete(result)) {
            return false;
        }

        lifecycle.reported(pending.attempt, result);
        return true;
    }

    /**
     * Phase 1: wait for the node to take the task.
     *
     * @return true if the task was delivered within the timeout, false if it
     *         is still waiting in the queue (caller decides what that means)
     * @throws AttemptAbandonedException if Core abandoned the attempt because
     *         the node went OFFLINE
     */
    public boolean awaitDelivery(
            String taskId,
            Duration timeout) throws InterruptedException {

        PendingTask pending = requirePending(taskId);

        try {
            pending.delivered.get(
                    timeout.toMillis(),
                    TimeUnit.MILLISECONDS
            );
            return true;

        } catch (TimeoutException exception) {
            return false;

        } catch (ExecutionException exception) {

            if (exception.getCause()
                    instanceof AttemptAbandonedException abandoned) {
                throw abandoned;
            }

            // The delivery signal is only ever completed normally or abandoned.
            throw new IllegalStateException(exception);
        }
    }

    /**
     * Phase 2: wait for the node's result. Call after the task has been
     * delivered; the timeout is therefore an execution timeout.
     *
     * @throws AttemptAbandonedException if Core abandoned the attempt because
     *         the node went OFFLINE
     */
    public TaskResult awaitResult(
            String taskId,
            Duration timeout) throws Exception {

        PendingTask pending = requirePending(taskId);

        try {
            return pending.result.get(
                    timeout.toMillis(),
                    TimeUnit.MILLISECONDS
            );

        } catch (ExecutionException exception) {

            if (exception.getCause()
                    instanceof AttemptAbandonedException abandoned) {
                throw abandoned;
            }

            throw exception;

        } finally {
            pendingTasks.remove(taskId, pending);
        }
    }

    /** Drops any pending state for the task; safe if already gone. */
    public void discard(String taskId) {
        pendingTasks.remove(taskId);
    }

    public int queueLength(String deviceId) {
        LinkedBlockingQueue<TaskDescriptor> queue =
                queuesByDeviceId.get(deviceId);

        return queue == null ? 0 : queue.size();
    }

    /**
     * Cancellation hook run when Core abandons the attempt (node OFFLINE).
     *
     * <p>The pending state is left in place on purpose: the woken waiter
     * needs it, and a late result must find a completed-exceptionally attempt
     * and be rejected rather than find nothing and be mistaken for a task
     * Core never queued.
     */
    private void abort(
            String deviceId,
            PendingTask pending) {

        String taskId = pending.attempt.taskId();

        // Only withdraw the queue entry if it still belongs to this attempt.
        if (pendingTasks.get(taskId) == pending) {

            LinkedBlockingQueue<TaskDescriptor> queue =
                    queuesByDeviceId.get(deviceId);

            if (queue != null
                    && queue.removeIf(task -> task.id().equals(taskId))) {

                System.out.printf(
                        "[TaskQueueService] withdrew abandoned task %s "
                                + "from %s%n",
                        taskId,
                        deviceId
                );
            }
        }

        AttemptAbandonedException abandoned =
                new AttemptAbandonedException(pending.attempt);

        pending.delivered.completeExceptionally(abandoned);
        pending.result.completeExceptionally(abandoned);
    }

    private PendingTask requirePending(String taskId) {

        PendingTask pending = pendingTasks.get(taskId);

        if (pending == null) {
            throw new IllegalStateException(
                    "No pending task: " + taskId
            );
        }

        return pending;
    }

    /**
     * Decides whether a node's report belongs to some other attempt.
     *
     * <ul>
     *     <li>An echoed id that is not the current attempt's: stale, always.
     *         The numeric attempt is never consulted: it is not unique.</li>
     *     <li>No id, current attempt {@link AttemptFencing#ENFORCED}: stale.
     *         The node was told to echo, so a report without one cannot be
     *         shown to belong to this attempt.</li>
     *     <li>No id, current attempt {@link AttemptFencing#LEGACY}: accepted,
     *         but it is <b>not fenced</b>; for results this is logged and
     *         counted so operators can see which agents still need
     *         upgrading.</li>
     * </ul>
     *
     * @param isResult true for {@code /results}, false for {@code /tasks/started}
     */
    private boolean isStaleAttempt(
            PendingTask pending,
            String echoedAttemptId,
            boolean isResult) {

        String taskId = pending.attempt.taskId();

        if (echoedAttemptId != null) {

            if (echoedAttemptId.equals(pending.attempt.id())) {
                return false;
            }

            System.out.printf(
                    "[TaskQueueService] dropping %s for %s: it belongs to "
                            + "attempt id %s but the current attempt is "
                            + "%d (id %s)%n",
                    isResult ? "result" : "start report",
                    taskId,
                    abbreviate(echoedAttemptId),
                    pending.attempt.number(),
                    pending.attempt.shortId()
            );
            return true;
        }

        if (pending.attempt.fencing() == AttemptFencing.ENFORCED) {
            System.out.printf(
                    "[TaskQueueService] dropping %s for %s: node %s "
                            + "negotiated attempt-id fencing but the report "
                            + "carries no attemptId (current attempt %d, "
                            + "id %s)%n",
                    isResult ? "result" : "start report",
                    taskId,
                    pending.attempt.nodeId(),
                    pending.attempt.number(),
                    pending.attempt.shortId()
            );
            return true;
        }

        if (isResult) {
            unfencedResults.incrementAndGet();

            System.out.printf(
                    "[TaskQueueService] WARNING accepted UNFENCED result "
                            + "for %s from legacy node %s (no attemptId): a "
                            + "late result from an abandoned attempt could "
                            + "not be told apart%n",
                    taskId,
                    pending.attempt.nodeId()
            );
        }

        return false;
    }

    /**
     * Results accepted without an attempt id because the current attempt was
     * {@link AttemptFencing#LEGACY}. Non-zero means some agent still needs
     * the {@code attempt-id} feature.
     */
    public long unfencedResultCount() {
        return unfencedResults.get();
    }

    /** Bounds untrusted, node-supplied text before it reaches the log. */
    private static String abbreviate(String untrusted) {
        String clean = untrusted.replaceAll("[\\r\\n]", " ");
        return clean.length() <= 36 ? clean : clean.substring(0, 36) + "...";
    }

    /**
     * A task handed to a polling node.
     *
     * @param attemptId     opaque unique id of the attempt (the identity)
     * @param attemptNumber 1, 2, 3 ... informational only
     * @param fencing       whether the id is sent to, and required back from,
     *                      the node
     */
    public record Delivery(
            TaskDescriptor task,
            String attemptId,
            int attemptNumber,
            AttemptFencing fencing
    ) {}

    /** State of one task attempt inside the pull transport. */
    private static final class PendingTask {

        final TaskLifecycleService.Attempt attempt;

        /** Completed when a node takes the task off the queue. */
        final CompletableFuture<Void> delivered =
                new CompletableFuture<>();

        /** Completed when the node reports a result. */
        final CompletableFuture<TaskResult> result =
                new CompletableFuture<>();

        PendingTask(TaskLifecycleService.Attempt attempt) {
            this.attempt = attempt;
        }
    }
}
