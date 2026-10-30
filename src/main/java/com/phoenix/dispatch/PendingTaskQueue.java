// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;

/**
 * Phoenix-level logical work queue, kept separate from TaskQueueService
 * (a transport queue belonging to one pull-based node).
 *
 * <p>Backed by LinkedHashMap: O(1)-average lookup/removal by task ID while
 * preserving strict insertion order for FIFO recovery. Synchronized because
 * the admission thread and the dispatcher execution threads both
 * touch this concurrently.
 *
 * <h2>The waiting window</h2>
 *
 * <p>{@code waitingSince} starts a task's <em>no eligible node</em> window.
 * It must mean exactly that. Two rules keep it honest:
 *
 * <ul>
 *     <li>While the admission loop is attempting a task it
 *         {@link #claim(String) claims} it. The entry keeps its FIFO
 *         position, but the clock is not "waiting" time any more.</li>
 *     <li>When an attempt ends in a retryable failure and the task returns to
 *         waiting ({@link #enqueue(TaskDescriptor, TaskFailureReason)}), a
 *         <em>fresh</em> window starts. Time spent queued at, or executing
 *         on, a node that then failed is not time spent without a node.
 *         Re-evaluations that merely find no node leave the window alone, so
 *         genuinely unplaceable tasks still expire.</li>
 * </ul>
 *
 * <p>Because a fresh window would otherwise let a task bounce between failing
 * nodes forever, each failure-driven return to waiting is counted and a task
 * is {@linkplain #isExhausted(Pending) exhausted} after
 * {@code phoenix.maxRecoveryCycles} of them (default 3).
 */
@Singleton
public class PendingTaskQueue {

    private final Map<String, Pending> waiting = new LinkedHashMap<>();

    private final Clock clock;
    private final Duration maxWait;
    private final int maxFailedCycles;

    @Inject
    public PendingTaskQueue() {
        this(
                Clock.systemUTC(),
                Duration.ofSeconds(
                        Integer.getInteger("phoenix.noNodeWaitSeconds", 60)),
                Integer.getInteger("phoenix.maxRecoveryCycles", 3)
        );
    }

    /**
     * Test seam: deterministic time and limits.
     *
     * @throws IllegalArgumentException if {@code maxWait} is not positive or
     *         {@code maxFailedCycles} is below 1
     */
    public PendingTaskQueue(
            Clock clock,
            Duration maxWait,
            int maxFailedCycles) {

        if (maxWait == null || maxWait.isZero() || maxWait.isNegative()) {
            throw new IllegalArgumentException(
                    "phoenix.noNodeWaitSeconds must be positive, was " + maxWait);
        }

        if (maxFailedCycles < 1) {
            throw new IllegalArgumentException(
                    "phoenix.maxRecoveryCycles must be at least 1, was "
                            + maxFailedCycles);
        }

        this.clock = Objects.requireNonNull(clock, "clock");
        this.maxWait = maxWait;
        this.maxFailedCycles = maxFailedCycles;
    }

    /**
     * Adds to the tail. Re-enqueueing an already-pending task is a no-op for
     * its position and waiting window - it must NOT move to the back and lose
     * its place in line. (If the task was claimed for an attempt, the claim
     * is released.)
     */
    public synchronized void enqueue(TaskDescriptor task) {
        enqueue(task, null);
    }

    /**
     * Returns a task to waiting.
     *
     * @param failure the retryable failure that ended the attempt that
     *        brought the task back, or {@code null} if no attempt ran (the
     *        task was simply evaluated and no node was eligible). A non-null
     *        failure starts a fresh waiting window and is counted against the
     *        task's recovery budget; the task keeps its FIFO position.
     */
    public synchronized void enqueue(
            TaskDescriptor task,
            TaskFailureReason failure) {

        Pending existing = waiting.get(task.id());
        Instant now = clock.instant();

        if (existing == null) {
            waiting.put(
                    task.id(),
                    new Pending(
                            task,
                            now,
                            false,
                            failure == null ? 0 : 1,
                            failure
                    )
            );
            return;
        }

        if (failure == null) {
            // Plain re-evaluation: keep position and window.
            if (existing.inFlight()) {
                waiting.put(task.id(), existing.released());
            }
            return;
        }

        // An attempt ran and failed: fresh window, same place in line.
        waiting.put(
                task.id(),
                new Pending(
                        existing.task(),
                        now,
                        false,
                        existing.failedCycles() + 1,
                        failure
                )
        );
    }

    /**
     * Marks a pending task as being attempted right now. It keeps its FIFO
     * position. Returns false if the task is no longer pending.
     */
    public synchronized boolean claim(String taskId) {

        Pending existing = waiting.get(taskId);

        if (existing == null) {
            return false;
        }

        waiting.put(taskId, existing.claimed());
        return true;
    }

    /** Ends a claim without touching the waiting window (e.g. on error). */
    public synchronized void release(String taskId) {

        Pending existing = waiting.get(taskId);

        if (existing != null && existing.inFlight()) {
            waiting.put(taskId, existing.released());
        }
    }

    public synchronized boolean remove(String taskId) {
        return waiting.remove(taskId) != null;
    }

    /** Oldest pending task, without removing it — what the scheduler
     *  uses to enforce strict FIFO. */
    public synchronized Pending peek() {
        for (Pending pending : waiting.values()) return pending;
        return null;
    }

    public synchronized List<Pending> snapshot() {
        return List.copyOf(waiting.values());
    }

    public boolean isExpired(Pending pending) {
        return Duration.between(pending.waitingSince(), clock.instant())
                .compareTo(maxWait) > 0;
    }

    /**
     * Expiry of the <em>live</em> entry. A {@link Pending} taken from a
     * snapshot goes stale while an attempt runs; this reads the current
     * window instead.
     */
    public synchronized boolean isExpired(String taskId) {

        Pending current = waiting.get(taskId);

        return current != null && isExpired(current);
    }

    /** True once the task has used up its failure-driven recoveries. */
    public boolean isExhausted(Pending pending) {
        return pending.failedCycles() >= maxFailedCycles;
    }

    public synchronized Pending get(String taskId) {
        return waiting.get(taskId);
    }

    public Duration maxWait() { return maxWait; }

    public int maxFailedCycles() { return maxFailedCycles; }

    public synchronized int size() { return waiting.size(); }

    /**
     * @param inFlight     an attempt is running right now
     * @param failedCycles how many times a failed attempt returned the task
     *                     to waiting
     * @param lastFailure  the failure behind the most recent return, if any
     */
    public record Pending(
            TaskDescriptor task,
            Instant waitingSince,
            boolean inFlight,
            int failedCycles,
            TaskFailureReason lastFailure
    ) {

        public Pending(TaskDescriptor task, Instant waitingSince) {
            this(task, waitingSince, false, 0, null);
        }

        Pending claimed() {
            return new Pending(
                    task, waitingSince, true, failedCycles, lastFailure);
        }

        Pending released() {
            return new Pending(
                    task, waitingSince, false, failedCycles, lastFailure);
        }
    }
}
