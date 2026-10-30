// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.UUID;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.model.TaskLifecycleState;
import com.phoenix.model.TaskResult;

/**
 * Core-owned, transport-neutral record of how far each task attempt has
 * progressed.
 *
 * <p>Transports (HTTP pull today, MQTT later) <em>report into</em> this
 * service as they observe lifecycle transitions; Core decides what a timeout
 * means by reading it back. Nothing here is HTTP-specific, and nothing here
 * performs scheduling or node-health policy, mirroring how
 * {@link NodeTelemetryService} is the neutral entry point for telemetry.
 *
 * <h2>Attempts</h2>
 *
 * <p>Every call to {@link #accepted(String, String, AttemptFencing)} starts a new
 * <em>attempt</em> of the task, numbered 1, 2, 3 ... per task id, and returns
 * an {@link Attempt} handle. The handle is the attempt's identity (a fencing
 * token): reports made through it are ignored once a newer attempt has
 * replaced it, so a stale reporter can never corrupt the ledger of a retry.
 *
 * <h2>Reconciliation with node liveness</h2>
 *
 * <p>An attempt is bound to the node it was placed on. When that node is
 * declared OFFLINE, {@link #abandonAttemptsOn(String)} moves every
 * non-terminal attempt on it to {@link TaskLifecycleState#ABANDONED} and runs
 * the cancellation hooks the owning transport registered with
 * {@link Attempt#onAbandon(Runnable)}, which wake the thread blocked in that
 * transport so recovery can begin immediately instead of at the next timeout.
 *
 * <p>Transitions never move backwards and terminal states are final, so late
 * or duplicate reports are ignored.
 */
@Singleton
public class TaskLifecycleService {

    /**
     * Bounds the ledger the same cheap way the rest of the prototype stays
     * bounded. Pruned tasks restart their attempt numbering, which is safe
     * because it takes thousands of intervening tasks to reach it.
     */
    private static final int PRUNE_THRESHOLD = 4096;

    private final Clock clock;
    private final Supplier<String> attemptIds;
    private final Map<String, Entry> attempts =
            new ConcurrentHashMap<>();

    @Inject
    public TaskLifecycleService() {
        this(Clock.systemUTC());
    }

    /** Test seam: deterministic time. */
    public TaskLifecycleService(Clock clock) {
        this(clock, () -> UUID.randomUUID().toString());
    }

    /**
     * Test seam: deterministic time and attempt ids.
     *
     * @param attemptIds source of opaque attempt ids; must never repeat
     */
    public TaskLifecycleService(
            Clock clock,
            Supplier<String> attemptIds) {

        this.clock = Objects.requireNonNull(clock, "clock");
        this.attemptIds = Objects.requireNonNull(attemptIds, "attemptIds");
    }

    /**
     * The transport took responsibility for the task; starts a new attempt
     * bound to {@code nodeId}.
     *
     * @param nodeId the node the attempt was placed on; required, because
     *        an attempt that is not bound to a node can never be reconciled
     *        by node liveness
     */
    public Attempt accepted(
            String taskId,
            String nodeId,
            AttemptFencing fencing) {

        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(fencing, "fencing");

        if (attempts.size() > PRUNE_THRESHOLD) {
            attempts.values().removeIf(
                    entry -> entry.snapshot().state().isTerminal()
            );
        }

        Instant now = clock.instant();
        String attemptId = requireUsable(attemptIds.get());
        Attempt[] superseded = new Attempt[1];

        Entry created = attempts.compute(
                taskId,
                (ignored, old) -> {

                    int number =
                            old == null
                                    ? 1
                                    : old.attempt().number() + 1;

                    if (old != null
                            && !old.snapshot().state().isTerminal()) {
                        superseded[0] = old.attempt();
                    }

                    return new Entry(
                            new Snapshot(
                                    taskId,
                                    TaskLifecycleState.ACCEPTED,
                                    now,
                                    null,
                                    null,
                                    null,
                                    number,
                                    attemptId,
                                    fencing,
                                    nodeId
                            ),
                            new Attempt(
                                    taskId,
                                    number,
                                    attemptId,
                                    fencing,
                                    nodeId)
                    );
                }
        );

        // A new attempt replaced one that never reached a terminal state:
        // release whatever that older attempt still holds.
        if (superseded[0] != null) {
            superseded[0].abandon();
        }

        System.out.printf(
                "[Lifecycle] %s ACCEPTED (attempt %d, id %s, fencing %s)%n",
                taskId,
                created.attempt().number(),
                created.attempt().shortId(),
                fencing
        );

        return created.attempt();
    }

    /** The task reached the node; ignored if {@code attempt} is stale. */
    public void delivered(Attempt attempt) {
        advance(attempt.taskId(), attempt, TaskLifecycleState.DELIVERED);
    }

    public void started(Attempt attempt) {
        advance(attempt.taskId(), attempt, TaskLifecycleState.STARTED);
    }

    public void completed(Attempt attempt) {
        advance(attempt.taskId(), attempt, TaskLifecycleState.COMPLETED);
    }

    public void failed(Attempt attempt) {
        advance(attempt.taskId(), attempt, TaskLifecycleState.FAILED);
    }

    /**
     * The node reported a result for {@code attempt}.
     *
     * <p>This is the single place that decides how a node-reported result
     * maps onto the ledger, so every transport (pull, push, and the planned
     * MQTT one) ends an attempt identically: a successful result completes
     * it, anything else fails it. Transports must use this instead of
     * choosing between {@link #completed(Attempt)} and
     * {@link #failed(Attempt)} themselves.
     */
    public void reported(Attempt attempt, TaskResult result) {

        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(result, "result");

        if (result.isSuccess()) {
            completed(attempt);
        } else {
            failed(attempt);
        }
    }

    /**
     * Abandons every non-terminal attempt that is bound to {@code nodeId}.
     *
     * <p>Called when the node is declared OFFLINE. Each affected attempt moves
     * to {@link TaskLifecycleState#ABANDONED}, then its cancellation hooks
     * run (outside any lock) so the blocked transport thread wakes up.
     *
     * @return the abandoned attempts as they were at the moment of
     *         abandonment, in no particular order
     */
    public List<Snapshot> abandonAttemptsOn(String nodeId) {

        Objects.requireNonNull(nodeId, "nodeId");

        Instant now = clock.instant();

        List<Snapshot> abandoned = new ArrayList<>();
        List<Attempt> toRelease = new ArrayList<>();

        for (String taskId : List.copyOf(attempts.keySet())) {

            attempts.computeIfPresent(
                    taskId,
                    (ignored, entry) -> {

                        if (entry.snapshot().state().isTerminal()
                                || !nodeId.equals(
                                        entry.attempt().nodeId())) {
                            return entry;
                        }

                        Snapshot next =
                                entry.snapshot().advancedTo(
                                        TaskLifecycleState.ABANDONED,
                                        now
                                );

                        abandoned.add(next);
                        toRelease.add(entry.attempt());

                        return new Entry(next, entry.attempt());
                    }
            );
        }

        // The state change is visible before any hook runs, so a woken
        // transport can already observe Attempt#isAbandoned().
        toRelease.forEach(Attempt::abandon);

        return abandoned;
    }

    public Optional<Snapshot> snapshot(String taskId) {
        Entry entry = attempts.get(taskId);

        return entry == null
                ? Optional.empty()
                : Optional.of(entry.snapshot());
    }

    /** The current attempt handle for a task, if one exists. */
    public Optional<Attempt> currentAttempt(String taskId) {
        Entry entry = attempts.get(taskId);

        return entry == null
                ? Optional.empty()
                : Optional.of(entry.attempt());
    }

    public void forget(String taskId) {
        attempts.remove(taskId);
    }

    /** An id is the identity: a null or blank one would fence nothing. */
    private static String requireUsable(String attemptId) {

        Objects.requireNonNull(attemptId, "attempt id");

        if (attemptId.isBlank()) {
            throw new IllegalStateException(
                    "attempt id supplier returned a blank id");
        }

        return attemptId;
    }

    private void advance(
            String taskId,
            Attempt expected,
            TaskLifecycleState target) {

        Instant now = clock.instant();

        // computeIfPresent is atomic per key, so concurrent reports
        // (poll thread vs. result thread) cannot interleave a transition.
        TaskLifecycleState[] before = new TaskLifecycleState[1];
        TaskLifecycleState[] after = new TaskLifecycleState[1];
        int[] number = new int[1];

        attempts.computeIfPresent(
                taskId,
                (ignored, current) -> {

                    // A report from an attempt that has since been replaced.
                    if (expected != null
                            && current.attempt() != expected) {
                        return current;
                    }

                    before[0] = current.snapshot().state();
                    Snapshot next =
                            current.snapshot().advancedTo(target, now);
                    after[0] = next.state();
                    number[0] = next.attempt();

                    return next == current.snapshot()
                            ? current
                            : new Entry(next, current.attempt());
                }
        );

        if (before[0] != null && before[0] != after[0]) {
            System.out.printf(
                    "[Lifecycle] %s %s -> %s%n",
                    taskId,
                    before[0],
                    after[0]
            );
        }
    }

    private record Entry(
            Snapshot snapshot,
            Attempt attempt
    ) {}

    /**
     * Identity of one execution attempt, handed to the transport that runs it.
     *
     * <p>Besides acting as a fencing token for ledger reports, it is the
     * channel through which Core cancels a blocked transport: the transport
     * registers a hook with {@link #onAbandon(Runnable)} that releases
     * whatever it is waiting on.
     */
    public static final class Attempt {

        private final String taskId;
        private final int number;
        private final String id;
        private final AttemptFencing fencing;
        private final String nodeId;

        private final List<Runnable> hooks = new ArrayList<>();
        private volatile boolean abandoned;

        private Attempt(
                String taskId,
                int number,
                String id,
                AttemptFencing fencing,
                String nodeId) {

            this.taskId = taskId;
            this.number = number;
            this.fencing = fencing;
            this.id = Objects.requireNonNull(id, "attempt id");
            this.nodeId = nodeId;
        }

        public String taskId() {
            return taskId;
        }

        /** 1 for the first attempt of a task, 2 for the first retry, ... */
        public int number() {
            return number;
        }

        /**
         * Opaque, globally unique identity of this attempt.
         *
         * <p>This, not {@link #number()}, is what goes on the wire and what
         * a node echoes back. Task ids restart at {@code task-001} every time
         * Core starts and attempt numbers restart at 1 for every new task, so
         * neither is unique across Core runs; the id is.
         */
        public String id() {
            return id;
        }

        /** Whether reports for this attempt must echo {@link #id()}. */
        public AttemptFencing fencing() {
            return fencing;
        }

        /** First 8 characters of {@link #id()}, for log lines only. */
        public String shortId() {
            return id.length() <= 8 ? id : id.substring(0, 8);
        }

        /** The node this attempt was placed on. */
        public String nodeId() {
            return nodeId;
        }

        /** True once Core stopped waiting for this attempt. */
        public boolean isAbandoned() {
            return abandoned;
        }

        /**
         * Registers a cancellation hook. It runs at most once, on whichever
         * thread abandons the attempt; if the attempt is already abandoned it
         * runs immediately on the caller's thread. Hooks must be quick and
         * must not throw.
         */
        public void onAbandon(Runnable hook) {

            boolean runNow;

            synchronized (this) {
                runNow = abandoned;

                if (!runNow) {
                    hooks.add(hook);
                }
            }

            if (runNow) {
                run(hook);
            }
        }

        private void abandon() {

            List<Runnable> toRun;

            synchronized (this) {
                if (abandoned) {
                    return;
                }

                abandoned = true;
                toRun = List.copyOf(hooks);
                hooks.clear();
            }

            toRun.forEach(Attempt::run);
        }

        private static void run(Runnable hook) {
            try {
                hook.run();
            } catch (RuntimeException exception) {
                System.err.println(
                        "[Lifecycle] abandon hook failed: " + exception
                );
            }
        }
    }

    /**
     * Immutable view of one task attempt's lifecycle.
     *
     * <p>Timestamps are {@code null} until the corresponding transition has
     * been observed, which is how Core tells "delivered but never started"
     * apart from "never delivered".
     *
     * @param attempt   attempt number (1 = first attempt); informational
     * @param attemptId opaque unique id of the attempt
     * @param fencing   whether reports must echo the id
     * @param nodeId  node the attempt was placed on
     */
    public record Snapshot(
            String taskId,
            TaskLifecycleState state,
            Instant acceptedAt,
            Instant deliveredAt,
            Instant startedAt,
            Instant finishedAt,
            int attempt,
            String attemptId,
            AttemptFencing fencing,
            String nodeId
    ) {

        /** True once the task is known to have reached the node. */
        public boolean reachedNode() {
            return deliveredAt != null;
        }

        /** True once the node acknowledged that execution began. */
        public boolean startAcknowledged() {
            return startedAt != null;
        }

        Snapshot advancedTo(
                TaskLifecycleState target,
                Instant now) {

            // Terminal is final; transitions never move backwards.
            if (state.isTerminal()
                    || target.ordinal() <= state.ordinal()) {
                return this;
            }

            Instant delivered = deliveredAt;
            Instant started = startedAt;

            // DELIVERED, STARTED and COMPLETED all mean the task reached the
            // node. FAILED and ABANDONED imply nothing: that is exactly the
            // information they are meant to preserve.
            if (target != TaskLifecycleState.FAILED
                    && target != TaskLifecycleState.ABANDONED
                    && delivered == null) {
                delivered = now;
            }

            if (target == TaskLifecycleState.STARTED) {
                started = now;
            }

            Instant finished =
                    target.isTerminal() ? now : finishedAt;

            return new Snapshot(
                    taskId,
                    target,
                    acceptedAt,
                    delivered,
                    started,
                    finished,
                    attempt,
                    attemptId,
                    fencing,
                    nodeId
            );
        }
    }
}
