// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.core.AttemptAbandonedException;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskLifecycleState;
import com.phoenix.model.TaskResult;

/**
 * Correlates node reports with the attempts Core is waiting on.
 *
 * <p>Every MQTT attempt is fenced: Core sends the attempt id with the task and
 * a report counts only if it names a pending attempt <em>and</em> arrives on
 * the topic of the device that attempt was sent to <em>and</em> names the
 * same task. A late report from an earlier attempt, from another device, or
 * for an attempt Core abandoned is dropped. There is no legacy, unfenced mode:
 * the MQTT protocol requires agents to echo the attempt id.
 *
 * <p>Duplicates are expected (QoS 1 is at-least-once) and harmless: the first
 * acknowledgement, start and result count, later copies change nothing.
 *
 * <p>Like the HTTP pull transport, a report that implies an earlier step
 * implies it here too: a start or a result means the task was delivered, even
 * if the acknowledgement was lost or arrives later.
 *
 * <p>Every transition is written to the lifecycle ledger <em>before</em> the
 * waiting transport thread is woken, so whoever acts on a returned result
 * already sees the matching terminal state. A result is handed to the waiter
 * only if the ledger actually holds it; if abandonment won the race, the
 * attempt stays abandoned and the waiter is told so, never given a result
 * the ledger disowns.
 */
@Singleton
public class MqttAttempts {

    private final TaskLifecycleService lifecycle;
    private final Map<String, Pending> byAttemptId = new ConcurrentHashMap<>();
    private final AtomicLong droppedReports = new AtomicLong();

    @Inject
    public MqttAttempts(TaskLifecycleService lifecycle) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
    }

    /** One attempt waiting for its node. */
    public static final class Pending {

        final String deviceId;
        final TaskLifecycleService.Attempt attempt;

        /** Completed when the node acknowledges, starts, or reports. */
        final CompletableFuture<Void> delivered = new CompletableFuture<>();

        /** Completed when the node reports a result. */
        final CompletableFuture<TaskResult> result = new CompletableFuture<>();

        /** Claimed by the first delivery signal and the first result. */
        private final AtomicBoolean deliveryClaimed = new AtomicBoolean();
        private final AtomicBoolean resultClaimed = new AtomicBoolean();

        Pending(String deviceId, TaskLifecycleService.Attempt attempt) {
            this.deviceId = deviceId;
            this.attempt = attempt;
        }

        /** Wakes every waiter with {@link AttemptAbandonedException}. */
        void abandon() {
            AttemptAbandonedException abandoned = new AttemptAbandonedException(attempt);
            delivered.completeExceptionally(abandoned);
            result.completeExceptionally(abandoned);
        }
    }

    /** Starts waiting for reports about {@code attempt} from {@code deviceId}. */
    public Pending track(String deviceId, TaskLifecycleService.Attempt attempt) {

        Pending pending = new Pending(deviceId, attempt);
        byAttemptId.put(attempt.id(), pending);

        // Registered after the entry exists, so an abandonment that happens
        // in between still finds and wakes it (the hook runs immediately).
        attempt.onAbandon(pending::abandon);
        return pending;
    }

    /** Stops waiting; later reports for the attempt are dropped. */
    public void forget(Pending pending) {
        byAttemptId.remove(pending.attempt.id(), pending);
    }

    /** {@code up/ack}: the node received the task. */
    public boolean acknowledged(String deviceId, MqttWire.AttemptReport report) {
        return match(deviceId, report.taskId, report.attemptId, "ack")
                .map(pending -> {
                    markDelivered(pending);
                    return true;
                })
                .orElse(false);
    }

    /** {@code up/started}: the node began executing. */
    public boolean started(String deviceId, MqttWire.AttemptReport report) {
        return match(deviceId, report.taskId, report.attemptId, "start report")
                .map(pending -> {
                    markDelivered(pending);
                    lifecycle.started(pending.attempt);
                    return true;
                })
                .orElse(false);
    }

    /** {@code up/result}: the node finished. Only the first result counts. */
    public boolean resulted(String deviceId, MqttWire.Result report) {

        Optional<Pending> match =
                match(deviceId, report.taskId, report.attemptId, "result");

        if (match.isEmpty()) {
            return false;
        }

        Pending pending = match.get();

        if (report.status == null || report.status.isBlank()) {
            reject("result", report.taskId, "it has no status");
            return false;
        }

        boolean success = "SUCCESS".equalsIgnoreCase(report.status);

        TaskResult result = new TaskResult(
                report.taskId,
                report.status,
                report.outputData,
                success ? TaskFailureReason.NONE : TaskFailureReason.EXECUTION_ERROR);

        // A duplicate changes nothing, so it is turned away before it can
        // touch any bookkeeping.
        if (!pending.resultClaimed.compareAndSet(false, true)) {
            return false;
        }

        markDelivered(pending);

        // Ledger first, so whoever acts on the result already sees the
        // matching terminal state.
        lifecycle.reported(pending.attempt, result);

        // The ledger ignores a report once the attempt is terminal, and
        // abandonment writes the ledger a moment before it marks the attempt
        // and wakes the waiter. A result that lost that race must not be
        // returned as if it had been accepted.
        if (!ledgerRecorded(pending, result.isSuccess())) {
            pending.abandon();
            return false;
        }

        return pending.result.complete(result);
    }

    /** Reports dropped as stale, misdirected or malformed. */
    public long droppedReportCount() {
        return droppedReports.get();
    }

    /** Attempts currently waiting. */
    public int pendingCount() {
        return byAttemptId.size();
    }

    /**
     * True if the ledger holds this attempt's result as the node reported it.
     * Terminal states are final, so once true it stays true.
     */
    private boolean ledgerRecorded(Pending pending, boolean success) {

        TaskLifecycleState expected =
                success ? TaskLifecycleState.COMPLETED : TaskLifecycleState.FAILED;

        return lifecycle.snapshot(pending.attempt.taskId())
                .filter(snapshot -> pending.attempt.id().equals(snapshot.attemptId()))
                .map(snapshot -> snapshot.state() == expected)
                .orElse(false);
    }

    private void markDelivered(Pending pending) {
        if (pending.deliveryClaimed.compareAndSet(false, true)) {
            lifecycle.delivered(pending.attempt);
            pending.delivered.complete(null);
        }
    }

    private Optional<Pending> match(
            String deviceId,
            String taskId,
            String attemptId,
            String what) {

        if (attemptId == null || attemptId.isBlank() || taskId == null) {
            return reject(what, taskId, "it carries no taskId/attemptId");
        }

        Pending pending = byAttemptId.get(attemptId);

        if (pending == null) {
            return reject(what, taskId,
                    "attempt " + abbreviate(attemptId) + " is not pending");
        }

        if (!pending.deviceId.equals(deviceId)) {
            return reject(what, taskId,
                    "it came from device " + deviceId + " but the attempt was sent to "
                            + pending.deviceId);
        }

        if (!pending.attempt.taskId().equals(taskId)) {
            return reject(what, taskId,
                    "attempt " + pending.attempt.shortId() + " belongs to "
                            + pending.attempt.taskId());
        }

        if (pending.attempt.isAbandoned()) {
            return reject(what, taskId,
                    "attempt " + pending.attempt.shortId() + " was abandoned");
        }

        return Optional.of(pending);
    }

    private Optional<Pending> reject(String what, String taskId, String why) {
        droppedReports.incrementAndGet();
        System.out.printf(
                "[MQTT] Dropping %s for %s: %s%n",
                what,
                taskId == null ? "?" : abbreviate(taskId),
                why);
        return Optional.empty();
    }

    /** Bounds untrusted, node-supplied text before it reaches the log. */
    static String abbreviate(String untrusted) {
        String clean = untrusted.replaceAll("[\\p{Cntrl}]", " ");
        return clean.length() <= 40 ? clean : clean.substring(0, 40) + "...";
    }
}
