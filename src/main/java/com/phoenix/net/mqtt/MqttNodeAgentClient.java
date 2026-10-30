// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.core.AttemptAbandonedException;
import com.phoenix.core.AttemptFencing;
import com.phoenix.core.NodeAttachments;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.core.TaskTimeoutPolicy;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;
import com.phoenix.net.NodeAgentClient;
import com.phoenix.net.TaskWire;

/**
 * MQTT implementation of the transport-neutral {@link NodeAgentClient}
 * contract.
 *
 * <p>Core publishes the task on the node's {@code down/task} topic. MQTT
 * gives the publisher no proof that the node received it (a QoS 1
 * acknowledgement comes from the broker), so the node acknowledges at the
 * application level on {@code up/ack}. That acknowledgement is the delivery
 * boundary, which lets MQTT keep the same two-phase timeout semantics as the
 * HTTP pull transport:
 *
 * <pre>
 * publish task -&gt; await ack (deliveryTimeout)
 *                   |-- Core not connected to the broker -&gt; TRANSPORT_UNAVAILABLE
 *                   |-- no ack, node still in contact     -&gt; DELIVERY_TIMEOUT
 *                   |-- no ack, node silent                -&gt; NODE_UNREACHABLE
 *                   '-- ack -&gt; await result (executionTimeout)
 *                                '-- no result           -&gt; EXECUTION_TIMEOUT
 * at any point: node declared OFFLINE                       -&gt; NODE_OFFLINE
 * </pre>
 *
 * <p>Every attempt is {@linkplain AttemptFencing#ENFORCED fenced}: the task
 * carries its attempt id and only reports echoing it, from the same device,
 * count ({@link MqttAttempts}). Whenever Core stops waiting before a result
 * (timeout, abandonment) it publishes {@code down/cancel} so the node can drop
 * the attempt; a late result is rejected either way.
 *
 * <p>Timeouts and the backlog-versus-unreachable rule come from
 * {@link TaskTimeoutPolicy}, shared with every transport.
 */
@Singleton
public class MqttNodeAgentClient implements NodeAgentClient {

    /** Granularity of the wait for the broker link while delivering. */
    private static final Duration LINK_POLL = Duration.ofMillis(250);

    private final MqttBrokerLink broker;
    private final NodeAttachments attachments;
    private final MqttSessions sessions;
    private final MqttAttempts attempts;
    private final TaskLifecycleService lifecycle;
    private final TaskTimeoutPolicy timeouts;

    @Inject
    public MqttNodeAgentClient(
            MqttBrokerLink broker,
            NodeAttachments attachments,
            MqttSessions sessions,
            MqttAttempts attempts,
            TaskLifecycleService lifecycle,
            TaskTimeoutPolicy timeouts) {

        this.broker = broker;
        this.attachments = attachments;
        this.sessions = sessions;
        this.attempts = attempts;
        this.lifecycle = lifecycle;
        this.timeouts = timeouts;
    }

    @Override
    public TaskResult execute(NodeExecutionTarget target, TaskDescriptor task) {

        Optional<String> deviceId = attachments.deviceIdFor(target.nodeId())
                .filter(device -> sessions.get(device).isPresent());

        if (deviceId.isEmpty()) {
            return failed(task, TaskFailureReason.NODE_UNREACHABLE,
                    "Node " + target.displayName() + " has no MQTT session");
        }

        String device = deviceId.get();

        TaskLifecycleService.Attempt attempt =
                lifecycle.accepted(task.id(), target.nodeId(), AttemptFencing.ENFORCED);

        MqttAttempts.Pending pending = attempts.track(device, attempt);

        long deliveryDeadline = System.nanoTime() + timeouts.deliveryTimeout().toNanos();

        try {

            if (!publishTask(device, task, attempt, deliveryDeadline)) {

                if (attempt.isAbandoned()) {
                    return nodeOffline(target, task, attempt, device);
                }

                lifecycle.failed(attempt);

                return failed(task, TaskFailureReason.TRANSPORT_UNAVAILABLE,
                        "Core could not hand " + task.id() + " to the MQTT broker within "
                                + timeouts.deliveryTimeout().toSeconds() + "s");
            }

            System.out.printf(
                    "[MqttNodeAgentClient] Published %s (attempt %d, id %s) to %s; "
                            + "waiting for acknowledgement...%n",
                    task.id(), attempt.number(), attempt.shortId(), target.displayName());

            /*
             * Phase 1 - delivery: the node's ack (or any later report).
             */
            if (!awaitDelivery(pending, deliveryDeadline)) {
                return undelivered(target, task, attempt, device);
            }

            /*
             * Phase 2 - execution, measured from delivery.
             */
            TaskResult result = pending.result.get(
                    timeouts.executionTimeout().toMillis(), TimeUnit.MILLISECONDS);

            return result;

        } catch (TimeoutException exception) {

            if (attempt.isAbandoned()) {
                return nodeOffline(target, task, attempt, device);
            }

            lifecycle.failed(attempt);
            cancel(device, task, attempt, "execution_timeout");

            String start = lifecycle.snapshot(task.id())
                    .filter(snapshot -> attempt.id().equals(snapshot.attemptId()))
                    .map(snapshot -> snapshot.startAcknowledged()
                            ? "start acknowledged"
                            : "start not acknowledged")
                    .orElse("start unknown");

            return failed(task, TaskFailureReason.EXECUTION_TIMEOUT,
                    "No result from " + target.displayName() + " within "
                            + timeouts.executionTimeout().toSeconds()
                            + "s of delivery (" + start + ")");

        } catch (ExecutionException exception) {

            if (exception.getCause() instanceof AttemptAbandonedException) {
                return nodeOffline(target, task, attempt, device);
            }

            lifecycle.failed(attempt);
            cancel(device, task, attempt, "transport_error");

            return failed(task, TaskFailureReason.TRANSPORT_ERROR,
                    "MQTT attempt failed: " + exception.getCause());

        } catch (InterruptedException exception) {

            Thread.currentThread().interrupt();
            lifecycle.failed(attempt);
            cancel(device, task, attempt, "interrupted");

            return failed(task, TaskFailureReason.TRANSPORT_ERROR,
                    "Interrupted while waiting for " + target.displayName());

        } finally {
            attempts.forget(pending);
        }
    }

    /**
     * Publishes the task, waiting (within the delivery window) for the broker
     * link if it is down. False if it could not be handed over in time, or the
     * attempt was abandoned meanwhile.
     */
    private boolean publishTask(
            String device,
            TaskDescriptor task,
            TaskLifecycleService.Attempt attempt,
            long deadline) throws InterruptedException {

        byte[] body = TaskWire.taskBody(
                task, AttemptFencing.ENFORCED, attempt.id(), attempt.number())
                .getBytes(StandardCharsets.UTF_8);

        String topic = broker.topics().down(device, MqttTopics.Down.TASK);

        while (!attempt.isAbandoned()) {

            if (broker.isConnected()
                    && broker.publish(topic, body, MqttBrokerLink.QOS_AT_LEAST_ONCE)) {
                return true;
            }

            long remaining = deadline - System.nanoTime();

            if (remaining <= 0) {
                return false;
            }

            Duration wait = Duration.ofNanos(Math.min(remaining, LINK_POLL.toNanos()));

            if (!broker.awaitConnected(wait)) {
                // Not started, or still down: sleep the slice so a missing
                // link does not spin.
                TimeUnit.NANOSECONDS.sleep(
                        Math.max(0, Math.min(deadline - System.nanoTime(), wait.toNanos())));
            }
        }

        return false;
    }

    private boolean awaitDelivery(MqttAttempts.Pending pending, long deadline)
            throws InterruptedException, ExecutionException {

        long remaining = Math.max(0, deadline - System.nanoTime());

        try {
            pending.delivered.get(remaining, TimeUnit.NANOSECONDS);
            return true;
        } catch (TimeoutException exception) {
            return pending.delivered.isDone() && !pending.delivered.isCompletedExceptionally();
        }
    }

    /**
     * The node never acknowledged. Whose fault was it?
     */
    private TaskResult undelivered(
            NodeExecutionTarget target,
            TaskDescriptor task,
            TaskLifecycleService.Attempt attempt,
            String device) {

        if (attempt.isAbandoned()) {
            return nodeOffline(target, task, attempt, device);
        }

        lifecycle.failed(attempt);

        // The task may still be queued at the broker for this node.
        cancel(device, task, attempt, "delivery_timeout");

        if (!broker.isConnected()) {
            return failed(task, TaskFailureReason.TRANSPORT_UNAVAILABLE,
                    "Task " + task.id() + " was not acknowledged by " + target.displayName()
                            + ": Core lost its broker connection");
        }

        Optional<Duration> sinceContact = sessions.timeSinceContact(device);
        TaskFailureReason reason = timeouts.classifyUndelivered(sinceContact);

        String message = reason == TaskFailureReason.DELIVERY_TIMEOUT
                ? "Task " + task.id() + " was not acknowledged by " + target.displayName()
                        + " within " + timeouts.deliveryTimeout().toSeconds()
                        + "s; node is still in contact (backlog)"
                : "Task " + task.id() + " was never acknowledged: " + target.displayName()
                        + (sinceContact.isPresent()
                                ? " last heard from " + sinceContact.get().toSeconds() + "s ago"
                                : " has never been heard from");

        return failed(task, reason, message);
    }

    /** The attempt was abandoned because the node went OFFLINE. */
    private TaskResult nodeOffline(
            NodeExecutionTarget target,
            TaskDescriptor task,
            TaskLifecycleService.Attempt attempt,
            String device) {

        cancel(device, task, attempt, "node_offline");

        String phase = lifecycle.snapshot(task.id())
                .filter(snapshot -> attempt.id().equals(snapshot.attemptId()))
                .map(snapshot -> snapshot.startAcknowledged()
                        ? "executing it"
                        : snapshot.reachedNode()
                                ? "holding it (acknowledged)"
                                : "not yet acknowledged")
                .orElse("in flight");

        return failed(task, TaskFailureReason.NODE_OFFLINE,
                "Node " + target.displayName() + " went OFFLINE while " + phase
                        + "; attempt abandoned");
    }

    /** Best effort: the node may never see it, and that is fine. */
    private void cancel(
            String device,
            TaskDescriptor task,
            TaskLifecycleService.Attempt attempt,
            String reason) {

        broker.publish(
                broker.topics().down(device, MqttTopics.Down.CANCEL),
                MqttWire.cancel(task.id(), attempt.id(), reason),
                MqttBrokerLink.QOS_AT_LEAST_ONCE);
    }

    private static TaskResult failed(
            TaskDescriptor task,
            TaskFailureReason reason,
            String message) {

        return new TaskResult(task.id(), "FAILED", message, reason);
    }
}
