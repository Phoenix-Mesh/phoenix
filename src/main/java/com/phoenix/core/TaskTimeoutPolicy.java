// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import java.time.Duration;
import java.util.Optional;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.model.TaskFailureReason;

/**
 * Core-owned timeout semantics for remote task execution (Issue #28).
 *
 * <p>Phoenix used to apply a single end-to-end timer from queue submission to
 * result. That conflated three different questions, so the policy now answers
 * them separately and transports simply apply it:
 *
 * <ul>
 *     <li><b>Delivery timeout</b> - how long a task may wait for the node to
 *         take it. This is a bound on queue backlog, not on node speed.</li>
 *     <li><b>Execution timeout</b> - how long the node has, <em>after the task
 *         was delivered</em>, to produce a result.</li>
 *     <li><b>Contact liveness</b> - how recently the node must have been in
 *         contact for an undelivered task to be called "backlog" rather than
 *         "unreachable".</li>
 * </ul>
 *
 * <p>The policy has no HTTP, MQTT, or device knowledge. Node health
 * (heartbeat-based ONLINE/DEGRADED/OFFLINE) remains owned by
 * {@link NodeHealthMonitor} and is intentionally not touched here.
 *
 * <p>System properties (seconds):
 * <ul>
 *     <li>{@code phoenix.deliveryTimeoutSeconds} (default 60)</li>
 *     <li>{@code phoenix.taskTimeoutSeconds} (default 12) - kept for
 *         compatibility; it now bounds execution after delivery, not
 *         queue wait</li>
 *     <li>{@code phoenix.contactLivenessSeconds} (default 15)</li>
 * </ul>
 */
@Singleton
public class TaskTimeoutPolicy {

    private final Duration deliveryTimeout;
    private final Duration executionTimeout;
    private final Duration contactLiveness;

    @Inject
    public TaskTimeoutPolicy() {
        this(
                Duration.ofSeconds(
                        Long.getLong("phoenix.deliveryTimeoutSeconds", 60L)),
                Duration.ofSeconds(
                        Long.getLong("phoenix.taskTimeoutSeconds", 12L)),
                Duration.ofSeconds(
                        Long.getLong("phoenix.contactLivenessSeconds", 15L))
        );
    }

    public TaskTimeoutPolicy(
            Duration deliveryTimeout,
            Duration executionTimeout,
            Duration contactLiveness) {

        this.deliveryTimeout = requirePositive(deliveryTimeout, "deliveryTimeout");
        this.executionTimeout = requirePositive(executionTimeout, "executionTimeout");
        this.contactLiveness = requirePositive(contactLiveness, "contactLiveness");
    }

    /** Max time a task may wait to be delivered to its node. */
    public Duration deliveryTimeout() {
        return deliveryTimeout;
    }

    /** Max time, measured from delivery, for the node to return a result. */
    public Duration executionTimeout() {
        return executionTimeout;
    }

    public Duration contactLiveness() {
        return contactLiveness;
    }

    /**
     * Classifies a task that was NOT delivered within the delivery window.
     *
     * @param sinceLastContact time since the transport last heard from the
     *        node (HTTP: last poll). Empty means never.
     * @return {@link TaskFailureReason#DELIVERY_TIMEOUT} if the node is still
     *         in contact (it is merely busy/backlogged), otherwise
     *         {@link TaskFailureReason#NODE_UNREACHABLE}
     */
    public TaskFailureReason classifyUndelivered(
            Optional<Duration> sinceLastContact) {

        boolean inContact =
                sinceLastContact.isPresent()
                        && sinceLastContact.get()
                                .compareTo(contactLiveness) <= 0;

        return inContact
                ? TaskFailureReason.DELIVERY_TIMEOUT
                : TaskFailureReason.NODE_UNREACHABLE;
    }

    private static Duration requirePositive(
            Duration value,
            String name) {

        if (value == null
                || value.isZero()
                || value.isNegative()) {

            throw new IllegalArgumentException(
                    name + " must be a positive duration"
            );
        }

        return value;
    }
}
