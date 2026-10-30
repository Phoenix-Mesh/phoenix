// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

/**
 * Transport-neutral lifecycle of one task attempt, as observable by Core.
 *
 * <pre>
 * ACCEPTED -> DELIVERED -> STARTED -> COMPLETED
 *     \            \           \
 *      +------------+-----------+--> FAILED
 *      +------------+-----------+--> ABANDONED
 * </pre>
 *
 * <ul>
 *     <li>{@link #ACCEPTED}  - the transport took responsibility for the task
 *         (HTTP: placed in the per-device pull queue; MQTT: published).
 *         The task has NOT reached the node.</li>
 *     <li>{@link #DELIVERED} - the task reached the node (HTTP: the node
 *         polled it; push: the request was handed to the node).</li>
 *     <li>{@link #STARTED}   - the node acknowledged that execution began.
 *         Optional: transports/agents that cannot report it simply skip it.</li>
 *     <li>{@link #COMPLETED} - the node reported a <em>successful</em>
 *         result.</li>
 *     <li>{@link #FAILED}    - the attempt ended without a successful
 *         result: the node reported a failure, or no result arrived
 *         (delivery/execution timeout, transport failure).</li>
 *     <li>{@link #ABANDONED} - Core stopped waiting because the node was
 *         declared OFFLINE. Unlike {@code FAILED} the node-side outcome is
 *         <em>unknown</em>: the computation may still have run, and any late
 *         message from the node for this attempt is ignored.</li>
 * </ul>
 *
 * <h2>How an attempt ends (the same for every transport)</h2>
 *
 * <table>
 *   <caption>Outcome to terminal state and failure reason</caption>
 *   <tr><th>What happened</th><th>State</th><th>TaskFailureReason</th></tr>
 *   <tr><td>node reported SUCCESS</td><td>COMPLETED</td><td>NONE</td></tr>
 *   <tr><td>node reported a failure</td><td>FAILED</td><td>EXECUTION_ERROR</td></tr>
 *   <tr><td>never delivered, node in contact</td><td>FAILED</td><td>DELIVERY_TIMEOUT</td></tr>
 *   <tr><td>never delivered, node silent</td><td>FAILED</td><td>NODE_UNREACHABLE</td></tr>
 *   <tr><td>delivered, no result in time</td><td>FAILED</td><td>EXECUTION_TIMEOUT</td></tr>
 *   <tr><td>transport broke</td><td>FAILED</td><td>TRANSPORT_ERROR</td></tr>
 *   <tr><td>node declared OFFLINE</td><td>ABANDONED</td><td>NODE_OFFLINE</td></tr>
 * </table>
 *
 * <p>A node-reported result is mapped by
 * {@code TaskLifecycleService.reported(Attempt, TaskResult)}; transports do
 * not choose between COMPLETED and FAILED themselves.
 *
 * <p>The state order is the declaration order; transitions never go backwards.
 */
public enum TaskLifecycleState {
    ACCEPTED,
    DELIVERED,
    STARTED,
    COMPLETED,
    FAILED,
    ABANDONED;

    public boolean isTerminal() {
        return this == COMPLETED ||
               this == FAILED ||
               this == ABANDONED;
    }
}
