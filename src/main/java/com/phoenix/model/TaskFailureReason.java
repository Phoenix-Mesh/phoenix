// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

/**
 * Why one execution attempt did not complete successfully.
 *
 * <p>Issue #28: a remote "timeout" is no longer a single ambiguous outcome.
 * Where a transport can tell the phases apart it reports one of:
 *
 * <ul>
 *     <li>{@link #DELIVERY_TIMEOUT} - the task never reached the node, but
 *         the node is demonstrably alive (ordinary queue backlog).</li>
 *     <li>{@link #NODE_UNREACHABLE} - the task never reached the node and
 *         the node has not been in contact either (genuinely unresponsive).</li>
 *     <li>{@link #EXECUTION_TIMEOUT} - the task reached the node, but no
 *         result came back in time.</li>
 * </ul>
 *
 * <p>{@link #TRANSPORT_UNAVAILABLE} covers the one failure that is Core's
 * own: Core could not hand the task to its transport at all (for example its
 * link to the MQTT broker is down). The task is re-placed, but the node is
 * not blamed.
 *
 * <p>{@link #TIMEOUT} is retained as the unclassified fallback for transports
 * that cannot distinguish these phases. It is treated conservatively (as an
 * execution-style failure) so detection of unresponsive nodes is never weakened.
 */
public enum TaskFailureReason {
    NONE,
    EXECUTION_ERROR,
    /** Unclassified timeout: the transport cannot tell delivery from execution. */
    TIMEOUT,
    /** Not delivered to a node that is still in contact (queue backlog). */
    DELIVERY_TIMEOUT,
    /** Delivered to the node, but no result arrived within the execution window. */
    EXECUTION_TIMEOUT,
    NODE_UNREACHABLE,
    TRANSPORT_ERROR,
    /** Core's own side of the transport was unavailable; the node is not implicated. */
    TRANSPORT_UNAVAILABLE,
    NODE_OFFLINE,
    NO_ELIGIBLE_NODE
}
