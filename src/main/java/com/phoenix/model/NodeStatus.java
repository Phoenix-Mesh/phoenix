// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

/**
 * Lifecycle state of a registered node.
 *
 * <pre>
 * ONLINE --(no heartbeat 20s)--&gt; DEGRADED --(no heartbeat 60s)--&gt; OFFLINE
 *   ^                               |                                |
 *   +---------- heartbeat ----------+-------------- heartbeat -------+
 * </pre>
 *
 * <p>Transitions are decided by {@link com.phoenix.core.NodeHealthMonitor};
 * the registry only stores the result. Registration is synchronous, so a node
 * enters the registry directly as {@link #ONLINE}.
 */
public enum NodeStatus {

    /** Heartbeating normally; eligible for new work. */
    ONLINE,

    /**
     * Heartbeats are late. The node may still be running work, so nothing in
     * flight is cancelled, but it receives no new work until it recovers.
     */
    DEGRADED,

    /**
     * Declared gone. Not schedulable, and every unfinished attempt bound to
     * it is abandoned.
     */
    OFFLINE
}
