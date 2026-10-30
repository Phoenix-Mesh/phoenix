// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

import java.time.Instant;
import java.util.Objects;

/**
 * The registry's view of one node: static identity, the latest reported
 * resources, the lifecycle state and the time of the last heartbeat.
 *
 * <p>Immutable. Every registry update produces a new record through one of
 * the {@code with*} methods, which keeps updates atomic under
 * {@code ConcurrentHashMap.compute}.
 *
 * @param info          static identity and capability, fixed at registration
 * @param status        latest node-reported resource snapshot
 * @param state         lifecycle state decided by the health monitor
 * @param lastHeartbeat when the node was last heard from
 */
public record NodeRecord(
        NodeInfo info,
        ResourceStatus status,
        NodeStatus state,
        Instant lastHeartbeat
) {

    /** A freshly registered node: {@link NodeStatus#ONLINE}, heartbeat stamped now. */
    public static NodeRecord newlyRegistered(NodeInfo info, ResourceStatus initialStatus) {
        Objects.requireNonNull(info, "info");
        Objects.requireNonNull(initialStatus, "initialStatus");
        return new NodeRecord(info, initialStatus, NodeStatus.ONLINE, Instant.now());
    }

    public NodeRecord withStatus(ResourceStatus newStatus) {
        return new NodeRecord(info, newStatus, state, lastHeartbeat);
    }

    public NodeRecord withState(NodeStatus newState) {
        return new NodeRecord(info, status, newState, lastHeartbeat);
    }

    public NodeRecord withHeartbeatNow() {
        return new NodeRecord(info, status, state, Instant.now());
    }

    /** Only {@link NodeStatus#ONLINE} nodes receive new work. */
    public boolean isSchedulable() {
        return state == NodeStatus.ONLINE;
    }

    public String nodeId() {
        return info.nodeId();
    }
}
