// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.inject.Singleton;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.NodeRegistrationRequest;
import com.phoenix.model.NodeStatus;
import com.phoenix.model.ResourceStatus;

/**
 * In-memory record of every node that has registered with Core.
 *
 * <p>Holds a {@link NodeRecord} per node, combining static identity with the
 * latest telemetry and lifecycle state, because placement needs both. The
 * backing map is an implementation detail; a persistent store can replace it
 * without changing this class's surface.
 *
 * <p>The registry stores state but owns no policy: lifecycle transitions are
 * decided by {@link NodeHealthMonitor}.
 */
@Singleton
public class DeviceRegistry {

    private final Map<String, NodeRecord> nodes = new ConcurrentHashMap<>();

    /**
     * Handles the self-registration protocol described in the design
     * discussion: a brand-new request gets a freshly minted stable
     * {@code nodeId}; a request carrying an {@code existingNodeId} (a
     * reconnect) keeps that identity and simply refreshes its record.
     *
     * @return the NodeInfo actually stored, with its resolved nodeId — read
     *         this back if you need to know the assigned id for a new node.
     */
    public NodeInfo register(NodeRegistrationRequest request) {
        String nodeId = request.isReconnect() ? request.existingNodeId() : UUID.randomUUID().toString();

        NodeInfo info = new NodeInfo(
                nodeId,
                request.displayName(),
                request.hardwareClass(),
                request.architecture(),
                request.cpuCores(),
                request.memoryMb(),
                request.adapterType(),
                request.capabilities());

        NodeRecord record = NodeRecord.newlyRegistered(info, request.initialStatus());
        nodes.put(nodeId, record);

        System.out.printf(
                "[Registry] Node registered: id=%s name=%s class=%s adapter=%s%n",
                nodeId, info.displayName(), info.hardwareClass(), info.adapterType());

        return info;
    }

    /** Lower-level register, used when NodeInfo/ResourceStatus are already assembled. */
    public void register(NodeInfo info, ResourceStatus initialStatus) {
        nodes.put(info.nodeId(), NodeRecord.newlyRegistered(info, initialStatus));
        System.out.printf(
                "[Registry] Node registered: id=%s name=%s class=%s adapter=%s%n",
                info.nodeId(), info.displayName(), info.hardwareClass(), info.adapterType());
    }

    public void remove(String nodeId) {
        NodeRecord removed = nodes.remove(nodeId);
        if (removed != null) {
            System.out.println("[Registry] Node removed: " + nodeId);
        }
    }

    public Optional<NodeRecord> get(String nodeId) {
        return Optional.ofNullable(nodes.get(nodeId));
    }

    public List<NodeRecord> list() {
        return List.copyOf(nodes.values());
    }

    /** Nodes that may receive new work; see {@link NodeRecord#isSchedulable()}. */
    public List<NodeRecord> listSchedulable() {
        return nodes.values().stream()
                .filter(NodeRecord::isSchedulable)
                .toList();
    }

    /** Refresh a node's live resource snapshot (e.g. after a heartbeat/report). */
    public void updateStatus(String nodeId, ResourceStatus newStatus) {
        nodes.computeIfPresent(nodeId, (id, record) -> record.withStatus(newStatus).withHeartbeatNow());
    }

    /**
     * Record that a heartbeat was received without applying lifecycle policy.
     *
     * <p>The heartbeat timestamp is authoritative liveness data. Whether the
     * node should transition to ONLINE, DEGRADED, or OFFLINE is decided by
     * {@link NodeHealthMonitor}, not by the registry itself.
     */
    public void updateHeartbeat(String nodeId) {
        nodes.computeIfPresent(nodeId,(id, record) -> record.withHeartbeatNow());
    }

    /** Apply an ONLINE lifecycle transition requested by the health policy. */
    public void markOnline(String nodeId) {
        transitionState(nodeId, NodeStatus.ONLINE);
    }

    public void markDegraded(String nodeId) {
        transitionState(nodeId, NodeStatus.DEGRADED);
    }

    public void markOffline(String nodeId) {
        transitionState(nodeId, NodeStatus.OFFLINE);
    }

    private void transitionState(String nodeId, NodeStatus newState) {
        nodes.computeIfPresent(nodeId, (id, record) -> {
            System.out.printf("[Registry] Node %s -> %s%n", id, newState);
            return record.withState(newState);
        });
    }

    public int size() {
        return nodes.size();
    }
}
