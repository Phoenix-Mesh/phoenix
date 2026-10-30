// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import java.util.Optional;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.NodeRegistrationRequest;
import com.phoenix.model.ResourceStatus;

/**
 * Transport-neutral entry point into the Phoenix domain model for node
 * registration and telemetry.
 *
 * <p>Transport gateways such as HTTP or MQTT decode their own wire formats
 * and translate them into these transport-neutral operations.
 *
 * <p>This service writes telemetry to the authoritative {@link DeviceRegistry}
 * and delegates lifecycle decisions to {@link NodeHealthMonitor}. It does
 * not contain transport-specific logic or lifecycle policy itself.
 */
@Singleton
public class NodeTelemetryService {

    private final DeviceRegistry deviceRegistry;
    private final NodeHealthMonitor nodeHealthMonitor;

    @Inject
    public NodeTelemetryService(
            DeviceRegistry deviceRegistry,
            NodeHealthMonitor nodeHealthMonitor) {

        this.deviceRegistry = deviceRegistry;
        this.nodeHealthMonitor = nodeHealthMonitor;
    }

    /**
     * Register a new node using transport-neutral registration data.
     */
    public NodeInfo register(NodeRegistrationRequest request) {
        return deviceRegistry.register(request);
    }

    /**
     * Record a heartbeat and resource snapshot for an already-registered
     * node.
     *
     * <p>The registry receives the telemetry and refreshes the authoritative
     * heartbeat timestamp. Lifecycle recovery is then delegated to the
     * health monitor.
     */
    public void reportHeartbeat(
            String nodeId,
            ResourceStatus status) {

        deviceRegistry.updateStatus(
                nodeId,
                status
        );

        nodeHealthMonitor.onHeartbeat(nodeId);
    }

    /**
     * Transport-neutral read access to a node's authoritative record.
     */
    public Optional<NodeRecord> get(String nodeId) {
        return deviceRegistry.get(nodeId);
    }
}
