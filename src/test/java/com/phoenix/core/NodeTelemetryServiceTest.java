// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.phoenix.model.HardwareClass;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.NodeRegistrationRequest;
import com.phoenix.model.NodeStatus;
import com.phoenix.model.ResourceStatus;

class NodeTelemetryServiceTest {

    private NodeRegistrationRequest sampleRequest() {
        return new NodeRegistrationRequest(
                null,
                "Linux-01",
                HardwareClass.LINUX,
                "x86_64",
                8,
                16_384,
                "linux",
                Set.of(),
                ResourceStatus.of(
                        -1.0,
                        0,
                        0,
                        0,
                        0.0,
                        3.0
                )
        );
    }

    @Test
    void registerCreatesAnAuthoritativeOnlineRecord() {
        DeviceRegistry registry = new DeviceRegistry();
        NodeHealthMonitor monitor =
                new NodeHealthMonitor(registry);

        NodeTelemetryService telemetry =
                new NodeTelemetryService(
                        registry,
                        monitor
                );

        var nodeInfo =
                telemetry.register(sampleRequest());

        NodeRecord record =
                registry.get(nodeInfo.nodeId())
                        .orElseThrow();

        assertEquals(
                NodeStatus.ONLINE,
                record.state()
        );

        assertEquals(
                "Linux-01",
                record.info().displayName()
        );
    }

    @Test
    void reportHeartbeatUpdatesAuthoritativeResourceStatus() {
        DeviceRegistry registry = new DeviceRegistry();
        NodeHealthMonitor monitor =
                new NodeHealthMonitor(registry);

        NodeTelemetryService telemetry =
                new NodeTelemetryService(
                        registry,
                        monitor
                );

        var nodeInfo =
                telemetry.register(sampleRequest());

        ResourceStatus freshStatus =
                ResourceStatus.of(
                        42.0,
                        4096,
                        1,
                        2,
                        12.5,
                        1.5
                );

        telemetry.reportHeartbeat(
                nodeInfo.nodeId(),
                freshStatus
        );

        NodeRecord record =
                registry.get(nodeInfo.nodeId())
                        .orElseThrow();

        assertEquals(
                freshStatus,
                record.status()
        );
    }

    @Test
    void reportHeartbeatRecoversDegradedNodeThroughHealthMonitor() {
        DeviceRegistry registry = new DeviceRegistry();
        NodeHealthMonitor monitor =
                new NodeHealthMonitor(registry);

        NodeTelemetryService telemetry =
                new NodeTelemetryService(
                        registry,
                        monitor
                );

        var nodeInfo =
                telemetry.register(sampleRequest());

        registry.markDegraded(nodeInfo.nodeId());

        assertEquals(
                NodeStatus.DEGRADED,
                registry.get(nodeInfo.nodeId())
                        .orElseThrow()
                        .state()
        );

        telemetry.reportHeartbeat(
                nodeInfo.nodeId(),
                ResourceStatus.of(
                        10.0,
                        8000,
                        0,
                        0,
                        5.0,
                        3.0
                )
        );

        NodeRecord record =
                registry.get(nodeInfo.nodeId())
                        .orElseThrow();

        assertEquals(
                NodeStatus.ONLINE,
                record.state()
        );
    }

    @Test
    void reportHeartbeatRecoversOfflineNodeThroughHealthMonitor() {
        DeviceRegistry registry = new DeviceRegistry();
        NodeHealthMonitor monitor =
                new NodeHealthMonitor(registry);

        NodeTelemetryService telemetry =
                new NodeTelemetryService(
                        registry,
                        monitor
                );

        var nodeInfo =
                telemetry.register(sampleRequest());

        registry.markOffline(nodeInfo.nodeId());

        telemetry.reportHeartbeat(
                nodeInfo.nodeId(),
                ResourceStatus.of(
                        20.0,
                        7000,
                        0,
                        0,
                        5.0,
                        3.0
                )
        );

        assertEquals(
                NodeStatus.ONLINE,
                registry.get(nodeInfo.nodeId())
                        .orElseThrow()
                        .state()
        );
    }

    @Test
    void getReflectsAuthoritativeRegistryState() {
        DeviceRegistry registry = new DeviceRegistry();
        NodeHealthMonitor monitor =
                new NodeHealthMonitor(registry);

        NodeTelemetryService telemetry =
                new NodeTelemetryService(
                        registry,
                        monitor
                );

        assertTrue(
                telemetry.get("does-not-exist").isEmpty()
        );

        var nodeInfo =
                telemetry.register(sampleRequest());

        assertTrue(
                telemetry.get(nodeInfo.nodeId()).isPresent()
        );
    }
}
