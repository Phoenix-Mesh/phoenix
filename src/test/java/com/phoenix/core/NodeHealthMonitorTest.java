// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.phoenix.model.HardwareClass;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.NodeStatus;
import com.phoenix.model.ResourceStatus;

class NodeHealthMonitorTest {

    private NodeInfo sampleNode(
            String id,
            String name) {

        return new NodeInfo(
                id,
                name,
                HardwareClass.LINUX,
                "x86_64",
                8,
                16_384,
                "linux",
                Set.of()
        );
    }

    private ResourceStatus sampleStatus() {
        return ResourceStatus.of(
                10.0,
                8000,
                0,
                0,
                5.0,
                3.0
        );
    }

    @Test
    void healthyOnlineNodeRemainsOnline() {
        DeviceRegistry registry =
                new DeviceRegistry();

        registry.register(
                sampleNode("node-1", "Linux-01"),
                sampleStatus()
        );

        NodeHealthMonitor monitor =
                new NodeHealthMonitor(registry);

        Instant registeredAt =
                registry.get("node-1")
                        .orElseThrow()
                        .lastHeartbeat();

        monitor.evaluate(
                registeredAt.plusMillis(5_000)
        );

        assertEquals(
                NodeStatus.ONLINE,
                registry.get("node-1")
                        .orElseThrow()
                        .state()
        );
    }

    @Test
    void onlineTransitionsToDegradedAfterThreshold() {
        DeviceRegistry registry =
                new DeviceRegistry();

        registry.register(
                sampleNode("node-1", "Linux-01"),
                sampleStatus()
        );

        NodeHealthMonitor monitor =
                new NodeHealthMonitor(registry);

        Instant registeredAt =
                registry.get("node-1")
                        .orElseThrow()
                        .lastHeartbeat();

        monitor.evaluate(
                registeredAt.plusMillis(
                        NodeHealthMonitor.DEGRADED_AFTER_MS + 1
                )
        );

        assertEquals(
                NodeStatus.DEGRADED,
                registry.get("node-1")
                        .orElseThrow()
                        .state()
        );
    }

    @Test
    void degradedTransitionsToOfflineAfterThreshold() {
        DeviceRegistry registry =
                new DeviceRegistry();

        registry.register(
                sampleNode("node-1", "Linux-01"),
                sampleStatus()
        );

        NodeHealthMonitor monitor =
                new NodeHealthMonitor(registry);

        Instant registeredAt =
                registry.get("node-1")
                        .orElseThrow()
                        .lastHeartbeat();

        monitor.evaluate(
                registeredAt.plusMillis(
                        NodeHealthMonitor.DEGRADED_AFTER_MS + 1
                )
        );

        assertEquals(
                NodeStatus.DEGRADED,
                registry.get("node-1")
                        .orElseThrow()
                        .state()
        );

        monitor.evaluate(
                registeredAt.plusMillis(
                        NodeHealthMonitor.OFFLINE_AFTER_MS + 1
                )
        );

        assertEquals(
                NodeStatus.OFFLINE,
                registry.get("node-1")
                        .orElseThrow()
                        .state()
        );
    }

    @Test
    void heartbeatRecoversDegradedNode() {
        DeviceRegistry registry =
                new DeviceRegistry();

        registry.register(
                sampleNode("node-1", "Linux-01"),
                sampleStatus()
        );

        NodeHealthMonitor monitor =
                new NodeHealthMonitor(registry);

        registry.markDegraded("node-1");

        assertEquals(
                NodeStatus.DEGRADED,
                registry.get("node-1")
                        .orElseThrow()
                        .state()
        );

        monitor.onHeartbeat("node-1");

        assertEquals(
                NodeStatus.ONLINE,
                registry.get("node-1")
                        .orElseThrow()
                        .state()
        );
    }

    @Test
    void heartbeatRecoversOfflineNode() {
        DeviceRegistry registry =
                new DeviceRegistry();

        registry.register(
                sampleNode("node-1", "Linux-01"),
                sampleStatus()
        );

        NodeHealthMonitor monitor =
                new NodeHealthMonitor(registry);

        registry.markOffline("node-1");

        monitor.onHeartbeat("node-1");

        assertEquals(
                NodeStatus.ONLINE,
                registry.get("node-1")
                        .orElseThrow()
                        .state()
        );
    }

    @Test
    void evaluatesEveryRegisteredNodeIndependently() {
        DeviceRegistry registry =
                new DeviceRegistry();

        registry.register(
                sampleNode("node-1", "Linux-01"),
                sampleStatus()
        );

        registry.register(
                sampleNode("node-2", "Linux-02"),
                sampleStatus()
        );

        NodeHealthMonitor monitor =
                new NodeHealthMonitor(registry);

        Instant registeredAt =
                registry.get("node-1")
                        .orElseThrow()
                        .lastHeartbeat();

        registry.markOffline("node-2");

        monitor.evaluate(
                registeredAt.plusMillis(
                        NodeHealthMonitor.DEGRADED_AFTER_MS + 1
                )
        );

        assertEquals(
                NodeStatus.DEGRADED,
                registry.get("node-1")
                        .orElseThrow()
                        .state()
        );

        assertEquals(
                NodeStatus.OFFLINE,
                registry.get("node-2")
                        .orElseThrow()
                        .state()
        );
    }
}
