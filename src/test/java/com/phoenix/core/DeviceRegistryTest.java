// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.phoenix.model.HardwareClass;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.NodeStatus;
import com.phoenix.model.ResourceStatus;

/**
 * The registry holds multiple nodes in memory, tracks heartbeats and keeps
 * non-schedulable nodes out of the candidate pool.
 */
class DeviceRegistryTest {

    private NodeInfo sampleNode(String id, String name) {
        return new NodeInfo(id, name, HardwareClass.LINUX, "x86_64", 8, 16_384, "linux", Set.of());
    }

    private ResourceStatus sampleStatus() {
        return ResourceStatus.of(10.0, 8000, 0, 0, 5.0, 3.0);
    }

    @Test
    void registersAndRetrievesANode() {
        DeviceRegistry registry = new DeviceRegistry();
        registry.register(sampleNode("node-1", "Linux-01"), sampleStatus());

        NodeRecord record = registry.get("node-1").orElseThrow();
        assertEquals("Linux-01", record.info().displayName());
        assertEquals(NodeStatus.ONLINE, record.state());
    }

    @Test
    void listReturnsAllRegisteredNodes() {
        DeviceRegistry registry = new DeviceRegistry();
        registry.register(sampleNode("node-1", "Linux-01"), sampleStatus());
        registry.register(sampleNode("node-2", "Linux-02"), sampleStatus());

        assertEquals(2, registry.list().size());
    }

    @Test
    void removeDropsTheNode() {
        DeviceRegistry registry = new DeviceRegistry();
        registry.register(sampleNode("node-1", "Linux-01"), sampleStatus());
        registry.remove("node-1");

        assertTrue(registry.get("node-1").isEmpty());
    }

    @Test
    void heartbeatRefreshesTimestampWithoutChangingLifecycleState() {
        DeviceRegistry registry = new DeviceRegistry();
        registry.register(sampleNode("node-1", "Linux-01"), sampleStatus());
        registry.markDegraded("node-1");

        NodeRecord before = registry.get("node-1").orElseThrow();
        registry.updateHeartbeat("node-1");
        NodeRecord after = registry.get("node-1").orElseThrow();

        assertEquals(NodeStatus.DEGRADED, after.state());
        assertTrue(!after.lastHeartbeat().isBefore(before.lastHeartbeat()));
    }

    @Test
    void offlineNodesAreExcludedFromSchedulableList() {
        DeviceRegistry registry = new DeviceRegistry();
        registry.register(sampleNode("node-1", "Linux-01"), sampleStatus());
        registry.register(sampleNode("node-2", "Linux-02"), sampleStatus());
        registry.markOffline("node-2");

        assertEquals(1, registry.listSchedulable().size());
        assertEquals("node-1", registry.listSchedulable().get(0).nodeId());
    }
}
