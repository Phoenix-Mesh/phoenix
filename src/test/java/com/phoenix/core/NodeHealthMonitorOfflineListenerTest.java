// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.phoenix.model.HardwareClass;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.ResourceStatus;

class NodeHealthMonitorOfflineListenerTest {

    private final DeviceRegistry registry = new DeviceRegistry();
    private final NodeHealthMonitor monitor =
            new NodeHealthMonitor(registry);

    private Instant register(String id) {
        registry.register(
                new NodeInfo(id, "name-" + id, HardwareClass.LINUX,
                        "x86_64", 4, 4096, "linux", Set.of()),
                ResourceStatus.of(10.0, 4096, 0, 0, 1.0, 1.0));

        return registry.get(id).orElseThrow().lastHeartbeat();
    }

    @Test
    void listenerIsNotifiedOnceWhenANodeGoesOffline() {

        Instant hb = register("n1");
        List<String> seen = new ArrayList<>();
        monitor.addOfflineListener((id, name) -> seen.add(id + "/" + name));

        monitor.evaluate(hb.plusSeconds(61));
        monitor.evaluate(hb.plusSeconds(90));   // already OFFLINE: no repeat

        assertEquals(List.of("n1/name-n1"), seen);
    }

    @Test
    void degradedIsNotAnOfflineTransition() {

        Instant hb = register("n1");
        List<String> seen = new ArrayList<>();
        monitor.addOfflineListener((id, name) -> seen.add(id));

        monitor.evaluate(hb.plusSeconds(25));   // DEGRADED only

        assertEquals(List.of(), seen);
    }

    @Test
    void anOfflineNodeThatRecoversAndFailsAgainNotifiesAgain() {

        Instant hb = register("n1");
        List<String> seen = new ArrayList<>();
        monitor.addOfflineListener((id, name) -> seen.add(id));

        monitor.evaluate(hb.plusSeconds(61));
        registry.markOnline("n1");
        monitor.evaluate(registry.get("n1").orElseThrow()
                .lastHeartbeat().plusSeconds(61));

        assertEquals(List.of("n1", "n1"), seen);
    }

    @Test
    void aFailingListenerDoesNotStopOthersOrTheEvaluation() {

        Instant hb = register("n1");
        register("n2");
        List<String> seen = new ArrayList<>();

        monitor.addOfflineListener((id, name) -> {
            throw new IllegalStateException("boom");
        });
        monitor.addOfflineListener((id, name) -> seen.add(id));

        monitor.evaluate(hb.plusSeconds(61));

        assertEquals(2, seen.size());   // both n1 and n2 still processed
    }
}
