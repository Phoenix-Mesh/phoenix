// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.phoenix.model.HardwareClass;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.ResourceStatus;
import com.phoenix.model.TaskLifecycleState;

class InFlightAttemptReconcilerTest {

    private final DeviceRegistry registry = new DeviceRegistry();
    private final NodeHealthMonitor monitor = new NodeHealthMonitor(registry);
    private final TaskLifecycleService lifecycle = new TaskLifecycleService();

    private final java.util.Map<String, TaskLifecycleService.Attempt> handles =
            new java.util.HashMap<>();

    private TaskLifecycleService.Attempt accept(String taskId, String nodeId) {
        TaskLifecycleService.Attempt attempt =
                lifecycle.accepted(taskId, nodeId, AttemptFencing.LEGACY);
        handles.put(taskId, attempt);
        return attempt;
    }

    private TaskLifecycleService.Attempt h(String taskId) {
        return handles.get(taskId);
    }
    private final InFlightAttemptReconciler reconciler =
            new InFlightAttemptReconciler(lifecycle, monitor);

    private Instant register(String id) {
        registry.register(
                new NodeInfo(id, id, HardwareClass.LINUX, "x86_64",
                        4, 4096, "linux", Set.of()),
                ResourceStatus.of(10.0, 4096, 0, 0, 1.0, 1.0));
        return registry.get(id).orElseThrow().lastHeartbeat();
    }

    private static void sleepBriefly() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private TaskLifecycleState state(String task) {
        return lifecycle.snapshot(task).orElseThrow().state();
    }

    @Test
    void offlineNodeAbandonsItsAttemptsButNotOtherNodes() {

        Instant hb = register("A");
        register("B");
        reconciler.start();

        accept("on-a", "A");
        accept("on-b", "B");

        // Only A is aged out: B's heartbeat is strictly later than A's.
        sleepBriefly();
        registry.updateHeartbeat("B");
        monitor.evaluate(hb.plusSeconds(60));

        assertEquals(TaskLifecycleState.ABANDONED, state("on-a"));
        assertEquals(TaskLifecycleState.ACCEPTED, state("on-b"));
    }

    @Test
    void degradedNodeKeepsItsAttempts() {

        Instant hb = register("A");
        reconciler.start();

        accept("on-a", "A");
        lifecycle.delivered(h("on-a"));

        monitor.evaluate(hb.plusSeconds(30));   // DEGRADED, not OFFLINE

        assertEquals(TaskLifecycleState.DELIVERED, state("on-a"));
    }

    @Test
    void startingTwiceSubscribesOnce() {

        Instant hb = register("A");
        reconciler.start();
        reconciler.start();

        TaskLifecycleService.Attempt attempt = accept("on-a", "A");
        int[] runs = new int[1];
        attempt.onAbandon(() -> runs[0]++);

        monitor.evaluate(hb.plusSeconds(61));

        assertEquals(1, runs[0]);
    }

    @Test
    void notStartedMeansNothingIsReconciled() {

        Instant hb = register("A");
        accept("on-a", "A");

        monitor.evaluate(hb.plusSeconds(61));

        assertEquals(TaskLifecycleState.ACCEPTED, state("on-a"));
    }
}
