// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.phoenix.adapter.impl.AndroidAdapter;
import com.phoenix.adapter.impl.Esp32Adapter;
import com.phoenix.adapter.impl.LinuxAdapter;
import com.phoenix.core.AdapterRegistry;
import com.phoenix.core.InFlightAttemptReconciler;
import com.phoenix.core.PlacementReservationService;
import com.phoenix.dispatch.PendingTaskQueue;
import com.phoenix.dispatch.TaskDispatcher;
import com.phoenix.model.NodeStatus;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskLifecycleState;
import com.phoenix.model.TaskResult;
import com.phoenix.net.NodeAgentClient;
import com.phoenix.net.NodeTransportRouter;
import com.phoenix.recovery.TaskRecoveryManager;
import com.phoenix.scheduler.GreedyCostScheduler;
import com.phoenix.scheduler.WeightedPlacementCostModel;
import com.phoenix.tui.TaskJournal;

/**
 * #22: MQTT nodes take part in placement, execution and recovery through the
 * unchanged dispatcher, reservation service and reconciler. Only the
 * adapters' transport differs.
 */
class MqttDispatchEndToEndTest {

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void stop() {
        executor.shutdownNow();
    }

    private static final class Core {
        final MqttFixture f;
        final PlacementReservationService reservations;
        final PendingTaskQueue pending = new PendingTaskQueue();
        final TaskDispatcher dispatcher;

        Core(MqttFixture f) {
            this.f = f;

            NodeAgentClient httpNeverUsed = (target, task) -> {
                throw new AssertionError("an MQTT node was sent over HTTP");
            };

            Map<String, NodeAgentClient> mqtt = Map.of(MqttNodeGateway.TRANSPORT_ID, f.client);

            AdapterRegistry adapters = new AdapterRegistry(Map.of(
                    "android", new AndroidAdapter(
                            new NodeTransportRouter(f.attachments, mqtt, httpNeverUsed)),
                    "linux", new LinuxAdapter(
                            new NodeTransportRouter(f.attachments, mqtt, httpNeverUsed)),
                    "esp32", new Esp32Adapter(
                            new NodeTransportRouter(f.attachments, mqtt, Esp32Adapter::noTransport))));

            reservations = new PlacementReservationService(
                    f.registry, new GreedyCostScheduler(new WeightedPlacementCostModel()));

            dispatcher = new TaskDispatcher(
                    f.registry, adapters, new TaskJournal(), new TaskRecoveryManager(),
                    pending, reservations);
        }

        FakeMqttAgent node(String deviceId, String hardwareClass, String adapter, double latency) {
            FakeMqttAgent agent = new FakeMqttAgent(f.broker, f.link.topics(), deviceId);
            agent.hardwareClass = hardwareClass;
            agent.adapterType = adapter;
            agent.latencyMs = latency;
            agent.register(MqttFixture.ENROLLMENT_TOKEN);
            agent.heartbeat(); // reports free memory, so the node is placeable
            return agent;
        }
    }

    private static TaskDescriptor task() {
        return new TaskDescriptor("task-001", "Monte-Carlo-Pi", 1, 128);
    }

    @Test
    void theUnchangedDispatcherRunsATaskOnAnMqttNode() {

        Core core = new Core(new MqttFixture());
        FakeMqttAgent phone = core.node("android-a1", "ANDROID", "android", 10);

        TaskDispatcher.Outcome.Completed done = assertInstanceOf(
                TaskDispatcher.Outcome.Completed.class, core.dispatcher.replace(task()));

        assertTrue(done.result().result().isSuccess());
        assertEquals(phone.nodeId, done.result().nodeId());
        assertEquals(0, core.reservations.reservationCount(phone.nodeId));
    }

    @Test
    void anEsp32NodeCanFinallyRunTasksWhenItRegisteredOverMqtt() {

        Core core = new Core(new MqttFixture());
        FakeMqttAgent esp = core.node("esp32-01", "ESP32", "esp32", 10);

        TaskDispatcher.Outcome.Completed done = assertInstanceOf(
                TaskDispatcher.Outcome.Completed.class, core.dispatcher.replace(task()));

        assertTrue(done.result().result().isSuccess());
        assertEquals(esp.nodeId, done.result().nodeId());
    }

    @Test
    void aBackloggedMqttNodeIsSkippedWithoutBeingDegraded() {

        Core core = new Core(new MqttFixture(true,
                Duration.ofMillis(400), Duration.ofSeconds(2), Duration.ofSeconds(10)));
        FakeMqttAgent busy = core.node("linux-busy", "LINUX", "linux", 1);
        FakeMqttAgent free = core.node("linux-free", "LINUX", "linux", 50);
        busy.mode = FakeMqttAgent.Mode.SILENT;

        TaskDispatcher.Outcome.Completed done = assertInstanceOf(
                TaskDispatcher.Outcome.Completed.class, core.dispatcher.replace(task()));

        assertEquals(free.nodeId, done.result().nodeId());
        assertEquals(1, busy.tasks.size());
        // Heard from recently, so DELIVERY_TIMEOUT: re-placed but not penalised.
        assertEquals(NodeStatus.ONLINE, core.f.registry.get(busy.nodeId).orElseThrow().state());
    }

    @Test
    void whenAnMqttNodeGoesOfflineItsTaskMovesToAnotherNodeAtOnce() throws Exception {

        MqttFixture f = new MqttFixture(true,
                Duration.ofSeconds(30), Duration.ofSeconds(30), Duration.ofSeconds(15));
        Core core = new Core(f);
        InFlightAttemptReconciler reconciler =
                new InFlightAttemptReconciler(f.lifecycle, f.monitor);

        FakeMqttAgent dying = core.node("android-dying", "ANDROID", "android", 1);
        FakeMqttAgent healthy = core.node("android-ok", "ANDROID", "android", 50);
        dying.mode = FakeMqttAgent.Mode.ACK_ONLY;

        Future<TaskDispatcher.Outcome> outcome =
                executor.submit(() -> core.dispatcher.replace(task()));

        long deadline = System.currentTimeMillis() + 3000;
        while (dying.tasks.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }

        // What NodeHealthMonitor does after 60 s without heartbeats:
        long start = System.nanoTime();
        f.registry.markOffline(dying.nodeId);
        reconciler.onNodeOffline(dying.nodeId, "android-dying");

        TaskDispatcher.Outcome.Completed done = assertInstanceOf(
                TaskDispatcher.Outcome.Completed.class, outcome.get(5, TimeUnit.SECONDS));

        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(3)) < 0);
        assertEquals(healthy.nodeId, done.result().nodeId());
        assertEquals(2, f.lifecycle.snapshot("task-001").orElseThrow().attempt());
        assertEquals(TaskLifecycleState.COMPLETED,
                f.lifecycle.snapshot("task-001").orElseThrow().state());
        assertEquals("node_offline", dying.cancels.get(0).get("reason").getAsString());

        // The dead node's late result for attempt 1 changes nothing.
        dying.result(dying.lastTask(), "SUCCESS", dying.sessionToken);
        assertEquals(TaskLifecycleState.COMPLETED,
                f.lifecycle.snapshot("task-001").orElseThrow().state());
        assertEquals(0, core.reservations.reservationCount(dying.nodeId));
    }

    @Test
    void aNodeWhoseOwnBrokerLinkIsDownIsNotPenalised() {

        Core core = new Core(new MqttFixture(true,
                Duration.ofMillis(300), Duration.ofSeconds(1), Duration.ofSeconds(10)));
        FakeMqttAgent phone = core.node("android-a1", "ANDROID", "android", 10);
        core.f.coreConnection.dropLink();

        TaskDispatcher.Outcome outcome = core.dispatcher.replace(task());

        assertInstanceOf(TaskDispatcher.Outcome.Waiting.class, outcome);
        assertEquals(TaskFailureReason.TRANSPORT_UNAVAILABLE,
                core.pending.get("task-001").lastFailure());
        assertEquals(NodeStatus.ONLINE,
                core.f.registry.get(phone.nodeId).orElseThrow().state());
    }

    @Test
    void theRouterSendsOtherNodesToTheAdaptersOwnTransport() {

        MqttFixture f = new MqttFixture();
        f.attachments.attach("android-http", "http", () -> "http-node");

        TaskResult viaHttp = new NodeTransportRouter(
                f.attachments,
                Map.of(MqttNodeGateway.TRANSPORT_ID, f.client),
                (target, task) -> new TaskResult(task.id(), "SUCCESS", "http"))
                .execute(new com.phoenix.model.NodeExecutionTarget("http-node", "p", "android"),
                        task());

        assertEquals("http", viaHttp.outputData());
    }
}
