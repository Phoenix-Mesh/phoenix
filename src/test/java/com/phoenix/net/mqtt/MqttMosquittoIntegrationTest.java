// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.gson.JsonObject;
import com.phoenix.model.TaskLifecycleState;
import com.phoenix.model.TaskResult;

/**
 * #19 end to end: Core and node agents over Eclipse Paho, through a real
 * Mosquitto broker with TLS, password authentication and the shipped ACL.
 */
class MqttMosquittoIntegrationTest {

    private static final List<String> DEVICES =
            List.of("dev-1", "dev-2", "dev-3", "dev-4", "dev-5");

    @TempDir
    static Path shared;

    private static MosquittoBroker broker;

    private final List<AutoCloseable> cleanup = new ArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @BeforeAll
    static void startBroker() throws Exception {
        MosquittoBroker.assumeInstalled();
        broker = MosquittoBroker.start(shared.resolve("broker"), List.of("localhost"), DEVICES);
    }

    @AfterAll
    static void stopBroker() throws Exception {
        if (broker != null) {
            broker.close();
        }
    }

    @AfterEach
    void closeEverything() throws Exception {
        executor.shutdownNow();
        for (AutoCloseable closeable : cleanup) {
            closeable.close();
        }
    }

    // ------------------------------------------------------------------
    // wiring
    // ------------------------------------------------------------------

    private static MqttConfig config(
            MosquittoBroker on, String host, int port, String clientId, String user,
            Map<String, String> extra) {

        Map<String, String> properties = new HashMap<>(extra);
        properties.put("phoenix.mqtt.url", "ssl://" + host + ":" + port);
        properties.put("phoenix.mqtt.clientId", clientId);
        properties.put("phoenix.mqtt.username", user);
        properties.putIfAbsent("phoenix.mqtt.tls.caFile", on.caFile().toString());

        Map<String, String> environment = new HashMap<>();
        environment.put("PHOENIX_MQTT_PASSWORD", on.password(user));
        environment.put("PHOENIX_MQTT_ENROLLMENT_TOKEN", MqttFixture.ENROLLMENT_TOKEN);

        return MqttConfig.from(properties::get, environment::get);
    }

    private MqttFixture core(MosquittoBroker on, String clientId) {
        MqttFixture core = new MqttFixture(
                config(on, "localhost", on.tlsPort(), clientId, MosquittoBroker.CORE_USER, Map.of()),
                PahoMqttConnection::new,
                Duration.ofSeconds(5), Duration.ofSeconds(5), Duration.ofSeconds(15));
        cleanup.add(0, core.gateway::stop);
        await(core.link::isConnected, "Core connects to the broker over TLS");
        return core;
    }

    private PahoMqttConnection connection(MosquittoBroker on, String device) {
        PahoMqttConnection connection = new PahoMqttConnection(
                config(on, "localhost", on.tlsPort(), device, device, Map.of()));
        cleanup.add(0, connection);
        return connection;
    }

    private FakeMqttAgent agent(MqttFixture core, MosquittoBroker on, String device) {
        PahoMqttConnection connection = connection(on, device);
        FakeMqttAgent agent = new FakeMqttAgent(connection, core.link.topics(), device);
        await(connection::isConnected, device + " connects to the broker");
        JsonObject answer = agent.register(MqttFixture.ENROLLMENT_TOKEN);
        assertEquals("ok", answer == null ? null : answer.get("status").getAsString(),
                "registration of " + device);
        return agent;
    }

    private static void await(BooleanSupplier condition, String what) {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out: " + what + "\n" + broker.log());
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new AssertionError(exception);
            }
        }
    }

    private static boolean staysFalse(BooleanSupplier condition, Duration period)
            throws InterruptedException {
        long deadline = System.nanoTime() + period.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }

    // ------------------------------------------------------------------
    // tests
    // ------------------------------------------------------------------

    @Test
    void aTaskRunsEndToEndOverTls() throws Exception {

        MqttFixture core = core(broker, "core-e2e");
        FakeMqttAgent agent = agent(core, broker, "dev-1");

        agent.heartbeat();
        await(() -> core.registry.get(agent.nodeId).orElseThrow().status().availableRamMb() == 3000,
                "heartbeat reaches the registry");

        TaskResult result = core.client.execute(core.target(agent), MqttFixture.task("task-001"));

        assertTrue(result.isSuccess(), result.toString());
        assertEquals(TaskLifecycleState.COMPLETED,
                core.lifecycle.snapshot("task-001").orElseThrow().state());
        assertTrue(core.lifecycle.snapshot("task-001").orElseThrow().startAcknowledged());
    }

    @Test
    void theAclKeepsDevicesFromSpeakingForOrListeningToEachOther() throws Exception {

        MqttFixture core = core(broker, "core-acl");
        FakeMqttAgent victim = agent(core, broker, "dev-2");
        agent(core, broker, "dev-3");
        victim.mode = FakeMqttAgent.Mode.ACK_ONLY;

        // dev-3 opens a second connection and tries to read dev-2's tasks...
        List<String> overheard = new CopyOnWriteArrayList<>();
        PahoMqttConnection spy = new PahoMqttConnection(
                config(broker, "localhost", broker.tlsPort(), "dev-3-spy", "dev-3", Map.of()));
        cleanup.add(0, spy);
        spy.subscribe("phoenix/v1/nodes/dev-2/down/#", 1, (topic, payload) -> overheard.add(topic));
        spy.open();
        await(spy::isConnected, "spy connects");

        Future<TaskResult> attempt = executor.submit(() ->
                core.client.execute(core.target(victim), MqttFixture.task("task-acl")));
        await(() -> !victim.tasks.isEmpty(), "victim receives the task");
        JsonObject task = victim.lastTask();

        // ...and to forge dev-2's result, with dev-2's own session token.
        long dropped = core.attempts.droppedReportCount();
        long rejected = core.gateway.rejectedMessageCount();
        spy.publish("phoenix/v1/nodes/dev-2/up/result",
                ("{\"sessionToken\":\"" + victim.sessionToken + "\",\"taskId\":\"task-acl\","
                        + "\"attemptId\":\"" + task.get("attemptId").getAsString()
                        + "\",\"status\":\"SUCCESS\",\"outputData\":\"forged\"}")
                        .getBytes(StandardCharsets.UTF_8), 1, false);

        assertTrue(staysFalse(attempt::isDone, Duration.ofMillis(700)),
                "the broker must drop a result published on another device's topic");
        assertEquals(dropped, core.attempts.droppedReportCount());
        assertEquals(rejected, core.gateway.rejectedMessageCount());
        assertTrue(overheard.isEmpty(), "dev-3 read dev-2's downlink: " + overheard);

        victim.result(task, "SUCCESS", victim.sessionToken);
        TaskResult result = attempt.get(5, TimeUnit.SECONDS);
        assertTrue(result.isSuccess());
        assertEquals("Computed Monte-Carlo pi ~= 3.1416", result.outputData());
    }

    @Test
    void coreRefusesABrokerWhoseCertificateItDoesNotTrust() throws Exception {

        Path otherCa = MosquittoBroker.otherCa(shared.resolve("other-ca"));

        PahoMqttConnection untrusted = new PahoMqttConnection(config(
                broker, "localhost", broker.tlsPort(), "core-untrusted", MosquittoBroker.CORE_USER,
                Map.of("phoenix.mqtt.tls.caFile", otherCa.toString())));
        cleanup.add(0, untrusted);
        untrusted.open();

        assertFalse(untrusted.awaitConnected(Duration.ofSeconds(3)));
    }

    @Test
    void theBrokerCertificateMustMatchTheHostCoreConnectsTo() throws Exception {

        // The certificate names "localhost"; 127.0.0.1 is the same machine
        // but not the name in the certificate.
        PahoMqttConnection mismatch = new PahoMqttConnection(config(
                broker, "127.0.0.1", broker.tlsPort(), "core-mismatch", MosquittoBroker.CORE_USER,
                Map.of()));
        cleanup.add(0, mismatch);
        mismatch.open();
        assertFalse(mismatch.awaitConnected(Duration.ofSeconds(3)));

        PahoMqttConnection unchecked = new PahoMqttConnection(config(
                broker, "127.0.0.1", broker.tlsPort(), "core-unchecked", MosquittoBroker.CORE_USER,
                Map.of("phoenix.mqtt.tls.verifyHostname", "false")));
        cleanup.add(0, unchecked);
        unchecked.open();
        assertTrue(unchecked.awaitConnected(Duration.ofSeconds(10)));
    }

    @Test
    void aWrongPasswordIsRefusedByTheBroker() throws Exception {

        Map<String, String> properties = Map.of(
                "phoenix.mqtt.url", "ssl://localhost:" + broker.tlsPort(),
                "phoenix.mqtt.clientId", "core-wrong-password",
                "phoenix.mqtt.username", MosquittoBroker.CORE_USER,
                "phoenix.mqtt.tls.caFile", broker.caFile().toString());

        PahoMqttConnection wrong = new PahoMqttConnection(MqttConfig.from(
                properties::get, Map.of("PHOENIX_MQTT_PASSWORD", "not-the-password")::get));
        cleanup.add(0, wrong);
        wrong.open();

        assertFalse(wrong.awaitConnected(Duration.ofSeconds(3)));
    }

    @Test
    void mutualTlsNeedsCoresClientCertificate() throws Exception {

        PahoMqttConnection withoutCertificate = new PahoMqttConnection(config(
                broker, "localhost", broker.mutualTlsPort(), "core-no-cert",
                MosquittoBroker.CORE_USER, Map.of()));
        cleanup.add(0, withoutCertificate);
        withoutCertificate.open();
        assertFalse(withoutCertificate.awaitConnected(Duration.ofSeconds(3)));

        PahoMqttConnection withCertificate = new PahoMqttConnection(config(
                broker, "localhost", broker.mutualTlsPort(), "core-cert",
                MosquittoBroker.CORE_USER,
                Map.of("phoenix.mqtt.tls.keyStore", broker.coreKeyStore().toString(),
                        "phoenix.mqtt.tls.keyStorePasswordFile",
                        broker.coreKeyStorePasswordFile().toString())));
        cleanup.add(0, withCertificate);
        withCertificate.open();
        assertTrue(withCertificate.awaitConnected(Duration.ofSeconds(10)));
    }

    @Test
    void afterABrokerRestartCoreReconnectsAndNodesCanRegisterAgain(@TempDir Path own)
            throws Exception {

        MosquittoBroker restartable =
                MosquittoBroker.start(own, List.of("localhost"), List.of("dev-4"));
        cleanup.add(restartable);

        MqttFixture core = core(restartable, "core-restart");
        FakeMqttAgent agent = agent(core, restartable, "dev-4");

        restartable.restart();

        await(() -> !core.link.isConnected(), "Core notices the broker went away");
        await(core.link::isConnected, "Core reconnects");
        await(agent.connection::isConnected, "the agent reconnects");

        // The restarted broker kept no sessions (persistence is off here), so
        // the agent registers again, as it would after a Core restart.
        String nodeId = agent.nodeId;
        agent.register(MqttFixture.ENROLLMENT_TOKEN);
        assertEquals(nodeId, agent.nodeId);

        TaskResult result = core.client.execute(core.target(agent), MqttFixture.task("task-restart"));
        assertTrue(result.isSuccess(), result.toString());
    }
}
