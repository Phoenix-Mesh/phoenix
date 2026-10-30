// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.NodeStatus;
import com.phoenix.net.RegistrationServer;

/** #20: authenticated registration and telemetry over MQTT. */
class MqttNodeGatewayTest {

    private final MqttFixture f = new MqttFixture();

    private NodeRecord node(String nodeId) {
        return f.registry.get(nodeId).orElseThrow();
    }

    // ------------------------------------------------------------------
    // registration
    // ------------------------------------------------------------------

    @Test
    void aRegisteredDeviceGetsANodeAndASessionToken() {

        FakeMqttAgent agent = new FakeMqttAgent(f.broker, f.link.topics(), "android-a1");
        JsonObject answer = agent.register(MqttFixture.ENROLLMENT_TOKEN);

        assertEquals("ok", answer.get("status").getAsString());
        assertEquals(43, answer.get("sessionToken").getAsString().length());
        assertEquals(5, answer.get("heartbeatIntervalSeconds").getAsInt());
        assertEquals(f.link.coreInstanceId(), answer.get("coreInstanceId").getAsString());

        NodeRecord record = node(agent.nodeId);
        assertEquals("fake-android-a1", record.info().displayName());
        assertEquals(NodeStatus.ONLINE, record.state());
        assertEquals(Optional.of(MqttNodeGateway.TRANSPORT_ID),
                f.attachments.transportOf(agent.nodeId));
        assertEquals(Optional.of("android-a1"), f.attachments.deviceIdFor(agent.nodeId));
    }

    @Test
    void aRepeatedRequestGetsTheSameAnswerAndNoSecondNode() {

        FakeMqttAgent agent = new FakeMqttAgent(f.broker, f.link.topics(), "android-a1");

        JsonObject first = agent.register("req-1", MqttFixture.ENROLLMENT_TOKEN, "android-a1");
        JsonObject again = agent.register("req-1", MqttFixture.ENROLLMENT_TOKEN, "android-a1");

        assertEquals(first, again);
        assertEquals(1, f.registry.size());
    }

    @Test
    void registeringAgainKeepsTheNodeButRotatesTheSession() {

        FakeMqttAgent agent = f.agent("android-a1");
        String nodeId = agent.nodeId;
        String oldToken = agent.sessionToken;

        agent.register(MqttFixture.ENROLLMENT_TOKEN);

        assertEquals(nodeId, agent.nodeId);
        assertNotEquals(oldToken, agent.sessionToken);
        assertEquals(1, f.registry.size());

        // A stale copy of the agent still using the old token is refused.
        long rejected = f.gateway.rejectedMessageCount();
        agent.heartbeat(oldToken);
        assertEquals(rejected + 1, f.gateway.rejectedMessageCount());
        assertEquals("invalid_session",
                agent.controls.get(agent.controls.size() - 1).get("reason").getAsString());
    }

    @Test
    void withoutTheEnrollmentTokenNoNodeIsCreated() {

        FakeMqttAgent wrong = new FakeMqttAgent(f.broker, f.link.topics(), "android-a1");
        JsonObject answer = wrong.register("not-the-enrollment-token");

        assertEquals("error", answer.get("status").getAsString());
        assertEquals("enrollment_rejected", answer.get("message").getAsString());

        FakeMqttAgent none = new FakeMqttAgent(f.broker, f.link.topics(), "android-a2");
        assertEquals("enrollment_rejected", none.register(null).get("message").getAsString());

        assertEquals(0, f.registry.size());
        assertNull(wrong.sessionToken);
    }

    @Test
    void aRepeatedRequestIdDoesNotBypassEnrollment() {

        FakeMqttAgent agent = f.agent("android-a1");
        String requestId = agent.registrations.get(0).get("requestId").getAsString();

        // Someone replays the request id without the enrollment token.
        FakeMqttAgent replay = new FakeMqttAgent(f.broker, f.link.topics(), "android-a1");
        replay.sessionToken = null;
        JsonObject answer = replay.register(requestId, null, "android-a1");

        assertEquals("enrollment_rejected", answer.get("message").getAsString());
    }

    @Test
    void enrollmentIsOpenWhenNoTokenIsConfigured() {

        MqttFixture open = new MqttFixture(false,
                java.time.Duration.ofSeconds(1), java.time.Duration.ofSeconds(1),
                java.time.Duration.ofSeconds(1));

        FakeMqttAgent agent = new FakeMqttAgent(open.broker, open.link.topics(), "android-a1");

        assertEquals("ok", agent.register(null).get("status").getAsString());
    }

    @Test
    void theDeviceIdComesFromTheTopicNotTheBody() {

        FakeMqttAgent agent = new FakeMqttAgent(f.broker, f.link.topics(), "android-a1");

        JsonObject answer =
                agent.register("r", MqttFixture.ENROLLMENT_TOKEN, "android-someone-else");

        assertEquals("device_id_mismatch", answer.get("message").getAsString());
        assertEquals(0, f.registry.size());

        // Omitting the body field is fine: the topic says who it is.
        JsonObject ok = agent.register("r2", MqttFixture.ENROLLMENT_TOKEN, null);
        assertEquals("ok", ok.get("status").getAsString());
    }

    @Test
    void anInvalidRegistrationIsRefusedWithTheReason() {

        FakeMqttAgent agent = new FakeMqttAgent(f.broker, f.link.topics(), "android-a1");

        JsonObject body = agent.registrationBody("r", MqttFixture.ENROLLMENT_TOKEN, null);
        body.remove("adapterType");
        agent.publish(MqttTopics.Up.REGISTER, body.toString());

        assertEquals("adapterType is required",
                agent.registrations.get(0).get("message").getAsString());

        agent.publish(MqttTopics.Up.REGISTER, "{not json");
        assertEquals("invalid_registration",
                agent.registrations.get(1).get("message").getAsString());

        JsonObject future = agent.registrationBody("r3", MqttFixture.ENROLLMENT_TOKEN, null);
        future.addProperty("protocolVersion", 2);
        agent.publish(MqttTopics.Up.REGISTER, future.toString());
        assertEquals("unsupported_protocol_version",
                agent.registrations.get(2).get("message").getAsString());

        assertEquals(0, f.registry.size());
    }

    @Test
    void aDeviceKeepsItsNodeWhenItMovesFromHttpToMqtt() {

        // The same device registered over HTTP first.
        String httpNodeId = f.attachments.attach(
                "android-a1", RegistrationServer.TRANSPORT_ID, () -> "node-from-http").nodeId();

        FakeMqttAgent agent = f.agent("android-a1");

        assertEquals(httpNodeId, agent.nodeId);
        assertEquals(Optional.of(MqttNodeGateway.TRANSPORT_ID),
                f.attachments.transportOf(httpNodeId));
    }

    // ------------------------------------------------------------------
    // telemetry
    // ------------------------------------------------------------------

    @Test
    void aHeartbeatUpdatesTelemetryAndRecoversADegradedNode() {

        FakeMqttAgent agent = f.agent("android-a1");
        f.registry.markDegraded(agent.nodeId);

        agent.heartbeat();

        NodeRecord record = node(agent.nodeId);
        assertEquals(NodeStatus.ONLINE, record.state());
        assertEquals(3000, record.status().availableRamMb());
        assertEquals(12.0, record.status().networkLatencyMs());
        // No cpuUsagePercent in the body: unknown, never 0 %.
        assertEquals(-1.0, record.status().cpuUsagePercent());
    }

    @Test
    void aHeartbeatFromAnUnknownDeviceAsksItToRegisterAtMostOncePerInterval() {

        FakeMqttAgent stranger = new FakeMqttAgent(f.broker, f.link.topics(), "android-x");

        stranger.heartbeat("whatever");
        stranger.heartbeat("whatever");

        assertEquals(1, stranger.controls.size());
        assertEquals("reregister", stranger.controls.get(0).get("type").getAsString());
        assertEquals("unknown_device", stranger.controls.get(0).get("reason").getAsString());
        assertEquals(0, f.registry.size());
    }

    @Test
    void aHeartbeatForANodeThatLeftTheRegistryEndsTheSession() {

        FakeMqttAgent agent = f.agent("android-a1");
        f.registry.remove(agent.nodeId);

        agent.heartbeat();

        assertEquals("node_not_found",
                agent.controls.get(agent.controls.size() - 1).get("reason").getAsString());
        assertTrue(f.sessions.get("android-a1").isEmpty());
        assertTrue(f.attachments.nodeIdFor("android-a1").isEmpty());
    }

    @Test
    void aMalformedHeartbeatChangesNothingAndDoesNotEndTheSession() {

        FakeMqttAgent agent = f.agent("android-a1");
        agent.heartbeat();

        var before = node(agent.nodeId).status();

        agent.publish(MqttTopics.Up.HEARTBEAT, "{not json");
        agent.publish(MqttTopics.Up.HEARTBEAT, "[]");

        assertEquals(before, node(agent.nodeId).status());
        assertTrue(f.sessions.get("android-a1").isPresent());

        // The next valid heartbeat is still accepted.
        agent.heartbeat();
        assertEquals(NodeStatus.ONLINE, node(agent.nodeId).state());
    }

    // ------------------------------------------------------------------
    // topics and payloads
    // ------------------------------------------------------------------

    @Test
    void messagesOnUnexpectedTopicsOrTooLargeAreDropped() {

        f.agent("android-a1");
        long before = f.gateway.rejectedMessageCount();

        f.gateway.onUplink("phoenix/v1/nodes/android-a1/up/teleport", new byte[] {'{', '}'});
        f.gateway.onUplink("phoenix/v1/nodes/a/b/up/heartbeat", new byte[] {'{', '}'});
        f.gateway.onUplink("phoenix/v1/nodes/android-a1/up/heartbeat",
                new byte[MqttWire.MAX_PAYLOAD_BYTES + 1]);

        assertEquals(before + 3, f.gateway.rejectedMessageCount());
    }

    @Test
    void coreAnnouncesItselfRetainedAndSaysGoodbyeOnStop() {

        String topic = f.link.topics().coreStatus();

        JsonObject online = json(f.broker.retainedOn(topic));
        assertEquals("online", online.get("state").getAsString());
        assertEquals(f.link.coreInstanceId(), online.get("coreInstanceId").getAsString());

        f.gateway.stop();

        assertEquals("offline", json(f.broker.retainedOn(topic)).get("state").getAsString());
    }

    @Test
    void ifCoreVanishesTheBrokerPublishesItsLastWill() {

        f.coreConnection.dropLink();

        JsonObject will = json(f.broker.retainedOn(f.link.topics().coreStatus()));
        assertEquals("offline", will.get("state").getAsString());
        assertEquals(f.link.coreInstanceId(), will.get("coreInstanceId").getAsString());

        // Reconnecting announces Core again.
        f.coreConnection.goOnline();
        assertEquals("online",
                json(f.broker.retainedOn(f.link.topics().coreStatus())).get("state").getAsString());
    }

    @Test
    void anAgentsLastWillIsOnlyAHintAndChangesNoNodeState() {

        FakeMqttAgent agent = f.agent("android-a1");
        ((InMemoryMqttBroker.Connection) agent.connection).dropLink();

        assertEquals(NodeStatus.ONLINE, node(agent.nodeId).state());
        assertArrayEquals("{\"state\":\"offline\"}".getBytes(StandardCharsets.UTF_8),
                f.broker.retainedOn(f.link.topics().up("android-a1", MqttTopics.Up.STATUS)));
    }

    private static JsonObject json(byte[] payload) {
        return JsonParser.parseString(new String(payload, StandardCharsets.UTF_8))
                .getAsJsonObject();
    }
}
