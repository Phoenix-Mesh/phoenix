// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * A scriptable node agent speaking the Phoenix MQTT protocol, for tests.
 *
 * <p>It behaves like a well-written agent (acknowledges each task once,
 * ignores duplicates of an attempt it already handled, drops cancelled
 * attempts) unless a test switches it to a misbehaving {@link Mode}.
 */
final class FakeMqttAgent {

    enum Mode {
        /** ack, started, SUCCESS result. */
        SUCCEED,
        /** ack, started, FAILED result. */
        FAIL,
        /** ack only, then silence. */
        ACK_ONLY,
        /** Receives tasks but never answers. */
        SILENT
    }

    final String deviceId;
    final MqttConnection connection;
    final MqttTopics topics;

    final List<JsonObject> tasks = new CopyOnWriteArrayList<>();
    final List<JsonObject> cancels = new CopyOnWriteArrayList<>();
    final List<JsonObject> controls = new CopyOnWriteArrayList<>();
    final List<JsonObject> registrations = new CopyOnWriteArrayList<>();

    volatile Mode mode = Mode.SUCCEED;
    volatile String sessionToken;
    volatile String nodeId;
    volatile String output = "Computed Monte-Carlo pi ~= 3.1416";
    volatile double latencyMs = 12.0;
    volatile String hardwareClass = "ANDROID";
    volatile String adapterType = "android";

    FakeMqttAgent(InMemoryMqttBroker broker, MqttTopics topics, String deviceId) {
        this(broker.connect(deviceId), topics, deviceId);
    }

    /** An agent on any connection, for example Paho to a real broker. */
    FakeMqttAgent(MqttConnection connection, MqttTopics topics, String deviceId) {
        this.deviceId = deviceId;
        this.topics = topics;
        this.connection = connection;

        connection.subscribe(
                topics.base() + "/nodes/" + deviceId + "/down/#", 1, this::onDownlink);
        connection.lastWill(
                topics.up(deviceId, MqttTopics.Up.STATUS),
                bytes("{\"state\":\"offline\"}"), 1, true);
        connection.open();
    }

    // ------------------------------------------------------------------
    // what the agent sends
    // ------------------------------------------------------------------

    /** Registers with a fresh request id; returns Core's answer. */
    JsonObject register(String enrollmentToken) {
        return register(UUID.randomUUID().toString(), enrollmentToken, deviceId);
    }

    JsonObject register(String requestId, String enrollmentToken, String bodyDeviceId) {
        JsonObject body = registrationBody(requestId, enrollmentToken, bodyDeviceId);
        int before = registrations.size();
        publish(MqttTopics.Up.REGISTER, body.toString());

        // Immediate on the in-memory broker; a round trip on a real one.
        long deadline = System.currentTimeMillis() + 5_000;
        while (registrations.size() <= before && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return registrations.size() > before ? registrations.get(registrations.size() - 1) : null;
    }

    JsonObject registrationBody(String requestId, String enrollmentToken, String bodyDeviceId) {
        JsonObject body = new JsonObject();
        body.addProperty("requestId", requestId);
        if (bodyDeviceId != null) {
            body.addProperty("deviceId", bodyDeviceId);
        }
        body.addProperty("displayName", "fake-" + deviceId);
        body.addProperty("hardwareClass", hardwareClass);
        body.addProperty("architecture", "arm64-v8a");
        body.addProperty("cpuCores", 4);
        body.addProperty("memoryMb", 4096);
        body.addProperty("adapterType", adapterType);
        body.add("capabilities", new com.google.gson.JsonArray());
        com.google.gson.JsonArray features = new com.google.gson.JsonArray();
        features.add("attempt-id");
        body.add("protocolFeatures", features);
        body.addProperty("protocolVersion", 1);
        if (enrollmentToken != null) {
            body.addProperty("enrollmentToken", enrollmentToken);
        }
        return body;
    }

    void heartbeat() {
        heartbeat(sessionToken);
    }

    void heartbeat(String token) {
        JsonObject body = new JsonObject();
        body.addProperty("sessionToken", token);
        body.addProperty("deviceId", deviceId);
        body.addProperty("availableRamMb", 3000);
        body.addProperty("activeTasks", 0);
        body.addProperty("queueLength", 0);
        body.addProperty("networkLatencyMs", latencyMs);
        body.addProperty("energyCostEstimate", 2.0);
        publish(MqttTopics.Up.HEARTBEAT, body.toString());
    }

    void ack(JsonObject task) {
        publish(MqttTopics.Up.ACK, report(task, sessionToken).toString());
    }

    void started(JsonObject task) {
        publish(MqttTopics.Up.STARTED, report(task, sessionToken).toString());
    }

    void result(JsonObject task, String status, String token) {
        JsonObject body = report(task, token);
        body.addProperty("status", status);
        body.addProperty("outputData", output);
        body.addProperty("executionTimeMs", 42);
        publish(MqttTopics.Up.RESULT, body.toString());
    }

    void resultFor(String taskId, String attemptId, String status) {
        JsonObject body = new JsonObject();
        body.addProperty("sessionToken", sessionToken);
        body.addProperty("taskId", taskId);
        if (attemptId != null) {
            body.addProperty("attemptId", attemptId);
        }
        body.addProperty("status", status);
        body.addProperty("outputData", "forged-or-late");
        publish(MqttTopics.Up.RESULT, body.toString());
    }

    void publish(MqttTopics.Up kind, String json) {
        connection.publish(topics.up(deviceId, kind), bytes(json), 1, false);
    }

    void publishTo(String topic, String json) {
        connection.publish(topic, bytes(json), 1, false);
    }

    // ------------------------------------------------------------------
    // what the agent receives
    // ------------------------------------------------------------------

    private void onDownlink(String topic, byte[] payload) {

        JsonObject body = JsonParser.parseString(new String(payload, StandardCharsets.UTF_8))
                .getAsJsonObject();

        if (topic.endsWith("/down/registered")) {
            registrations.add(body);
            if ("ok".equals(body.get("status").getAsString())) {
                sessionToken = body.get("sessionToken").getAsString();
                nodeId = body.get("nodeId").getAsString();
            }
        } else if (topic.endsWith("/down/task")) {
            onTask(body);
        } else if (topic.endsWith("/down/cancel")) {
            cancels.add(body);
        } else if (topic.endsWith("/down/control")) {
            controls.add(body);
        }
    }

    private void onTask(JsonObject task) {

        String attemptId = task.get("attemptId").getAsString();

        // Idempotent: a redelivered attempt is acknowledged again, never re-run.
        boolean duplicate = tasks.stream()
                .anyMatch(t -> t.get("attemptId").getAsString().equals(attemptId));

        tasks.add(task);

        if (mode == Mode.SILENT) {
            return;
        }

        ack(task);

        if (duplicate || mode == Mode.ACK_ONLY) {
            return;
        }

        started(task);
        result(task, mode == Mode.FAIL ? "FAILED" : "SUCCESS", sessionToken);
    }

    JsonObject lastTask() {
        return tasks.get(tasks.size() - 1);
    }

    private static JsonObject report(JsonObject task, String token) {
        JsonObject body = new JsonObject();
        body.addProperty("sessionToken", token);
        body.addProperty("taskId", task.get("taskId").getAsString());
        body.addProperty("attemptId", task.get("attemptId").getAsString());
        return body;
    }

    static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
