// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.phoenix.net.DeviceAnnouncement;
import com.phoenix.net.RegistrationServer;

/**
 * JSON bodies of the Phoenix MQTT protocol (version 1).
 *
 * <p>The node-to-Core bodies reuse the HTTP field names, so an agent that
 * speaks both transports builds the same JSON for both. Every message a node
 * sends after registering carries the {@code sessionToken} Core returned at
 * registration.
 *
 * <p>The full protocol is described in {@code docs/mqtt-agent-protocol.md}.
 */
public final class MqttWire {

    /** Largest payload Core accepts from a node, the same limit as HTTP. */
    public static final int MAX_PAYLOAD_BYTES = 64 * 1024;

    private static final Gson GSON = new Gson();

    private MqttWire() {
    }

    /** {@code up/register}: the HTTP registration body plus MQTT fields. */
    public static final class Register extends DeviceAnnouncement {

        /** Agent-chosen id; a repeated id gets the same answer (idempotent). */
        public String requestId;

        /** Must equal the configured enrollment token, when one is configured. */
        public String enrollmentToken;

        /** Optional; must be {@link MqttTopics#PROTOCOL_VERSION} if present. */
        public Integer protocolVersion;
    }

    /** {@code up/heartbeat}: the HTTP heartbeat body plus the session token. */
    public static final class Heartbeat extends RegistrationServer.HeartbeatMessage {
        public String sessionToken;
    }

    /** {@code up/ack} and {@code up/started}: one attempt of one task. */
    public static final class AttemptReport {
        public String sessionToken;
        public String taskId;
        public String attemptId;
    }

    /** {@code up/result}. */
    public static final class Result {
        public String sessionToken;
        public String taskId;
        public String attemptId;
        public String status;
        public String outputData;
        public Long executionTimeMs;
    }

    /** {@code up/status}: also the node's last will. */
    public static final class Status {
        public String state;
    }

    /**
     * Parses a body, or returns empty if it is not a JSON object of that shape.
     */
    public static <T> Optional<T> parse(byte[] payload, Class<T> type) {
        try {
            return Optional.ofNullable(
                    GSON.fromJson(new String(payload, StandardCharsets.UTF_8), type));
        } catch (JsonParseException | IllegalStateException exception) {
            return Optional.empty();
        }
    }

    /** {@code down/registered} for an accepted registration. */
    static byte[] registered(
            String requestId,
            String nodeId,
            String sessionToken,
            long heartbeatIntervalSeconds,
            String coreInstanceId) {

        JsonObject body = new JsonObject();
        body.addProperty("requestId", requestId);
        body.addProperty("status", "ok");
        body.addProperty("nodeId", nodeId);
        body.addProperty("sessionToken", sessionToken);
        body.addProperty("heartbeatIntervalSeconds", heartbeatIntervalSeconds);
        body.addProperty("coreInstanceId", coreInstanceId);
        body.addProperty("protocolVersion", MqttTopics.PROTOCOL_VERSION);
        return bytes(body);
    }

    /** {@code down/registered} for a refused registration. */
    static byte[] registrationRefused(String requestId, String message) {
        JsonObject body = new JsonObject();
        body.addProperty("requestId", requestId);
        body.addProperty("status", "error");
        body.addProperty("message", message);
        return bytes(body);
    }

    /** {@code down/cancel}: stop, or never start, this attempt. */
    static byte[] cancel(String taskId, String attemptId, String reason) {
        JsonObject body = new JsonObject();
        body.addProperty("taskId", taskId);
        body.addProperty("attemptId", attemptId);
        body.addProperty("reason", reason);
        return bytes(body);
    }

    /** {@code down/control}: register again (unknown device or stale session). */
    static byte[] reregister(String reason) {
        JsonObject body = new JsonObject();
        body.addProperty("type", "reregister");
        body.addProperty("reason", reason);
        return bytes(body);
    }

    /** {@code core/status}, published retained. */
    static byte[] coreStatus(String state, String coreInstanceId) {
        JsonObject body = new JsonObject();
        body.addProperty("state", state);
        body.addProperty("coreInstanceId", coreInstanceId);
        body.addProperty("protocolVersion", MqttTopics.PROTOCOL_VERSION);
        return bytes(body);
    }

    private static byte[] bytes(JsonObject body) {
        return GSON.toJson(body).getBytes(StandardCharsets.UTF_8);
    }
}
