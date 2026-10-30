// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonParser;
import com.phoenix.core.DeviceRegistry;
import com.phoenix.core.NodeAttachments;
import com.phoenix.core.NodeHealthMonitor;
import com.phoenix.core.NodeTelemetryService;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.model.ResourceStatus;

/** Input hardening of the node-agent HTTP gateway. */
class RegistrationServerTest {

    private static final String DEVICE_ID = "android-gateway-test";

    private final HttpClient http = HttpClient.newHttpClient();

    private DeviceRegistry registry;
    private NodeAttachments attachments;
    private RegistrationServer server;

    @BeforeEach
    void start() throws Exception {
        registry = new DeviceRegistry();
        attachments = new NodeAttachments();
        server = new RegistrationServer(
                new NodeTelemetryService(registry, new NodeHealthMonitor(registry)),
                new TaskQueueService(new TaskLifecycleService()),
                attachments);
        server.start(0);
    }

    @AfterEach
    void stop() {
        server.stop();
    }

    private static String registration(String extraFields) {
        return "{\"deviceId\":\"" + DEVICE_ID + "\","
                + "\"displayName\":\"gateway-phone\","
                + "\"hardwareClass\":\"ANDROID\","
                + "\"architecture\":\"arm64\","
                + "\"cpuCores\":4,\"memoryMb\":4096,"
                + "\"adapterType\":\"android\","
                + "\"capabilities\":[]"
                + extraFields + "}";
    }

    private String registerAndGetNodeId() throws Exception {
        HttpResponse<String> response = post("/register", registration(""));
        assertEquals(200, response.statusCode());
        return JsonParser.parseString(response.body())
                .getAsJsonObject().get("nodeId").getAsString();
    }

    @Test
    void anOversizedBodyIsRefusedWith413() throws Exception {
        String huge = "{\"deviceId\":\"" + "x".repeat(RegistrationServer.MAX_BODY_BYTES) + "\"}";

        HttpResponse<String> response = post("/register", huge);

        assertEquals(413, response.statusCode());
        assertTrue(response.body().contains("request_body_too_large"));
        assertEquals(0, registry.size());
    }

    @Test
    void aBodyAtTheLimitIsStillRead() throws Exception {
        String padded = registration(
                ",\"pad\":\"" + "x".repeat(RegistrationServer.MAX_BODY_BYTES - 300) + "\"");

        assertTrue(padded.length() <= RegistrationServer.MAX_BODY_BYTES);
        assertEquals(200, post("/register", padded).statusCode());
    }

    @Test
    void aRegistrationWithoutAdapterTypeIsRejected() throws Exception {
        String body = registration("").replace("\"adapterType\":\"android\",", "");

        HttpResponse<String> response = post("/register", body);

        assertEquals(400, response.statusCode());
        assertTrue(response.body().contains("adapterType is required"));
    }

    @Test
    void anOutOfRangeCallbackPortIsRejected() throws Exception {
        assertEquals(400, post("/register", registration(",\"callbackPort\":0")).statusCode());
        assertEquals(400, post("/register", registration(",\"callbackPort\":70000")).statusCode());
    }

    @Test
    void errorResponsesAreValidJsonEvenForMultiLineMessages() throws Exception {
        // Gson's syntax error messages span several lines.
        HttpResponse<String> response = post("/register", "{\"deviceId\": [");

        assertEquals(400, response.statusCode());
        assertEquals("error", JsonParser.parseString(response.body())
                .getAsJsonObject().get("status").getAsString());
    }

    @Test
    void aHeartbeatWithoutCpuUsageIsRecordedAsUnknownNotIdle() throws Exception {
        String nodeId = registerAndGetNodeId();

        assertEquals(200, post("/heartbeat",
                "{\"deviceId\":\"" + DEVICE_ID + "\",\"availableRamMb\":1000}").statusCode());

        assertEquals(
                ResourceStatus.CPU_UNKNOWN,
                registry.get(nodeId).orElseThrow().status().cpuUsagePercent());
    }

    @Test
    void aHeartbeatWithCpuUsageKeepsTheReportedValue() throws Exception {
        String nodeId = registerAndGetNodeId();

        post("/heartbeat",
                "{\"deviceId\":\"" + DEVICE_ID + "\",\"cpuUsagePercent\":37.5}");

        assertEquals(37.5, registry.get(nodeId).orElseThrow().status().cpuUsagePercent());
    }

    @Test
    void aDeviceThatRegisteredOverAnotherTransportKeepsItsNode() throws Exception {
        attachments.attach(DEVICE_ID, "mqtt", () -> "node-from-mqtt");

        HttpResponse<String> response = post("/register", registration(""));

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"nodeId\":\"node-from-mqtt\""));
        assertTrue(response.body().contains("already_registered"));
        assertEquals(java.util.Optional.of(RegistrationServer.TRANSPORT_ID),
                attachments.transportOf("node-from-mqtt"));
        assertEquals(0, registry.size());
    }

    @Test
    void aNewDeviceIsAttachedAsAnHttpNode() throws Exception {
        String nodeId = registerAndGetNodeId();

        assertEquals(java.util.Optional.of(RegistrationServer.TRANSPORT_ID),
                attachments.transportOf(nodeId));
        assertEquals(java.util.Optional.of(DEVICE_ID), server.deviceIdForNode(nodeId));
    }

    private HttpResponse<String> post(String path, String json) throws Exception {
        return http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + path))
                        .timeout(Duration.ofSeconds(5))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
