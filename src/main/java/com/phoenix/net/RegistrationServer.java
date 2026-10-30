// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.core.AttemptFencing;
import com.phoenix.core.NodeAttachments;
import com.phoenix.core.NodeTelemetryService;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.NodeRegistrationRequest;
import com.phoenix.model.NodeStatus;
import com.phoenix.model.ResourceStatus;
import com.phoenix.model.TaskResult;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * HTTP gateway through which node agents register, heartbeat, poll for work
 * and report results.
 *
 * <p>The gateway decodes the wire format and translates it into
 * transport-neutral calls on {@link NodeTelemetryService} and
 * {@link TaskQueueService}. It performs no scheduling and owns no lifecycle
 * policy; ONLINE/DEGRADED/OFFLINE is decided by
 * {@link com.phoenix.core.NodeHealthMonitor}.
 *
 * <p>Push callback addresses and announced protocol features are specific to
 * this transport and therefore live here. Which device is which node is
 * shared with every other transport through {@link NodeAttachments}, so a
 * device keeps one node identity whichever transport it registers through.
 *
 * <p>The wire protocol is documented in {@code README.md} and
 * {@code docs/linux-agent-protocol.md}. Request bodies are limited to
 * {@value #MAX_BODY_BYTES} bytes.
 */
@Singleton
public class RegistrationServer {

    /** Largest request body accepted on any endpoint. */
    static final int MAX_BODY_BYTES = 64 * 1024;

    /** Transport id recorded in {@link NodeAttachments} for HTTP nodes. */
    public static final String TRANSPORT_ID = "http";

    private final NodeTelemetryService nodeTelemetryService;
    private final TaskQueueService taskQueue;
    private final NodeAttachments attachments;
    private final Gson gson = new Gson();

    private final Map<String, String> callbackAddressByNodeId =
            new ConcurrentHashMap<>();

    /** Protocol features each node announced at its latest registration. */
    private final Map<String, Set<String>> protocolFeaturesByNodeId =
            new ConcurrentHashMap<>();

    private HttpServer server;

    /** A gateway with its own, unshared device-to-node table (tests). */
    public RegistrationServer(
            NodeTelemetryService nodeTelemetryService,
            TaskQueueService taskQueue) {

        this(nodeTelemetryService, taskQueue, new NodeAttachments());
    }

    @Inject
    public RegistrationServer(
            NodeTelemetryService nodeTelemetryService,
            TaskQueueService taskQueue,
            NodeAttachments attachments) {

        this.nodeTelemetryService = nodeTelemetryService;
        this.taskQueue = taskQueue;
        this.attachments = attachments;
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(
                new InetSocketAddress(port),
                0
        );

        server.createContext("/register", this::handleRegister);
        server.createContext("/heartbeat", this::handleHeartbeat);
        server.createContext("/tasks/poll", this::handlePoll);
        server.createContext("/tasks/started", this::handleStarted);
        server.createContext("/results", this::handleResult);

        // The built-in executor is sufficient for this lightweight prototype.
        server.setExecutor(null);
        server.start();

        System.out.println(
                "[RegistrationServer] Listening for Phoenix Node Agents on port "
                        + port
        );
    }

    /** Actual bound port (useful when started on port 0), or -1 if stopped. */
    public int port() {
        return server == null
                ? -1
                : server.getAddress().getPort();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    public Optional<String> deviceIdForNode(String nodeId) {
        return attachments.deviceIdFor(nodeId);
    }

    /**
     * How reports for the next attempt on {@code nodeId} are fenced:
     * {@link AttemptFencing#ENFORCED} if the node announced
     * {@link ProtocolFeatures#ATTEMPT_ID} at its latest registration,
     * otherwise {@link AttemptFencing#LEGACY}.
     */
    public AttemptFencing fencingFor(String nodeId) {
        return protocolFeaturesByNodeId
                .getOrDefault(nodeId, Set.of())
                .contains(ProtocolFeatures.ATTEMPT_ID)
                ? AttemptFencing.ENFORCED
                : AttemptFencing.LEGACY;
    }

    /**
     * Operator policy {@code -Dphoenix.requireAttemptId=true}: refuse to
     * register agents that cannot echo an attempt id, so no unfenced node
     * ever joins. Off by default.
     */
    private static boolean attemptIdRequired() {
        return Boolean.getBoolean("phoenix.requireAttemptId");
    }

    private void rememberProtocolFeatures(
            DeviceAnnouncement announcement,
            String nodeId) {

        Set<String> features =
                announcement.protocolFeatures != null
                        ? Set.copyOf(announcement.protocolFeatures)
                        : Set.of();

        // Replaced on every registration, so an agent that is downgraded
        // (or re-installed without the feature) is treated as legacy again.
        protocolFeaturesByNodeId.put(nodeId, features);

        System.out.printf(
                features.contains(ProtocolFeatures.ATTEMPT_ID)
                        ? "[RegistrationServer] Node %s announced %s: late "
                                + "results will be fenced by attempt id%n"
                        : "[RegistrationServer] WARNING node %s is a LEGACY "
                                + "agent (no %s): its late results cannot be "
                                + "fenced by attempt id%n",
                nodeId,
                ProtocolFeatures.ATTEMPT_ID
        );
    }

    /** {@code host:port} a push node accepts tasks on, if it announced one. */
    public Optional<String> callbackAddressForNode(String nodeId) {
        return Optional.ofNullable(callbackAddressByNodeId.get(nodeId));
    }

    private void handleRegister(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(
                    exchange,
                    405,
                    """
                    {"status":"error","message":"POST required"}
                    """
            );
            return;
        }

        try {
            String body = readBody(exchange);
            DeviceAnnouncement announcement =
                    gson.fromJson(body, DeviceAnnouncement.class);

            if (announcement == null) {
                throw new IllegalArgumentException(
                        "request body is required"
                );
            }

            announcement.validate();

            if (attemptIdRequired()
                    && (announcement.protocolFeatures == null
                            || !announcement.protocolFeatures.contains(
                                    ProtocolFeatures.ATTEMPT_ID))) {

                System.out.printf(
                        "[RegistrationServer] Registration refused for %s: "
                                + "phoenix.requireAttemptId is set and the "
                                + "agent does not announce %s%n",
                        announcement.deviceId,
                        ProtocolFeatures.ATTEMPT_ID
                );

                sendJson(
                        exchange,
                        400,
                        "{\"status\":\"error\",\"message\":"
                                + "\"attempt_id_required\"}"
                );
                return;
            }

            ResourceStatus initialStatus = ResourceStatus.of(
                    -1.0,
                    0,
                    0,
                    0,
                    0.0,
                    3.0
            );

            NodeRegistrationRequest request =
                    new NodeRegistrationRequest(
                            null,
                            announcement.displayName,
                            announcement.hardwareClass,
                            announcement.architecture,
                            announcement.cpuCores,
                            announcement.memoryMb,
                            announcement.adapterType,
                            announcement.capabilities != null
                                    ? announcement.capabilities
                                    : Set.of(),
                            initialStatus
                    );

            NodeAttachments.Attachment attachment =
                    attachments.attach(
                            announcement.deviceId,
                            TRANSPORT_ID,
                            () -> nodeTelemetryService
                                    .register(request)
                                    .nodeId()
                    );

            String nodeId = attachment.nodeId();

            if (!attachment.created()) {

                System.out.printf(
                        "[RegistrationServer] Device %s already registered as node %s%s%n",
                        announcement.deviceId,
                        nodeId,
                        attachment.movedFrom()
                                .map(previous -> " (moved from " + previous + " to "
                                         + TRANSPORT_ID + ")")
                                .orElse("")
                );

                rememberProtocolFeatures(announcement, nodeId);

                storeCallbackAddressIfPresent(
                        announcement,
                        nodeId,
                        exchange
                );

                sendJson(
                        exchange,
                        200,
                        "{\"status\":\"ok\",\"nodeId\":\""
                                + nodeId
                                + "\",\"message\":\"already_registered\"}"
                );
                return;
            }

            rememberProtocolFeatures(announcement, nodeId);

            storeCallbackAddressIfPresent(
                    announcement,
                    nodeId,
                    exchange
            );

            String remoteAddress =
                    exchange.getRemoteAddress()
                            .getAddress()
                            .getHostAddress();

            System.out.printf(
                    "[RegistrationServer] Device registered: %s nodeId=%s source=%s%n",
                    announcement.displayName,
                    nodeId,
                    remoteAddress
            );

            sendJson(
                    exchange,
                    200,
                    "{\"status\":\"ok\",\"nodeId\":\""
                            + nodeId
                            + "\"}"
            );

        } catch (Exception exception) {
            System.out.println(
                    "[RegistrationServer] Registration rejected: "
                            + exception.getMessage()
            );

            sendError(exchange, exception);
        }
    }

    private void storeCallbackAddressIfPresent(
            DeviceAnnouncement announcement,
            String nodeId,
            HttpExchange exchange) {

        if (announcement.callbackPort == null) {
            return;
        }

        String host =
                exchange.getRemoteAddress()
                        .getAddress()
                        .getHostAddress();

        callbackAddressByNodeId.put(
                nodeId,
                host + ":" + announcement.callbackPort
        );

        System.out.printf(
                "[RegistrationServer] Node %s reachable for push at %s:%d%n",
                nodeId,
                host,
                announcement.callbackPort
        );
    }

    private void handleHeartbeat(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(
                    exchange,
                    405,
                    """
                    {"status":"error","message":"POST required"}
                    """
            );
            return;
        }

        try {
            String body = readBody(exchange);

            HeartbeatMessage heartbeat =
                    gson.fromJson(body, HeartbeatMessage.class);

            if (heartbeat.deviceId == null
                    || heartbeat.deviceId.isBlank()) {
                throw new IllegalArgumentException(
                        "deviceId is required"
                );
            }

            String nodeId =
                    attachments.nodeIdFor(heartbeat.deviceId).orElse(null);

            if (nodeId == null) {
                sendJson(
                        exchange,
                        404,
                        """
                        {"status":"error","message":"unknown_device"}
                        """
                );
                return;
            }

            ResourceStatus status = heartbeat.toResourceStatus();
            double cpuUsage = status.cpuUsagePercent();

            Optional<NodeRecord> existing =
                    nodeTelemetryService.get(nodeId);

            if (existing.isEmpty()) {
                attachments.detach(heartbeat.deviceId);
                sendJson(
                        exchange,
                        404,
                        """
                        {"status":"error","message":"node_not_found"}
                        """
                );
                return;
            }

            NodeStatus previousState =
                    existing.get().state();

            nodeTelemetryService.reportHeartbeat(
                    nodeId,
                    status
            );

            if (previousState == NodeStatus.DEGRADED
                    || previousState == NodeStatus.OFFLINE) {

                System.out.printf(
                        "[RegistrationServer] Heartbeat resumed from node %s -> ONLINE%n",
                        nodeId
                );
            }

            System.out.printf(
                    "[RegistrationServer] Heartbeat from %s: cpu=%s freeRam=%dMB%n",
                    heartbeat.deviceId,
                    cpuUsage >= 0
                            ? String.format(
                                    "%.1f%%",
                                    cpuUsage
                            )
                            : "N/A",
                    heartbeat.availableRamMb
            );

            sendJson(
                    exchange,
                    200,
                    "{\"status\":\"ok\",\"nodeId\":\""
                            + nodeId
                            + "\"}"
            );

        } catch (Exception exception) {
            System.out.println(
                    "[RegistrationServer] Heartbeat rejected: "
                            + exception.getMessage()
            );

            sendError(exchange, exception);
        }
    }

    /**
     * Hands the next task queued for a device to that device.
     *
     * <p>{@code GET /tasks/poll?deviceId=<id>} answers 200 with the task body
     * (see {@link TaskWire}), 204 when there is nothing to deliver or the node
     * is not ONLINE, 400 without a device id and 404 for an unknown device.
     */
    private void handlePoll(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        try {
            String deviceId = extractQueryParameter(
                    exchange.getRequestURI().getQuery(),
                    "deviceId");

            if (deviceId == null || deviceId.isBlank()) {
                sendError(exchange, 400, "deviceId is required");
                return;
            }

            String nodeId = attachments.nodeIdFor(deviceId).orElse(null);
            if (nodeId == null) {
                sendError(exchange, 404, "unknown_device");
                return;
            }

            // A polling node is in contact even when it receives no work; this
            // is what separates queue backlog from an unreachable node.
            taskQueue.recordPoll(deviceId);

            Optional<NodeRecord> record = nodeTelemetryService.get(nodeId);
            if (record.isEmpty() || record.get().state() != NodeStatus.ONLINE) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            Optional<TaskQueueService.Delivery> delivery = taskQueue.pollDelivery(deviceId);
            if (delivery.isEmpty()) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            sendJson(exchange, 200, TaskWire.taskBody(
                    delivery.get().task(),
                    delivery.get().fencing(),
                    delivery.get().attemptId(),
                    delivery.get().attemptNumber()));

        } catch (Exception exception) {
            System.out.println("[RegistrationServer] /tasks/poll failed: " + exception);
            sendError(exchange, 500, exception.getMessage());
        }
    }

    /**
     * Optional node acknowledgement that execution of a delivered task began.
     *
     * POST /tasks/started   {"taskId": "task-001"}
     *
     * Agents that do not send this still work: Core then measures the
     * execution timeout from delivery and reports the start as unacknowledged.
     */
    private void handleStarted(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        try {
            StartedBody started =
                    gson.fromJson(readBody(exchange), StartedBody.class);

            if (started == null
                    || started.taskId == null
                    || started.taskId.isBlank()) {

                sendJson(
                        exchange,
                        400,
                        "{\"status\":\"error\",\"message\":\"invalid_started\"}"
                );
                return;
            }

            if (!taskQueue.markStarted(started.taskId, started.attemptId)) {
                sendJson(
                        exchange,
                        404,
                        "{\"status\":\"error\",\"message\":\"unknown_task\"}"
                );
                return;
            }

            sendJson(exchange, 200, "{\"status\":\"ok\"}");

        } catch (Exception exception) {
            sendError(exchange, exception);
        }
    }

    private static class StartedBody {
        String taskId;

        /** Optional echo of the attemptId Core sent with the task. */
        String attemptId;
    }

    /**
     * Extract a single URL query parameter.
     */
    private static String extractQueryParameter(
            String query,
            String requestedName) {

        if (query == null || query.isBlank()) {
            return null;
        }

        for (String part : query.split("&")) {
            String[] pair = part.split("=", 2);

            if (pair.length != 2) {
                continue;
            }

            if (pair[0].equals(requestedName)) {
                return URLDecoder.decode(
                        pair[1],
                        StandardCharsets.UTF_8
                );
            }
        }

        return null;
    }

    /**
     * Receive a completed task result from a Node Agent.
     *
     * POST /results
     */
    private void handleResult(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        try {
            String body = readBody(exchange);

            ResultBody resultBody =
                    gson.fromJson(body, ResultBody.class);

            if (resultBody == null
                    || resultBody.taskId == null
                    || resultBody.status == null) {

                sendJson(
                        exchange,
                        400,
                        """
                        {"status":"error","message":"invalid_result"}
                        """
                );
                return;
            }

            TaskResult result = new TaskResult(
                    resultBody.taskId,
                    resultBody.status,
                    resultBody.outputData
            );

            boolean matched =
                    taskQueue.completeResult(result, resultBody.attemptId);

            if (!matched) {
                sendJson(
                        exchange,
                        404,
                        """
                        {"status":"error","message":"unknown_task"}
                        """
                );
                return;
            }

            System.out.printf(
                    "[RegistrationServer] Result received: task=%s status=%s%n",
                    result.taskId(),
                    result.status()
            );

            sendJson(
                    exchange,
                    200,
                    """
                    {"status":"ok"}
                    """
            );

        } catch (Exception exception) {
            sendError(exchange, exception);
        }
    }

    /**
     * Result posted by a node agent. Agents also send {@code executionTimeMs};
     * it is not part of {@link TaskResult} yet, so the field is ignored.
     */
    private static class ResultBody {
        String taskId;
        String status;
        String outputData;

        /** Optional echo of the attemptId Core sent with the task. */
        String attemptId;
    }

    /**
     * Reads the request body, refusing anything larger than
     * {@link #MAX_BODY_BYTES} so a misbehaving client cannot exhaust memory.
     */
    private static String readBody(
            HttpExchange exchange) throws IOException {

        try (var inputStream = exchange.getRequestBody()) {
            byte[] bytes = inputStream.readNBytes(MAX_BODY_BYTES + 1);

            if (bytes.length > MAX_BODY_BYTES) {
                throw new BodyTooLargeException();
            }

            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    /** 413 for an oversized body, otherwise 400 with the exception message. */
    private static void sendError(
            HttpExchange exchange,
            Exception exception) throws IOException {

        if (exception instanceof BodyTooLargeException) {
            sendError(exchange, 413, "request_body_too_large");
        } else {
            sendError(exchange, 400, exception.getMessage());
        }
    }

    private static void sendError(
            HttpExchange exchange,
            int statusCode,
            String message) throws IOException {

        sendJson(
                exchange,
                statusCode,
                "{\"status\":\"error\",\"message\":\""
                        + JsonText.escape(message)
                        + "\"}"
        );
    }

    private static void sendJson(
            HttpExchange exchange,
            int statusCode,
            String body) throws IOException {

        byte[] bytes =
                body.getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders()
                .set("Content-Type", "application/json");

        exchange.sendResponseHeaders(
                statusCode,
                bytes.length
        );

        try (OutputStream outputStream =
                     exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }

    /** Raised by {@link #readBody} for a body above {@link #MAX_BODY_BYTES}. */
    private static final class BodyTooLargeException extends IOException {

        private static final long serialVersionUID = 1L;

        BodyTooLargeException() {
            super("request body exceeds " + MAX_BODY_BYTES + " bytes");
        }
    }

    /** Heartbeat posted by a node agent. */
    public static class HeartbeatMessage {

        public String deviceId;
        /** Optional; absent means the agent cannot measure it. */
        public Double cpuUsagePercent;
        public long availableRamMb;
        public int activeTasks;
        public int queueLength;
        public double networkLatencyMs;
        public double energyCostEstimate;

        /**
         * The resource snapshot this heartbeat reports, observed now.
         *
         * <p>A missing or negative {@code cpuUsagePercent} means the platform
         * cannot measure it, never 0 %. Shared by every transport.
         *
         * @return the snapshot
         */
        public ResourceStatus toResourceStatus() {

            double cpuUsage =
                    cpuUsagePercent == null
                            ? ResourceStatus.CPU_UNKNOWN
                            : cpuUsagePercent;

            return ResourceStatus.of(
                    cpuUsage,
                    availableRamMb,
                    activeTasks,
                    queueLength,
                    networkLatencyMs,
                    energyCostEstimate
            );
        }
    }
}
