// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.core.NodeAttachments;
import com.phoenix.core.NodeTelemetryService;
import com.phoenix.model.NodeRegistrationRequest;
import com.phoenix.model.ResourceStatus;
import com.phoenix.net.ProtocolFeatures;

/**
 * MQTT gateway through which node agents register, report telemetry and
 * report on the tasks they were sent. The MQTT counterpart of
 * {@link com.phoenix.net.RegistrationServer}.
 *
 * <p>It decodes the wire format and translates it into the same
 * transport-neutral calls the HTTP gateway makes:
 * {@link NodeTelemetryService#register}, {@link NodeTelemetryService#reportHeartbeat}
 * and the shared {@link NodeAttachments} table. It owns no lifecycle policy:
 * ONLINE/DEGRADED/OFFLINE is decided by
 * {@link com.phoenix.core.NodeHealthMonitor} from the heartbeats this gateway
 * feeds in, exactly as for HTTP nodes.
 *
 * <h2>Authentication</h2>
 * <ol>
 *     <li><b>Broker</b>: TLS and per-device broker credentials with an ACL
 *         that confines each device to its own topics. The device id is
 *         always taken from the topic, never from the body.</li>
 *     <li><b>Enrollment</b>: if an enrollment token is configured, a
 *         registration must present it.</li>
 *     <li><b>Session</b>: registration returns a session token; every later
 *         message must carry it ({@link MqttSessions}).</li>
 * </ol>
 */
@Singleton
public class MqttNodeGateway {

    /** Transport id recorded in {@link NodeAttachments} for MQTT nodes. */
    public static final String TRANSPORT_ID = "mqtt";

    /** At most one reregister prompt per device in this interval. */
    static final Duration REREGISTER_PROMPT_INTERVAL = Duration.ofSeconds(5);

    private final MqttBrokerLink broker;
    private final NodeTelemetryService telemetry;
    private final NodeAttachments attachments;
    private final MqttSessions sessions;
    private final MqttAttempts attempts;
    private final Clock clock;

    private final Map<String, Instant> lastPrompt = new ConcurrentHashMap<>();
    private final AtomicLong rejectedMessages = new AtomicLong();

    private boolean started;

    @Inject
    public MqttNodeGateway(
            MqttBrokerLink broker,
            NodeTelemetryService telemetry,
            NodeAttachments attachments,
            MqttSessions sessions,
            MqttAttempts attempts) {

        this(broker, telemetry, attachments, sessions, attempts, Clock.systemUTC());
    }

    /** Test seam: deterministic time. */
    public MqttNodeGateway(
            MqttBrokerLink broker,
            NodeTelemetryService telemetry,
            NodeAttachments attachments,
            MqttSessions sessions,
            MqttAttempts attempts,
            Clock clock) {

        this.broker = broker;
        this.telemetry = telemetry;
        this.attachments = attachments;
        this.sessions = sessions;
        this.attempts = attempts;
        this.clock = clock;
    }

    /**
     * Connects to the broker if MQTT is configured; otherwise does nothing.
     * Safe to call repeatedly.
     */
    public synchronized void start() {

        if (started || !broker.enabled()) {
            return;
        }

        if (broker.config().enrollmentToken().isEmpty()) {
            System.out.println(
                    "[MQTT] WARNING no enrollment token configured: any client the "
                            + "broker admits can register a node. Set "
                            + "phoenix.mqtt.enrollmentTokenFile or "
                            + "PHOENIX_MQTT_ENROLLMENT_TOKEN.");
        }

        broker.start(this::onUplink);
        started = true;
    }

    public synchronized void stop() {
        if (started) {
            broker.stop();
            started = false;
        }
    }

    /** Messages dropped for a bad token, a bad body or an unknown device. */
    public long rejectedMessageCount() {
        return rejectedMessages.get();
    }

    /**
     * Entry point for every uplink message. Public so tests and alternative
     * connections can feed messages in directly.
     */
    public void onUplink(String topic, byte[] payload) {

        Optional<MqttTopics.Inbound> inbound = broker.topics().parseUplink(topic);

        if (inbound.isEmpty()) {
            rejected("message on unexpected topic " + MqttAttempts.abbreviate(topic));
            return;
        }

        String deviceId = inbound.get().deviceId();

        if (payload.length > MqttWire.MAX_PAYLOAD_BYTES) {
            rejected(inbound.get().kind().level() + " from " + deviceId
                    + " exceeds " + MqttWire.MAX_PAYLOAD_BYTES + " bytes");
            return;
        }

        switch (inbound.get().kind()) {
            case REGISTER -> onRegister(deviceId, payload);
            case HEARTBEAT -> onHeartbeat(deviceId, payload);
            case ACK -> onAttemptReport(deviceId, payload, false);
            case STARTED -> onAttemptReport(deviceId, payload, true);
            case RESULT -> onResult(deviceId, payload);
            case STATUS -> onStatus(deviceId, payload);
        }
    }

    // ------------------------------------------------------------------
    // registration
    // ------------------------------------------------------------------

    private void onRegister(String deviceId, byte[] payload) {

        Optional<MqttWire.Register> parsed = MqttWire.parse(payload, MqttWire.Register.class);

        if (parsed.isEmpty()) {
            refuse(deviceId, null, "invalid_registration");
            return;
        }

        MqttWire.Register request = parsed.get();

        if (request.deviceId == null) {
            request.deviceId = deviceId;
        } else if (!request.deviceId.equals(deviceId)) {
            refuse(deviceId, request.requestId, "device_id_mismatch");
            return;
        }

        if (request.protocolVersion != null
                && request.protocolVersion != MqttTopics.PROTOCOL_VERSION) {
            refuse(deviceId, request.requestId, "unsupported_protocol_version");
            return;
        }

        if (!enrollmentAccepted(request.enrollmentToken)) {
            refuse(deviceId, request.requestId, "enrollment_rejected");
            return;
        }

        // A repeated request (QoS 1 redelivery, or the agent retrying after a
        // lost answer) gets the original answer, not a second session.
        Optional<MqttSessions.Session> existing = sessions.get(deviceId);

        if (request.requestId != null
                && existing.isPresent()
                && request.requestId.equals(existing.get().requestId())) {

            broker.publish(
                    broker.topics().down(deviceId, MqttTopics.Down.REGISTERED),
                    existing.get().response(),
                    MqttBrokerLink.QOS_AT_LEAST_ONCE);
            return;
        }

        try {
            request.validate();
        } catch (IllegalArgumentException exception) {
            refuse(deviceId, request.requestId, exception.getMessage());
            return;
        }

        if (request.protocolFeatures != null
                && !request.protocolFeatures.contains(ProtocolFeatures.ATTEMPT_ID)) {
            System.out.printf(
                    "[MQTT] Node %s does not list %s; MQTT agents must echo attempt "
                            + "ids, so it will be held to it anyway%n",
                    deviceId,
                    ProtocolFeatures.ATTEMPT_ID);
        }

        NodeRegistrationRequest registration = new NodeRegistrationRequest(
                null,
                request.displayName,
                request.hardwareClass,
                request.architecture,
                request.cpuCores,
                request.memoryMb,
                request.adapterType,
                request.capabilities != null ? request.capabilities : Set.of(),
                ResourceStatus.of(-1.0, 0, 0, 0, 0.0, 3.0));

        NodeAttachments.Attachment attachment = attachments.attach(
                deviceId,
                TRANSPORT_ID,
                () -> telemetry.register(registration).nodeId());

        MqttSessions.Session session = sessions.open(
                deviceId,
                attachment.nodeId(),
                request.requestId,
                token -> MqttWire.registered(
                        request.requestId,
                        attachment.nodeId(),
                        token,
                        broker.config().heartbeatInterval().toSeconds(),
                        broker.coreInstanceId()));

        lastPrompt.remove(deviceId);

        broker.publish(
                broker.topics().down(deviceId, MqttTopics.Down.REGISTERED),
                session.response(),
                MqttBrokerLink.QOS_AT_LEAST_ONCE);

        if (attachment.created()) {
            System.out.printf(
                    "[MQTT] Device registered: %s nodeId=%s (%s, %s)%n",
                    request.displayName,
                    attachment.nodeId(),
                    request.hardwareClass,
                    request.adapterType);
        } else {
            System.out.printf(
                    "[MQTT] Device %s registered again as node %s%s; new session%n",
                    deviceId,
                    attachment.nodeId(),
                    attachment.movedFrom()
                            .map(previous -> " (moved from " + previous + " to MQTT)")
                            .orElse(""));
        }
    }

    private boolean enrollmentAccepted(String presented) {

        Optional<String> expected = broker.config().enrollmentToken();

        if (expected.isEmpty()) {
            return true;
        }

        return presented != null
                && MessageDigest.isEqual(
                        expected.get().getBytes(StandardCharsets.UTF_8),
                        presented.getBytes(StandardCharsets.UTF_8));
    }

    private void refuse(String deviceId, String requestId, String reason) {

        rejected("registration from " + deviceId + " refused: " + reason);

        broker.publish(
                broker.topics().down(deviceId, MqttTopics.Down.REGISTERED),
                MqttWire.registrationRefused(requestId, reason),
                MqttBrokerLink.QOS_AT_LEAST_ONCE);
    }

    // ------------------------------------------------------------------
    // telemetry
    // ------------------------------------------------------------------

    private void onHeartbeat(String deviceId, byte[] payload) {

        Optional<MqttWire.Heartbeat> heartbeat =
                MqttWire.parse(payload, MqttWire.Heartbeat.class);

        if (heartbeat.isEmpty()) {
            rejected("malformed heartbeat from " + deviceId);
            return;
        }

        Optional<MqttSessions.Session> session =
                authenticate(deviceId, heartbeat.get().sessionToken, "heartbeat");

        if (session.isEmpty()) {
            return;
        }

        String nodeId = session.get().nodeId();

        if (telemetry.get(nodeId).isEmpty()) {
            // The node left the registry; the device must enroll again.
            sessions.close(deviceId);
            attachments.detach(deviceId);
            promptReregister(deviceId, "node_not_found");
            return;
        }

        telemetry.reportHeartbeat(nodeId, heartbeat.get().toResourceStatus());
    }

    /**
     * {@code up/status} carries the node's presence; it is also the node's
     * last will, published by the broker when the node vanishes. It cannot
     * carry a session token, so it is only a hint: "online" counts as contact,
     * "offline" is logged. Node health stays with the heartbeat monitor.
     */
    private void onStatus(String deviceId, byte[] payload) {

        Optional<MqttWire.Status> status = MqttWire.parse(payload, MqttWire.Status.class);

        if (status.isEmpty() || status.get().state == null) {
            return;
        }

        if (sessions.get(deviceId).isEmpty()) {
            return; // retained presence of a device this Core never met
        }

        if ("online".equals(status.get().state)) {
            sessions.touch(deviceId);
        } else if ("offline".equals(status.get().state)) {
            System.out.printf(
                    "[MQTT] Device %s disconnected from the broker%n", deviceId);
        }
    }

    // ------------------------------------------------------------------
    // task reports
    // ------------------------------------------------------------------

    private void onAttemptReport(String deviceId, byte[] payload, boolean started) {

        Optional<MqttWire.AttemptReport> report =
                MqttWire.parse(payload, MqttWire.AttemptReport.class);

        String what = started ? "start report" : "ack";

        if (report.isEmpty()) {
            rejected("malformed " + what + " from " + deviceId);
            return;
        }

        if (authenticate(deviceId, report.get().sessionToken, what).isEmpty()) {
            return;
        }

        if (started) {
            attempts.started(deviceId, report.get());
        } else {
            attempts.acknowledged(deviceId, report.get());
        }
    }

    private void onResult(String deviceId, byte[] payload) {

        Optional<MqttWire.Result> result = MqttWire.parse(payload, MqttWire.Result.class);

        if (result.isEmpty()) {
            rejected("malformed result from " + deviceId);
            return;
        }

        if (authenticate(deviceId, result.get().sessionToken, "result").isEmpty()) {
            return;
        }

        if (attempts.resulted(deviceId, result.get())) {
            System.out.printf(
                    "[MQTT] Result received: task=%s status=%s from %s%n",
                    MqttAttempts.abbreviate(result.get().taskId),
                    MqttAttempts.abbreviate(result.get().status),
                    deviceId);
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private Optional<MqttSessions.Session> authenticate(
            String deviceId,
            String token,
            String what) {

        Optional<MqttSessions.Session> session = sessions.verify(deviceId, token);

        if (session.isEmpty()) {
            rejected(what + " from " + deviceId + ": "
                    + (sessions.get(deviceId).isEmpty()
                            ? "device has no session"
                            : "stale or missing session token"));
            promptReregister(deviceId,
                    sessions.get(deviceId).isEmpty() ? "unknown_device" : "invalid_session");
            return Optional.empty();
        }

        sessions.touch(deviceId);
        return session;
    }

    /** Tells a device to register again, at most once per interval. */
    private void promptReregister(String deviceId, String reason) {

        Instant now = clock.instant();
        Instant previous = lastPrompt.get(deviceId);

        if (previous != null
                && Duration.between(previous, now).compareTo(REREGISTER_PROMPT_INTERVAL) < 0) {
            return;
        }

        lastPrompt.put(deviceId, now);

        broker.publish(
                broker.topics().down(deviceId, MqttTopics.Down.CONTROL),
                MqttWire.reregister(reason),
                MqttBrokerLink.QOS_AT_LEAST_ONCE);
    }

    private void rejected(String message) {
        rejectedMessages.incrementAndGet();
        System.out.println("[MQTT] Rejected " + message);
    }
}
