// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * MQTT sessions of registered devices.
 *
 * <p>A session starts when a device registers and holds the
 * {@code sessionToken} Core issued for it. Every later message from the
 * device must carry that token. A new registration issues a new token and
 * invalidates the old one, so a stale copy of an agent (an old process that
 * kept running, a device restored from a backup) cannot report results or
 * telemetry for the node once its successor registered.
 *
 * <p>The table also records when Core last heard from each device. That is
 * the MQTT transport's evidence that a node is in contact while a task waits
 * to be acknowledged, the same role the last poll plays for HTTP.
 *
 * <p>Sessions live in memory: after a Core restart every device registers
 * again, prompted by the new Core instance id or by a {@code reregister}
 * control message.
 */
@Singleton
public class MqttSessions {

    private static final SecureRandom RANDOM = new SecureRandom();

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastContact = new ConcurrentHashMap<>();
    private final Clock clock;

    @Inject
    public MqttSessions() {
        this(Clock.systemUTC());
    }

    /** Test seam: deterministic time. */
    public MqttSessions(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * One device's session.
     *
     * @param deviceId   the device (also its topic level)
     * @param nodeId     the node Core registered it as
     * @param token      the current session token
     * @param requestId  the registration request that created the session
     * @param response   Core's answer to that request, resent for duplicates
     */
    public record Session(
            String deviceId,
            String nodeId,
            String token,
            String requestId,
            byte[] response
    ) {}

    /** Starts a session with a fresh token, replacing any previous one. */
    public Session open(
            String deviceId,
            String nodeId,
            String requestId,
            Function<String, byte[]> responseForToken) {

        String token = newToken();
        Session session = new Session(
                deviceId, nodeId, token, requestId, responseForToken.apply(token));
        sessions.put(deviceId, session);
        touch(deviceId);
        return session;
    }

    public Optional<Session> get(String deviceId) {
        return Optional.ofNullable(sessions.get(deviceId));
    }

    /**
     * The session if {@code token} is its current token.
     * Compared in constant time.
     */
    public Optional<Session> verify(String deviceId, String token) {

        Session session = sessions.get(deviceId);

        if (session == null || token == null) {
            return Optional.empty();
        }

        boolean equal = MessageDigest.isEqual(
                session.token().getBytes(StandardCharsets.UTF_8),
                token.getBytes(StandardCharsets.UTF_8));

        return equal ? Optional.of(session) : Optional.empty();
    }

    /** Ends a device's session. */
    public void close(String deviceId) {
        sessions.remove(deviceId);
    }

    /** Records that Core just heard from the device. */
    public void touch(String deviceId) {
        lastContact.put(deviceId, clock.instant());
    }

    /** Time since Core last heard from the device; empty if never. */
    public Optional<Duration> timeSinceContact(String deviceId) {
        Instant last = lastContact.get(deviceId);

        return last == null
                ? Optional.empty()
                : Optional.of(Duration.between(last, clock.instant()));
    }

    private static String newToken() {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
