// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The Phoenix MQTT topic namespace (protocol version 1).
 *
 * <pre>
 * {prefix}/v1/core/status                       Core -&gt; all     retained online/offline
 * {prefix}/v1/nodes/{deviceId}/up/{kind}        node -&gt; Core    register, heartbeat, ack,
 *                                                                started, result, status
 * {prefix}/v1/nodes/{deviceId}/down/{kind}      Core -&gt; node    registered, task, cancel,
 *                                                                control
 * </pre>
 *
 * <p>Every topic a node uses contains its own device id, and the direction
 * is a separate level. That makes broker access control one rule per
 * direction: a node may publish only below {@code nodes/<its id>/up/} and
 * read only below {@code nodes/<its id>/down/} (see
 * {@code deploy/mosquitto/acl}). Because the device id comes from the topic,
 * and the broker decides who may publish on a topic, a node cannot speak for
 * another node even if it lies in a message body.
 *
 * <p>Per-task topics are deliberately not used: a node would have to
 * subscribe to topics it cannot know in advance, and they could not be
 * restricted to one node.
 */
public final class MqttTopics {

    /** Protocol version, also the second topic level. */
    public static final int PROTOCOL_VERSION = 1;

    private static final Pattern SEGMENT =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");

    private final String base;

    /** Messages a node publishes to Core. */
    public enum Up {
        REGISTER, HEARTBEAT, ACK, STARTED, RESULT, STATUS;

        /** The topic level for this kind. */
        public String level() {
            return name().toLowerCase(Locale.ROOT);
        }

        static Optional<Up> fromLevel(String level) {
            for (Up kind : values()) {
                if (kind.level().equals(level)) {
                    return Optional.of(kind);
                }
            }
            return Optional.empty();
        }
    }

    /** Messages Core publishes to one node. */
    public enum Down {
        REGISTERED, TASK, CANCEL, CONTROL;

        /** The topic level for this kind. */
        public String level() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * A message a node published, located by its topic.
     *
     * @param deviceId the publishing device, taken from the topic
     * @param kind     what the message is
     */
    public record Inbound(String deviceId, Up kind) {}

    public MqttTopics(String prefix) {
        if (!isValidPrefix(prefix)) {
            throw new IllegalArgumentException("invalid topic prefix: " + prefix);
        }
        this.base = prefix + "/v" + PROTOCOL_VERSION;
    }

    /** {@code {prefix}/v1}. */
    public String base() {
        return base;
    }

    /** Retained Core status, read by every node. */
    public String coreStatus() {
        return base + "/core/status";
    }

    /** Filter Core subscribes to: everything every node publishes. */
    public String allUplinks() {
        return base + "/nodes/+/up/#";
    }

    public String up(String deviceId, Up kind) {
        return node(deviceId) + "/up/" + kind.level();
    }

    public String down(String deviceId, Down kind) {
        return node(deviceId) + "/down/" + kind.level();
    }

    /**
     * Locates an uplink topic. Anything that is not exactly
     * {@code {base}/nodes/{valid id}/up/{known kind}} is rejected.
     */
    public Optional<Inbound> parseUplink(String topic) {

        if (topic == null || !topic.startsWith(base + "/nodes/")) {
            return Optional.empty();
        }

        String[] levels =
                topic.substring(base.length() + "/nodes/".length()).split("/", -1);

        if (levels.length != 3
                || !"up".equals(levels[1])
                || !isValidDeviceId(levels[0])) {
            return Optional.empty();
        }

        return Up.fromLevel(levels[2])
                .map(kind -> new Inbound(levels[0], kind));
    }

    /**
     * Whether {@code deviceId} can be used as a topic level: 1-128 characters
     * of {@code [A-Za-z0-9._-]}, starting with a letter or digit. This excludes
     * the MQTT wildcards {@code +} and {@code #}, the level separator and
     * anything a broker ACL pattern could misread.
     */
    public static boolean isValidDeviceId(String deviceId) {
        return isValidSegment(deviceId);
    }

    static boolean isValidSegment(String segment) {
        return segment != null && SEGMENT.matcher(segment).matches();
    }

    static boolean isValidPrefix(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return false;
        }

        for (String level : prefix.split("/", -1)) {
            if (!isValidSegment(level)) {
                return false;
            }
        }

        return true;
    }

    private String node(String deviceId) {
        Objects.requireNonNull(deviceId, "deviceId");

        if (!isValidDeviceId(deviceId)) {
            throw new IllegalArgumentException("invalid device id: " + deviceId);
        }

        return base + "/nodes/" + deviceId;
    }
}
