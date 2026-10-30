// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Core's single link to the MQTT broker.
 *
 * <p>Owns the {@link MqttConnection}, Core's retained presence on
 * {@code {prefix}/v1/core/status} and the subscription to every node uplink.
 * The gateway and the agent client publish through it; neither knows which
 * MQTT library is underneath.
 *
 * <p>Core's presence:
 * <ul>
 *     <li>on every (re)connect: {@code {"state":"online","coreInstanceId":...}},
 *         retained;</li>
 *     <li>on a clean stop: {@code "offline"}, retained;</li>
 *     <li>if Core dies without stopping, the broker publishes the same
 *         {@code "offline"} message as Core's last will.</li>
 * </ul>
 * The instance id is new for every Core process, so a node that sees a
 * different id knows Core restarted and has forgotten it, and registers
 * again.
 */
@Singleton
public class MqttBrokerLink {

    /** QoS for everything that must not be lost; heartbeats use 0. */
    public static final int QOS_AT_LEAST_ONCE = 1;

    private final MqttConfig config;
    private final Function<MqttConfig, MqttConnection> connections;
    private final MqttTopics topics;
    private final String coreInstanceId = UUID.randomUUID().toString();

    private volatile MqttConnection connection;

    @Inject
    public MqttBrokerLink(MqttConfig config) {
        this(config, PahoMqttConnection::new);
    }

    /** Test seam: supply the connection implementation. */
    public MqttBrokerLink(
            MqttConfig config,
            Function<MqttConfig, MqttConnection> connections) {

        this.config = Objects.requireNonNull(config, "config");
        this.connections = Objects.requireNonNull(connections, "connections");
        this.topics = new MqttTopics(config.topicPrefix());
    }

    public boolean enabled() {
        return config.enabled();
    }

    public MqttConfig config() {
        return config;
    }

    public MqttTopics topics() {
        return topics;
    }

    /** Identity of this Core process, announced on the core status topic. */
    public String coreInstanceId() {
        return coreInstanceId;
    }

    /**
     * Connects and delivers every node uplink to {@code uplinks}. Returns at
     * once; the connection is made, and remade, in the background.
     *
     * @throws IllegalStateException if MQTT is not configured
     */
    public synchronized void start(MqttConnection.MessageHandler uplinks) {

        if (!config.enabled()) {
            throw new IllegalStateException("MQTT is not configured (phoenix.mqtt.url)");
        }

        if (connection != null) {
            return;
        }

        MqttConnection created = connections.apply(config);

        created.lastWill(
                topics.coreStatus(),
                MqttWire.coreStatus("offline", coreInstanceId),
                QOS_AT_LEAST_ONCE,
                true);

        created.subscribe(topics.allUplinks(), QOS_AT_LEAST_ONCE, uplinks);

        created.addListener(new MqttConnection.Listener() {
            @Override
            public void connected(boolean reconnect) {
                created.publish(
                        topics.coreStatus(),
                        MqttWire.coreStatus("online", coreInstanceId),
                        QOS_AT_LEAST_ONCE,
                        true);
            }

            @Override
            public void disconnected(String cause) {
                // Nodes see Core's last will; reconnection is automatic.
            }
        });

        System.out.printf(
                "[MQTT] Connecting to %s as %s (topics %s/..., TLS %s, instance %s)%n",
                config.serverUri(),
                config.clientId(),
                topics.base(),
                config.usesTls() ? "on" : "off",
                coreInstanceId.substring(0, 8));

        connection = created;
        created.open();
    }

    /** Announces Core offline and disconnects. Safe to call repeatedly. */
    public synchronized void stop() {

        MqttConnection current = connection;
        connection = null;

        if (current == null) {
            return;
        }

        if (current.isConnected()) {
            current.publish(
                    topics.coreStatus(),
                    MqttWire.coreStatus("offline", coreInstanceId),
                    QOS_AT_LEAST_ONCE,
                    true);
        }

        current.close();
    }

    /** Publishes a non-retained message; false if it was not accepted. */
    public boolean publish(String topic, byte[] payload, int qos) {
        MqttConnection current = connection;
        return current != null && current.publish(topic, payload, qos, false);
    }

    public boolean isConnected() {
        MqttConnection current = connection;
        return current != null && current.isConnected();
    }

    /** Waits up to {@code timeout} for the link; false if not started. */
    public boolean awaitConnected(Duration timeout) throws InterruptedException {
        MqttConnection current = connection;
        return current != null && current.awaitConnected(timeout);
    }

    /** {@code off}, {@code connected} or {@code connecting}, for the console. */
    public String statusLabel() {
        if (!config.enabled()) {
            return "off";
        }
        return isConnected() ? "connected" : "connecting";
    }
}
