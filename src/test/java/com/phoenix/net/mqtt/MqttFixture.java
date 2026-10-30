// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.phoenix.core.DeviceRegistry;
import com.phoenix.core.NodeAttachments;
import com.phoenix.core.NodeHealthMonitor;
import com.phoenix.core.NodeTelemetryService;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.core.TaskTimeoutPolicy;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;

/**
 * Core's MQTT transport wired to an {@link InMemoryMqttBroker}, with every
 * collaborator real except the broker.
 */
final class MqttFixture {

    static final String ENROLLMENT_TOKEN = "enroll-0123456789abcdef";

    final InMemoryMqttBroker broker = new InMemoryMqttBroker();
    final DeviceRegistry registry = new DeviceRegistry();
    final NodeHealthMonitor monitor = new NodeHealthMonitor(registry);
    final NodeTelemetryService telemetry = new NodeTelemetryService(registry, monitor);
    final TaskLifecycleService lifecycle = new TaskLifecycleService();
    final NodeAttachments attachments = new NodeAttachments();
    final MqttSessions sessions = new MqttSessions();
    final MqttAttempts attempts = new MqttAttempts(lifecycle);
    final MqttConfig config;
    final MqttBrokerLink link;
    final MqttNodeGateway gateway;
    final MqttNodeAgentClient client;

    /** Holds Core's broker connection once started. */
    InMemoryMqttBroker.Connection coreConnection;

    MqttFixture() {
        this(true, Duration.ofMillis(600), Duration.ofMillis(600), Duration.ofMillis(300));
    }

    MqttFixture(
            boolean requireEnrollment,
            Duration delivery,
            Duration execution,
            Duration contactLiveness) {

        this(inMemoryConfig(requireEnrollment), null, delivery, execution, contactLiveness);
    }

    /**
     * Core's transport on a caller-supplied connection, for example Paho to a
     * real broker; {@code connections == null} means the in-memory broker.
     */
    MqttFixture(
            MqttConfig config,
            Function<MqttConfig, MqttConnection> connections,
            Duration delivery,
            Duration execution,
            Duration contactLiveness) {

        this.config = config;

        link = new MqttBrokerLink(config, connections != null ? connections : cfg -> {
            coreConnection = broker.connect(cfg.clientId());
            return coreConnection;
        });

        gateway = new MqttNodeGateway(link, telemetry, attachments, sessions, attempts);

        client = new MqttNodeAgentClient(
                link, attachments, sessions, attempts, lifecycle,
                new TaskTimeoutPolicy(delivery, execution, contactLiveness));

        gateway.start();
    }

    private static MqttConfig inMemoryConfig(boolean requireEnrollment) {

        Map<String, String> properties = new HashMap<>();
        properties.put("phoenix.mqtt.url", "tcp://127.0.0.1:1883");

        Map<String, String> environment = new HashMap<>();
        if (requireEnrollment) {
            environment.put("PHOENIX_MQTT_ENROLLMENT_TOKEN", ENROLLMENT_TOKEN);
        }

        return MqttConfig.from(properties::get, environment::get);
    }

    /** A registered agent with a session. */
    FakeMqttAgent agent(String deviceId) {
        FakeMqttAgent agent = new FakeMqttAgent(broker, link.topics(), deviceId);
        agent.register(ENROLLMENT_TOKEN);
        if (agent.sessionToken == null) {
            throw new AssertionError("registration failed: " + agent.registrations);
        }
        return agent;
    }

    NodeExecutionTarget target(FakeMqttAgent agent) {
        return new NodeExecutionTarget(agent.nodeId, "fake-" + agent.deviceId, "android");
    }

    static TaskDescriptor task(String id) {
        return new TaskDescriptor(id, "Monte-Carlo-Pi", 1, 128);
    }
}
