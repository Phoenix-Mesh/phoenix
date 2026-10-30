// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.net.BrokerTransport;
import com.phoenix.net.NodeAgentClient;
import com.phoenix.net.NodeTransportRouter;
import com.phoenix.net.PullTransport;
import com.phoenix.net.PushTransport;
import com.phoenix.net.RegistrationServer;
import com.phoenix.net.mqtt.MqttBrokerLink;
import com.phoenix.net.mqtt.MqttNodeGateway;

/**
 * Every adapter reaches its nodes through a router, every gateway shares one
 * identity table, and without phoenix.mqtt.url the MQTT transport stays off.
 */
class TransportWiringTest {

    private final Injector injector = Guice.createInjector(new PhoenixModule());

    @Test
    void everyAdapterTransportIsARouter() {
        for (Class<? extends java.lang.annotation.Annotation> binding :
                Set.of(PullTransport.class, PushTransport.class, BrokerTransport.class)) {
            assertInstanceOf(NodeTransportRouter.class,
                    injector.getInstance(Key.get(NodeAgentClient.class, binding)));
        }
    }

    @Test
    void withoutABrokerUrlMqttIsOffAndStartingItDoesNothing() {
        MqttBrokerLink link = injector.getInstance(MqttBrokerLink.class);
        MqttNodeGateway gateway = injector.getInstance(MqttNodeGateway.class);

        assertFalse(link.enabled());
        assertEquals("off", link.statusLabel());

        gateway.start();
        gateway.stop();
        assertFalse(link.isConnected());
    }

    @Test
    void anEsp32NodeNotAttachedOverMqttStillFailsRetryably() {
        NodeAgentClient esp32 =
                injector.getInstance(Key.get(NodeAgentClient.class, BrokerTransport.class));

        var result = esp32.execute(
                new NodeExecutionTarget("n1", "esp32-node", "esp32"),
                new TaskDescriptor("task-001", "Monte-Carlo-Pi", 1, 128));

        assertEquals(TaskFailureReason.TRANSPORT_ERROR, result.failureReason());
    }

    @Test
    void theGatewaysShareOneIdentityTable() {
        assertSame(
                injector.getInstance(NodeAttachments.class),
                injector.getInstance(NodeAttachments.class));
        // RegistrationServer resolves devices through the shared table.
        injector.getInstance(NodeAttachments.class).attach("dev-1", "mqtt", () -> "node-1");
        assertEquals(java.util.Optional.of("dev-1"),
                injector.getInstance(RegistrationServer.class).deviceIdForNode("node-1"));
    }
}
