// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.phoenix.core.AttemptFencing;
import com.phoenix.core.DeviceRegistry;
import com.phoenix.core.NodeHealthMonitor;
import com.phoenix.core.NodeTelemetryService;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.core.TaskTimeoutPolicy;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.sun.net.httpserver.HttpServer;

/** Issue #36: the push request carries the same attempt identity as pull. */
class HttpPushAttemptWireTest {

    private String pushAndCaptureBody(AttemptFencing fencing)
            throws Exception {

        AtomicReference<String> body = new AtomicReference<>();
        TaskLifecycleService lifecycle = new TaskLifecycleService();

        HttpServer node = HttpServer.create(new InetSocketAddress(0), 0);
        node.createContext("/execute", exchange -> {
            body.set(new String(
                    exchange.getRequestBody().readAllBytes(),
                    StandardCharsets.UTF_8));
            byte[] ok = "{\"status\":\"SUCCESS\",\"outputData\":\"x\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, ok.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(ok);
            }
        });
        node.setExecutor(null);
        node.start();

        try {
            DeviceRegistry registry = new DeviceRegistry();
            RegistrationServer registration = new RegistrationServer(
                    new NodeTelemetryService(
                            registry, new NodeHealthMonitor(registry)),
                    new TaskQueueService(lifecycle)) {
                @Override
                public AttemptFencing fencingFor(String nodeId) {
                    return fencing;
                }

                @Override
                public Optional<String> callbackAddressForNode(String id) {
                    return Optional.of(
                            "127.0.0.1:" + node.getAddress().getPort());
                }
            };

            new HttpPushNodeAgentClient(
                    registration,
                    lifecycle,
                    new TaskTimeoutPolicy(
                            Duration.ofSeconds(2),
                            Duration.ofSeconds(2),
                            Duration.ofSeconds(5)))
                    .execute(
                            new NodeExecutionTarget("n1", "node", "linux"),
                            new TaskDescriptor("task-001", "Monte-Carlo-Pi", 1, 128));

            String sent = body.get();
            String id = lifecycle.snapshot("task-001").orElseThrow().attemptId();
            return sent + "|" + id;

        } finally {
            node.stop(0);
        }
    }

    @Test
    void theRequestCarriesTheAttemptIdAndNumberForAnEnforcedNode()
            throws Exception {

        String[] parts =
                pushAndCaptureBody(AttemptFencing.ENFORCED).split("\\|");

        assertTrue(parts[0].contains("\"attemptId\":\"" + parts[1] + "\""));
        assertTrue(parts[0].contains("\"attemptNumber\":1"));
        assertFalse(parts[0].contains("\"attempt\":"));
        assertFalse(parts[0].contains("@@ATTEMPT@@"));
    }

    @Test
    void theRequestHasNoAttemptFieldsForALegacyNode() throws Exception {

        String sent =
                pushAndCaptureBody(AttemptFencing.LEGACY).split("\\|")[0];

        assertFalse(sent.contains("attempt"));
        assertFalse(sent.contains("@@ATTEMPT@@"));
        assertEquals(sent, sent.strip());
    }
}
