// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.phoenix.core.DeviceRegistry;
import com.phoenix.core.NodeHealthMonitor;
import com.phoenix.core.NodeTelemetryService;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.core.TaskTimeoutPolicy;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskLifecycleState;
import com.phoenix.model.TaskResult;
import com.sun.net.httpserver.HttpServer;

/**
 * Push transport: an abandoned attempt must not sit in a blocked socket read
 * until the (here 30 s) execution timeout.
 */
class HttpPushNodeAgentClientAbandonTest {

    private static final String NODE_ID = "node-1";

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private HttpServer server;

    @AfterEach
    void stop() {
        executor.shutdownNow();
        if (server != null) {
            server.stop(0);
        }
    }

    private HttpPushNodeAgentClient client(
            String address,
            TaskLifecycleService lifecycle) {

        DeviceRegistry registry = new DeviceRegistry();

        RegistrationServer registration = new RegistrationServer(
                new NodeTelemetryService(
                        registry, new NodeHealthMonitor(registry)),
                new TaskQueueService(lifecycle)) {

            @Override
            public Optional<String> callbackAddressForNode(String nodeId) {
                return NODE_ID.equals(nodeId)
                        ? Optional.of(address)
                        : Optional.empty();
            }
        };

        return new HttpPushNodeAgentClient(
                registration,
                lifecycle,
                new TaskTimeoutPolicy(
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(15)));
    }

    @Test
    void offlineNodeWakesABlockedPushImmediately() throws Exception {

        CountDownLatch requestReceived = new CountDownLatch(1);

        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/execute", exchange -> {
            requestReceived.countDown();
            try {
                Thread.sleep(10_000);           // the node hangs
            } catch (InterruptedException ignored) {
            } finally {
                exchange.close();
            }
        });
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        TaskLifecycleService lifecycle = new TaskLifecycleService();
        HttpPushNodeAgentClient client = client(
                "127.0.0.1:" + server.getAddress().getPort(), lifecycle);

        Future<TaskResult> attempt = executor.submit(() -> client.execute(
                new NodeExecutionTarget(NODE_ID, "Linux-Test", "linux"),
                new TaskDescriptor("push-001", "Monte-Carlo-Pi", 1, 128)));

        assertTrue(requestReceived.await(3, TimeUnit.SECONDS));

        long start = System.nanoTime();
        lifecycle.abandonAttemptsOn(NODE_ID);

        TaskResult result = attempt.get(3, TimeUnit.SECONDS);

        assertTrue(Duration.ofNanos(System.nanoTime() - start)
                .compareTo(Duration.ofSeconds(2)) < 0,
                "must return on reconciliation, not on the 30s read timeout");
        assertEquals(TaskFailureReason.NODE_OFFLINE, result.failureReason());
        assertTrue(result.isRetryable());
        assertEquals(TaskLifecycleState.ABANDONED,
                lifecycle.snapshot("push-001").orElseThrow().state());
    }

    @Test
    void aNormalPushIsUnaffectedByTheAbandonHook() throws Exception {

        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/execute", exchange -> {
            byte[] body =
                    "{\"status\":\"SUCCESS\",\"outputData\":\"ok\"}".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();

        TaskLifecycleService lifecycle = new TaskLifecycleService();
        HttpPushNodeAgentClient client = client(
                "127.0.0.1:" + server.getAddress().getPort(), lifecycle);

        TaskResult result = client.execute(
                new NodeExecutionTarget(NODE_ID, "Linux-Test", "linux"),
                new TaskDescriptor("push-002", "Monte-Carlo-Pi", 1, 128));

        assertTrue(result.isSuccess());
        assertEquals(TaskLifecycleState.COMPLETED,
                lifecycle.snapshot("push-002").orElseThrow().state());

        // Abandoning afterwards must not touch a completed attempt.
        assertTrue(lifecycle.abandonAttemptsOn(NODE_ID).isEmpty());
    }
}
