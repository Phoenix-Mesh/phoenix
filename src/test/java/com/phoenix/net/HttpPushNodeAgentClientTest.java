// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.phoenix.core.DeviceRegistry;
import com.phoenix.core.NodeHealthMonitor;
import com.phoenix.core.NodeTelemetryService;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.core.TaskTimeoutPolicy;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Phase 7 tests for the push-side HTTP transport.
 *
 * <p>These tests deliberately exercise the real HttpURLConnection path rather
 * than mocking HTTP. The remote node is represented by a small local
 * HttpServer.
 *
 * <p>The tests verify the Issue #28 contract:
 *
 * <pre>
 * accepted
 *     |
 *     v
 * delivered
 *     |
 *     +-- no result in time -> EXECUTION_TIMEOUT
 *     |
 *     v
 * result
 *     |
 *     +-- SUCCESS -> COMPLETED
 *     +-- failure -> FAILED
 *
 * Connection failure before delivery -> NODE_UNREACHABLE
 * </pre>
 */
class HttpPushNodeAgentClientTest {

    private static final String NODE_ID = "node-1";

    private static final String NODE_NAME = "Linux-Test";

    private static final String ADAPTER_TYPE = "linux";

    /**
     * Creates the real lifecycle and timeout services used by the push client.
     */
    private static TaskLifecycleService lifecycleService() {
        return new TaskLifecycleService();
    }

    /**
     * Creates a push client whose RegistrationServer callback lookup is
     * overridden by the test.
     *
     * <p>The actual RegistrationServer does not need to be started because
     * HttpPushNodeAgentClient only needs its callbackAddressForNode() lookup.
     */
    private static HttpPushNodeAgentClient clientFor(
            String callbackAddress,
            TaskLifecycleService lifecycle,
            TaskTimeoutPolicy timeouts) {

        DeviceRegistry registry =
                new DeviceRegistry();

        NodeHealthMonitor healthMonitor =
                new NodeHealthMonitor(registry);

        NodeTelemetryService telemetry =
                new NodeTelemetryService(
                        registry,
                        healthMonitor
                );

        TaskQueueService queue =
                new TaskQueueService(lifecycle);

        RegistrationServer registrationServer =
                new RegistrationServer(
                        telemetry,
                        queue
                ) {

                    @Override
                    public Optional<String> callbackAddressForNode(
                            String nodeId) {

                        if (!NODE_ID.equals(nodeId)) {
                            return Optional.empty();
                        }

                        return Optional.of(callbackAddress);
                    }
                };

        return new HttpPushNodeAgentClient(
                registrationServer,
                lifecycle,
                timeouts
        );
    }

    private static NodeExecutionTarget target() {
        return new NodeExecutionTarget(
                NODE_ID,
                NODE_NAME,
                ADAPTER_TYPE
        );
    }

    private static TaskDescriptor task(String id) {
        return new TaskDescriptor(
                id,
                "Monte-Carlo-Pi",
                1,
                512
        );
    }

    private static TaskTimeoutPolicy policy(
            Duration deliveryTimeout,
            Duration executionTimeout,
            Duration contactLiveness) {

        return new TaskTimeoutPolicy(
                deliveryTimeout,
                executionTimeout,
                contactLiveness
        );
    }

    private static HttpServer server(
            HttpHandler handler) throws IOException {

        HttpServer server =
                HttpServer.create(
                        new InetSocketAddress(0),
                        0
                );

        server.createContext(
                "/execute",
                handler::handle
        );

        server.setExecutor(null);
        server.start();

        return server;
    }

    private interface HttpHandler {

        void handle(HttpExchange exchange)
                throws IOException;
    }

    @Test
    void successfulPushCompletesTask() throws Exception {

        TaskLifecycleService lifecycle =
                lifecycleService();

        TaskTimeoutPolicy timeouts =
                policy(
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(5)
                );

        HttpServer server =
                server(exchange -> {

                    byte[] response =
                            """
                            {
                              "status": "SUCCESS",
                              "outputData": "pi=3.14"
                            }
                            """
                                    .getBytes(StandardCharsets.UTF_8);

                    exchange.getResponseHeaders()
                            .set(
                                    "Content-Type",
                                    "application/json"
                            );

                    exchange.sendResponseHeaders(
                            200,
                            response.length
                    );

                    try (OutputStream output =
                                 exchange.getResponseBody()) {

                        output.write(response);
                    }
                });

        try {

            int port =
                    server.getAddress().getPort();

            HttpPushNodeAgentClient client =
                    clientFor(
                            "127.0.0.1:" + port,
                            lifecycle,
                            timeouts
                    );

            TaskResult result =
                    client.execute(
                            target(),
                            task("push-success")
                    );

            assertEquals(
                    "SUCCESS",
                    result.status()
            );

            assertEquals(
                    TaskFailureReason.NONE,
                    result.failureReason()
            );

            assertTrue(
                    lifecycle.snapshot("push-success").isPresent()
            );

            assertEquals(
                    "COMPLETED",
                    lifecycle.snapshot("push-success")
                            .orElseThrow()
                            .state()
                            .name()
            );

        } finally {
            server.stop(0);
        }
    }

    @Test
    void remoteHttpFailureDoesNotMarkTaskCompleted() throws Exception {

        TaskLifecycleService lifecycle =
                lifecycleService();

        TaskTimeoutPolicy timeouts =
                policy(
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(5)
                );

        HttpServer server =
                server(exchange -> {

                    byte[] response =
                            """
                            {"status":"FAILED","outputData":"node error"}
                            """
                                    .getBytes(StandardCharsets.UTF_8);

                    exchange.sendResponseHeaders(
                            500,
                            response.length
                    );

                    try (OutputStream output =
                                 exchange.getResponseBody()) {

                        output.write(response);
                    }
                });

        try {

            int port =
                    server.getAddress().getPort();

            HttpPushNodeAgentClient client =
                    clientFor(
                            "127.0.0.1:" + port,
                            lifecycle,
                            timeouts
                    );

            TaskResult result =
                    client.execute(
                            target(),
                            task("push-http-500")
                    );

            assertEquals(
                    "FAILED",
                    result.status()
            );

            assertEquals(
                    TaskFailureReason.TRANSPORT_ERROR,
                    result.failureReason()
            );

            assertEquals(
                    "FAILED",
                    lifecycle.snapshot("push-http-500")
                            .orElseThrow()
                            .state()
                            .name()
            );

        } finally {
            server.stop(0);
        }
    }

    @Test
    void deliveredButSilentNodeProducesExecutionTimeout()
            throws Exception {

        TaskLifecycleService lifecycle =
                lifecycleService();

        TaskTimeoutPolicy timeouts =
                policy(
                        Duration.ofSeconds(2),
                        Duration.ofMillis(150),
                        Duration.ofSeconds(5)
                );

        CountDownLatch requestReceived =
                new CountDownLatch(1);

        HttpServer server =
                server(exchange -> {

                    requestReceived.countDown();

                    /*
                     * Deliberately keep the HTTP response open longer than
                     * the configured execution timeout.
                     */
                    try {

                        Thread.sleep(1_000);

                    } catch (InterruptedException exception) {

                        Thread.currentThread().interrupt();

                    } finally {

                        exchange.close();
                    }
                });

        try {

            int port =
                    server.getAddress().getPort();

            HttpPushNodeAgentClient client =
                    clientFor(
                            "127.0.0.1:" + port,
                            lifecycle,
                            timeouts
                    );

            TaskResult result =
                    client.execute(
                            target(),
                            task("push-execution-timeout")
                    );

            assertTrue(
                    requestReceived.await(
                            1,
                            TimeUnit.SECONDS
                    )
            );

            assertEquals(
                    "FAILED",
                    result.status()
            );

            assertEquals(
                    TaskFailureReason.EXECUTION_TIMEOUT,
                    result.failureReason()
            );

            assertEquals(
                    "FAILED",
                    lifecycle.snapshot(
                                    "push-execution-timeout"
                            )
                            .orElseThrow()
                            .state()
                            .name()
            );

        } finally {
            server.stop(0);
        }
    }

    @Test
    void unreachableNodeProducesNodeUnreachableBeforeDelivery() {

        TaskLifecycleService lifecycle =
                lifecycleService();

        TaskTimeoutPolicy timeouts =
                policy(
                        Duration.ofMillis(250),
                        Duration.ofMillis(250),
                        Duration.ofSeconds(5)
                );

        /*
         * Port 1 should not have a listening Phoenix test node.
         *
         * The important assertion is not the exact socket exception. The
         * transport must classify the failure as NODE_UNREACHABLE because
         * delivery never happened.
         */
        HttpPushNodeAgentClient client =
                clientFor(
                        "127.0.0.1:1",
                        lifecycle,
                        timeouts
                );

        TaskResult result =
                client.execute(
                        target(),
                        task("push-unreachable")
                );

        assertEquals(
                "FAILED",
                result.status()
        );

        assertEquals(
                TaskFailureReason.NODE_UNREACHABLE,
                result.failureReason()
        );

        assertEquals(
                "FAILED",
                lifecycle.snapshot("push-unreachable")
                        .orElseThrow()
                        .state()
                        .name()
        );
    }

    @Test
    void aMalformedResultBodyFailsTheAttemptAsATransportError() throws Exception {

        TaskLifecycleService lifecycle =
                lifecycleService();

        HttpServer server =
                server(exchange -> {
                    byte[] response =
                            "{not json".getBytes(StandardCharsets.UTF_8);

                    exchange.sendResponseHeaders(200, response.length);

                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write(response);
                    }
                });

        try {
            HttpPushNodeAgentClient client =
                    clientFor(
                            "127.0.0.1:" + server.getAddress().getPort(),
                            lifecycle,
                            policy(
                                    Duration.ofSeconds(2),
                                    Duration.ofSeconds(2),
                                    Duration.ofSeconds(5)));

            TaskResult result =
                    client.execute(target(), task("push-malformed"));

            assertEquals(
                    TaskFailureReason.TRANSPORT_ERROR,
                    result.failureReason());

            // The attempt must reach a terminal state, or a later OFFLINE
            // transition would "abandon" an attempt that already ended.
            assertEquals(
                    "FAILED",
                    lifecycle.snapshot("push-malformed")
                            .orElseThrow()
                            .state()
                            .name());

        } finally {
            server.stop(0);
        }
    }
}
