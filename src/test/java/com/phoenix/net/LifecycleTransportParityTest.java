// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.phoenix.core.AttemptFencing;
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
 * Issue #37: the same node-reported outcome must leave the same lifecycle
 * state whichever transport carried it.
 */
class LifecycleTransportParityTest {

    private static final TaskDescriptor TASK =
            new TaskDescriptor("task-001", "Monte-Carlo-Pi", 1, 128);

    private TaskLifecycleState viaPull(String status) throws Exception {

        TaskLifecycleService lifecycle = new TaskLifecycleService();
        TaskQueueService queue = new TaskQueueService(lifecycle);

        queue.submit("n1", "dev-1", TASK, AttemptFencing.LEGACY);
        queue.poll("dev-1");
        queue.completeResult(
                new TaskResult(
                        TASK.id(), status, "x",
                        "SUCCESS".equals(status)
                                ? TaskFailureReason.NONE
                                : TaskFailureReason.EXECUTION_ERROR));

        return lifecycle.snapshot(TASK.id()).orElseThrow().state();
    }

    private TaskLifecycleState viaPush(String status) throws Exception {

        TaskLifecycleService lifecycle = new TaskLifecycleService();

        HttpServer node = HttpServer.create(new InetSocketAddress(0), 0);
        node.createContext("/execute", exchange -> {
            byte[] body = ("{\"status\":\"" + status
                    + "\",\"outputData\":\"x\"}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
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
                            TASK);

            return lifecycle.snapshot(TASK.id()).orElseThrow().state();

        } finally {
            node.stop(0);
        }
    }

    @Test
    void aNodeReportedSuccessIsCompletedOnBothTransports() throws Exception {
        assertEquals(TaskLifecycleState.COMPLETED, viaPull("SUCCESS"));
        assertEquals(TaskLifecycleState.COMPLETED, viaPush("SUCCESS"));
    }

    @Test
    void aNodeReportedFailureIsFailedOnBothTransports() throws Exception {
        assertEquals(TaskLifecycleState.FAILED, viaPull("FAILED"));
        assertEquals(TaskLifecycleState.FAILED, viaPush("FAILED"));
    }
}
