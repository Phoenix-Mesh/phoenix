// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

/**
 * Issue #28 end-to-end over the real HTTP pull transport: a real
 * RegistrationServer on an ephemeral port, driven the way an Android agent
 * drives it (register, poll, started, results).
 */
class HttpNodeAgentClientTest {

    private static final String DEVICE_ID = "android-test-device";

    private final HttpClient http = HttpClient.newHttpClient();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private DeviceRegistry registry;
    private TaskLifecycleService lifecycle;
    private TaskQueueService queue;
    private RegistrationServer server;
    private HttpNodeAgentClient client;
    private NodeExecutionTarget target;
    private String nodeId;

    @BeforeEach
    void startCore() throws Exception {

        registry = new DeviceRegistry();
        lifecycle = new TaskLifecycleService();
        queue = new TaskQueueService(lifecycle);

        NodeTelemetryService telemetry =
                new NodeTelemetryService(
                        registry,
                        new NodeHealthMonitor(registry));

        server = new RegistrationServer(telemetry, queue);
        server.start(0);

        // delivery 600ms, execution 600ms, "in contact" = polled within 300ms
        client = new HttpNodeAgentClient(
                server,
                queue,
                lifecycle,
                new TaskTimeoutPolicy(
                        Duration.ofMillis(600),
                        Duration.ofMillis(600),
                        Duration.ofMillis(300)));

        String response = post(
                "/register",
                "{\"deviceId\":\"" + DEVICE_ID + "\","
                        + "\"displayName\":\"test-phone\","
                        + "\"hardwareClass\":\"ANDROID\","
                        + "\"architecture\":\"arm64\","
                        + "\"cpuCores\":4,\"memoryMb\":4096,"
                        + "\"adapterType\":\"android\","
                        + "\"capabilities\":[]}"
        ).body();

        nodeId = response.replaceAll(
                ".*\"nodeId\":\"([^\"]+)\".*", "$1");

        target = new NodeExecutionTarget(
                nodeId, "test-phone", "android");
    }

    @AfterEach
    void stopCore() {
        executor.shutdownNow();
        server.stop();
    }

    // ------------------------------------------------------------------
    // happy path
    // ------------------------------------------------------------------

    @Test
    void deliveredTaskThatReportsAResultSucceedsAndWalksTheLifecycle()
            throws Exception {

        Future<TaskResult> attempt = run("task-001");

        assertTrue(pollUntilTask().contains("task-001"));

        assertEquals(200, post("/tasks/started",
                "{\"taskId\":\"task-001\"}").statusCode());

        assertEquals(200, post("/results",
                "{\"taskId\":\"task-001\",\"status\":\"SUCCESS\","
                        + "\"outputData\":\"ok\","
                        + "\"executionTimeMs\":5}").statusCode());

        TaskResult result = attempt.get(3, TimeUnit.SECONDS);

        assertTrue(result.isSuccess());

        TaskLifecycleService.Snapshot snapshot =
                lifecycle.snapshot("task-001").orElseThrow();

        assertEquals(TaskLifecycleState.COMPLETED, snapshot.state());
        assertTrue(snapshot.reachedNode());
        assertTrue(snapshot.startAcknowledged());
    }

    @Test
    void agentsThatNeverSendStartedStillWork() throws Exception {

        Future<TaskResult> attempt = run("task-001");

        pollUntilTask();

        post("/results",
                "{\"taskId\":\"task-001\",\"status\":\"SUCCESS\","
                        + "\"outputData\":\"ok\"}");

        assertTrue(attempt.get(3, TimeUnit.SECONDS).isSuccess());
        assertFalse(
                lifecycle.snapshot("task-001").orElseThrow()
                        .startAcknowledged());
    }

    // ------------------------------------------------------------------
    // the #28 distinction: not delivered
    // ------------------------------------------------------------------

    @Test
    void undeliveredTaskOnANodeThatIsStillPollingIsOrdinaryBacklog()
            throws Exception {

        /*
         * The node is polling (so it is in contact) but Core is not handing
         * it work: a non-ONLINE node gets 204s. This is exactly the
         * situation the #28 issue log shows, where a stale timeout degraded
         * the node and then starved its own queue.
         */
        registry.markDegraded(nodeId);

        AtomicBoolean polling = new AtomicBoolean(true);
        executor.submit(() -> {
            while (polling.get()) {
                try {
                    get("/tasks/poll?deviceId=" + DEVICE_ID);
                    Thread.sleep(50);
                } catch (Exception ignored) {
                }
            }
        });

        try {
            TaskResult result =
                    run("task-001").get(5, TimeUnit.SECONDS);

            assertEquals(
                    TaskFailureReason.DELIVERY_TIMEOUT,
                    result.failureReason());

            assertTrue(result.isRetryable());
            assertFalse(result.implicatesNodeHealth());

            // The attempt was withdrawn: the node can never run it late.
            assertEquals(0, queue.queueLength(DEVICE_ID));
            assertEquals(
                    TaskLifecycleState.FAILED,
                    lifecycle.snapshot("task-001").orElseThrow().state());
            assertFalse(
                    lifecycle.snapshot("task-001").orElseThrow()
                            .reachedNode());

        } finally {
            polling.set(false);
        }
    }

    @Test
    void undeliveredTaskOnANodeThatNeverPolledIsUnreachable()
            throws Exception {

        TaskResult result = run("task-001").get(5, TimeUnit.SECONDS);

        assertEquals(
                TaskFailureReason.NODE_UNREACHABLE,
                result.failureReason());

        assertTrue(result.isRetryable());
        assertTrue(result.implicatesNodeHealth());
        assertTrue(result.outputData().contains("never polled"));
        assertEquals(0, queue.queueLength(DEVICE_ID));
    }

    @Test
    void undeliveredTaskOnANodeWhoseLastPollIsStaleIsUnreachable()
            throws Exception {

        // One poll long ago (>300ms by the time the 600ms window ends).
        queue.recordPoll(DEVICE_ID);

        TaskResult result = run("task-001").get(5, TimeUnit.SECONDS);

        assertEquals(
                TaskFailureReason.NODE_UNREACHABLE,
                result.failureReason());
        assertTrue(result.implicatesNodeHealth());
    }

    // ------------------------------------------------------------------
    // the #28 distinction: delivered but silent
    // ------------------------------------------------------------------

    @Test
    void deliveredTaskWithNoResultIsAnExecutionTimeout()
            throws Exception {

        Future<TaskResult> attempt = run("task-001");

        pollUntilTask();   // delivered, then the node goes silent

        TaskResult result = attempt.get(5, TimeUnit.SECONDS);

        assertEquals(
                TaskFailureReason.EXECUTION_TIMEOUT,
                result.failureReason());

        assertTrue(result.isRetryable());
        assertTrue(result.implicatesNodeHealth());
        assertTrue(result.outputData().contains("start not acknowledged"));

        assertTrue(
                lifecycle.snapshot("task-001").orElseThrow()
                        .reachedNode());
    }

    @Test
    void startedButSilentTaskReportsThatTheStartWasAcknowledged()
            throws Exception {

        Future<TaskResult> attempt = run("task-001");

        pollUntilTask();
        post("/tasks/started", "{\"taskId\":\"task-001\"}");

        TaskResult result = attempt.get(5, TimeUnit.SECONDS);

        assertEquals(
                TaskFailureReason.EXECUTION_TIMEOUT,
                result.failureReason());
        assertTrue(result.outputData().contains("start acknowledged"));
    }

    @Test
    void lateResultAfterAnExecutionTimeoutIsRejected() throws Exception {

        Future<TaskResult> attempt = run("task-001");

        pollUntilTask();
        attempt.get(5, TimeUnit.SECONDS);

        assertEquals(404, post("/results",
                "{\"taskId\":\"task-001\",\"status\":\"SUCCESS\","
                        + "\"outputData\":\"late\"}").statusCode());
    }

    @Test
    void startedAckForAnUndeliveredTaskIsRejected() throws Exception {

        Future<TaskResult> attempt = run("task-001");

        // Wait until it is queued but do NOT poll it.
        long deadline = System.currentTimeMillis() + 2000;
        while (queue.queueLength(DEVICE_ID) == 0
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        assertEquals(404, post("/tasks/started",
                "{\"taskId\":\"task-001\"}").statusCode());

        attempt.get(5, TimeUnit.SECONDS);
    }

    @Test
    void pollingNeverMakesAnUnknownDeviceLookReachable() throws Exception {

        assertEquals(404,
                get("/tasks/poll?deviceId=someone-else").statusCode());
        assertTrue(queue.timeSinceLastPoll("someone-else").isEmpty());
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private Future<TaskResult> run(String taskId) {
        return executor.submit(() -> client.execute(
                target,
                new TaskDescriptor(taskId, "Monte-Carlo-Pi", 1, 128)));
    }

    /** Polls like an agent until Core hands over a task body. */
    private String pollUntilTask() throws Exception {

        long deadline = System.currentTimeMillis() + 2000;

        while (System.currentTimeMillis() < deadline) {

            HttpResponse<String> response =
                    get("/tasks/poll?deviceId=" + DEVICE_ID);

            if (response.statusCode() == 200) {
                return response.body();
            }

            Thread.sleep(10);
        }

        throw new AssertionError("no task was delivered to the node");
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(
                HttpRequest.newBuilder(uri(path))
                        .timeout(Duration.ofSeconds(3))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(
            String path,
            String json) throws Exception {

        return http.send(
                HttpRequest.newBuilder(uri(path))
                        .timeout(Duration.ofSeconds(3))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + server.port() + path);
    }
}
