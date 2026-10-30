// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskLifecycleState;
import com.phoenix.model.TaskResult;

/**
 * #21 and #22: task delivery, result correlation, idempotence and lifecycle
 * integration of the MQTT transport.
 *
 * <p>Delivery and execution windows are 600 ms, contact liveness 300 ms, as
 * in the HTTP transport tests.
 */
class MqttNodeAgentClientTest {

    private final MqttFixture f = new MqttFixture();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void stop() {
        executor.shutdownNow();
    }

    private TaskResult run(FakeMqttAgent agent, String taskId) {
        return f.client.execute(f.target(agent), MqttFixture.task(taskId));
    }

    private Future<TaskResult> runAsync(FakeMqttAgent agent, String taskId) {
        return executor.submit(() -> run(agent, taskId));
    }

    private TaskLifecycleService.Snapshot snapshot(String taskId) {
        return f.lifecycle.snapshot(taskId).orElseThrow();
    }

    private static void awaitTask(FakeMqttAgent agent, int count) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (agent.tasks.size() < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(agent.tasks.size() >= count, "task was not published to the agent");
    }

    // ------------------------------------------------------------------
    // happy paths
    // ------------------------------------------------------------------

    @Test
    void aTaskIsDeliveredExecutedAndCompletedWithTheFullLifecycle() {

        FakeMqttAgent agent = f.agent("android-a1");

        TaskResult result = run(agent, "task-001");

        assertTrue(result.isSuccess());
        assertEquals("Computed Monte-Carlo pi ~= 3.1416", result.outputData());

        JsonObject task = agent.lastTask();
        assertEquals("task-001", task.get("taskId").getAsString());
        assertEquals("Monte-Carlo-Pi", task.get("type").getAsString());
        assertEquals(1, task.get("attemptNumber").getAsInt());

        TaskLifecycleService.Snapshot snapshot = snapshot("task-001");
        assertEquals(TaskLifecycleState.COMPLETED, snapshot.state());
        assertEquals(task.get("attemptId").getAsString(), snapshot.attemptId());
        assertTrue(snapshot.reachedNode());
        assertTrue(snapshot.startAcknowledged());
        assertEquals(0, f.attempts.pendingCount());
        assertTrue(agent.cancels.isEmpty());
    }

    @Test
    void aNodeReportedFailureIsAFinalExecutionError() {

        FakeMqttAgent agent = f.agent("android-a1");
        agent.mode = FakeMqttAgent.Mode.FAIL;

        TaskResult result = run(agent, "task-001");

        assertEquals(TaskFailureReason.EXECUTION_ERROR, result.failureReason());
        assertFalse(result.isRetryable());
        assertEquals(TaskLifecycleState.FAILED, snapshot("task-001").state());
    }

    @Test
    void aNodeWithoutAnMqttSessionIsUnreachable() {

        TaskResult result = f.client.execute(
                new NodeExecutionTarget("ghost", "ghost", "android"),
                MqttFixture.task("task-001"));

        assertEquals(TaskFailureReason.NODE_UNREACHABLE, result.failureReason());
        assertTrue(f.lifecycle.snapshot("task-001").isEmpty());
    }

    @Test
    void whenTheResultReachesTheDispatcherTheLedgerIsAlreadyTerminal() throws Exception {

        // The result arrives on another thread (as with Paho). Whoever gets the
        // result back must already see COMPLETED, never a lagging STARTED.
        FakeMqttAgent agent = f.agent("android-a1");
        agent.mode = FakeMqttAgent.Mode.ACK_ONLY;

        for (int i = 0; i < 200; i++) {
            String taskId = "task-" + i;
            int before = agent.tasks.size();

            Future<TaskLifecycleState> seen = executor.submit(() -> {
                run(agent, taskId);
                return snapshot(taskId).state();
            });

            awaitTask(agent, before + 1);
            agent.started(agent.lastTask());
            agent.result(agent.lastTask(), "SUCCESS", agent.sessionToken);

            assertEquals(TaskLifecycleState.COMPLETED, seen.get(3, TimeUnit.SECONDS), taskId);
        }
    }

    // ------------------------------------------------------------------
    // the #28 distinctions, over MQTT
    // ------------------------------------------------------------------

    @Test
    void noAckFromANodeThatIsStillInContactIsBacklog() throws Exception {

        FakeMqttAgent agent = f.agent("android-a1");
        agent.mode = FakeMqttAgent.Mode.SILENT;

        Future<TaskResult> attempt = runAsync(agent, "task-001");

        // Heartbeats keep arriving while the task waits.
        while (!attempt.isDone()) {
            agent.heartbeat();
            Thread.sleep(50);
        }

        TaskResult result = attempt.get();

        assertEquals(TaskFailureReason.DELIVERY_TIMEOUT, result.failureReason());
        assertTrue(result.isRetryable());
        assertFalse(result.implicatesNodeHealth());
        assertFalse(snapshot("task-001").reachedNode());
        assertEquals(TaskLifecycleState.FAILED, snapshot("task-001").state());

        // The node is told to drop the attempt if it ever gets it.
        assertEquals(1, agent.cancels.size());
        assertEquals("delivery_timeout", agent.cancels.get(0).get("reason").getAsString());
        assertEquals(agent.lastTask().get("attemptId"), agent.cancels.get(0).get("attemptId"));
    }

    @Test
    void noAckFromASilentNodeIsUnreachable() {

        FakeMqttAgent agent = f.agent("android-a1");
        agent.mode = FakeMqttAgent.Mode.SILENT;

        TaskResult result = run(agent, "task-001");

        assertEquals(TaskFailureReason.NODE_UNREACHABLE, result.failureReason());
        assertTrue(result.implicatesNodeHealth());
        assertTrue(result.outputData().contains("last heard from"));
    }

    @Test
    void anAckedTaskWithoutAResultIsAnExecutionTimeout() {

        FakeMqttAgent agent = f.agent("android-a1");
        agent.mode = FakeMqttAgent.Mode.ACK_ONLY;

        TaskResult result = run(agent, "task-001");

        assertEquals(TaskFailureReason.EXECUTION_TIMEOUT, result.failureReason());
        assertTrue(result.outputData().contains("start not acknowledged"));
        assertTrue(snapshot("task-001").reachedNode());
        assertEquals("execution_timeout", agent.cancels.get(0).get("reason").getAsString());

        // The late result is rejected: Core is no longer waiting for it.
        long dropped = f.attempts.droppedReportCount();
        agent.result(agent.lastTask(), "SUCCESS", agent.sessionToken);
        assertEquals(dropped + 1, f.attempts.droppedReportCount());
        assertEquals(TaskLifecycleState.FAILED, snapshot("task-001").state());
    }

    @Test
    void whenCoreIsNotConnectedToTheBrokerTheNodeIsNotBlamed() {

        FakeMqttAgent agent = f.agent("android-a1");
        f.coreConnection.dropLink();

        TaskResult result = run(agent, "task-001");

        assertEquals(TaskFailureReason.TRANSPORT_UNAVAILABLE, result.failureReason());
        assertTrue(result.isRetryable());
        assertFalse(result.implicatesNodeHealth());
        assertTrue(agent.tasks.isEmpty());
    }

    @Test
    void aBrokerLinkThatReturnsWithinTheDeliveryWindowStillDelivers() throws Exception {

        FakeMqttAgent agent = f.agent("android-a1");
        f.coreConnection.dropLink();

        Future<TaskResult> attempt = runAsync(agent, "task-001");
        Thread.sleep(150);
        f.coreConnection.goOnline();

        assertTrue(attempt.get(3, TimeUnit.SECONDS).isSuccess());
    }

    // ------------------------------------------------------------------
    // abandonment (node OFFLINE)
    // ------------------------------------------------------------------

    @Test
    void anOfflineNodeWakesTheAttemptAtOnceAndTheNodeIsToldToDropIt() throws Exception {

        MqttFixture slow = new MqttFixture(true,
                Duration.ofSeconds(30), Duration.ofSeconds(30), Duration.ofSeconds(15));
        FakeMqttAgent agent = slow.agent("android-a1");
        agent.mode = FakeMqttAgent.Mode.ACK_ONLY;

        Future<TaskResult> attempt = executor.submit(() ->
                slow.client.execute(slow.target(agent), MqttFixture.task("task-001")));
        awaitTask(agent, 1);
        Thread.sleep(50);

        long start = System.nanoTime();
        slow.lifecycle.abandonAttemptsOn(agent.nodeId);
        TaskResult result = attempt.get(3, TimeUnit.SECONDS);

        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(2)) < 0,
                "must return on reconciliation, not on the 30 s timeout");
        assertEquals(TaskFailureReason.NODE_OFFLINE, result.failureReason());
        assertFalse(result.implicatesNodeHealth());
        assertTrue(result.outputData().contains("acknowledged"));
        assertEquals(TaskLifecycleState.ABANDONED,
                slow.lifecycle.snapshot("task-001").orElseThrow().state());
        assertEquals("node_offline", agent.cancels.get(0).get("reason").getAsString());

        // The node comes back and reports the abandoned attempt: rejected.
        agent.result(agent.lastTask(), "SUCCESS", agent.sessionToken);
        assertEquals(TaskLifecycleState.ABANDONED,
                slow.lifecycle.snapshot("task-001").orElseThrow().state());
    }

    // ------------------------------------------------------------------
    // fencing and idempotence
    // ------------------------------------------------------------------

    @Test
    void onlyAReportForTheRightAttemptFromTheRightDeviceWithAValidSessionCounts()
            throws Exception {

        FakeMqttAgent agent = f.agent("android-a1");
        FakeMqttAgent other = f.agent("android-b2");
        agent.mode = FakeMqttAgent.Mode.ACK_ONLY;

        Future<TaskResult> attempt = runAsync(agent, "task-001");
        awaitTask(agent, 1);
        JsonObject task = agent.lastTask();
        String attemptId = task.get("attemptId").getAsString();

        // No attempt id, a wrong one, the wrong task, the wrong device, a bad token:
        agent.resultFor("task-001", null, "SUCCESS");
        agent.resultFor("task-001", "not-the-attempt", "SUCCESS");
        agent.resultFor("task-999", attemptId, "SUCCESS");
        other.publishTo(f.link.topics().up("android-b2", MqttTopics.Up.RESULT),
                "{\"sessionToken\":\"" + other.sessionToken + "\",\"taskId\":\"task-001\","
                        + "\"attemptId\":\"" + attemptId + "\",\"status\":\"SUCCESS\"}");
        agent.result(task, "SUCCESS", "forged-token");

        assertFalse(attempt.isDone());
        assertEquals(TaskLifecycleState.DELIVERED, snapshot("task-001").state());

        agent.result(task, "SUCCESS", agent.sessionToken);

        assertTrue(attempt.get(3, TimeUnit.SECONDS).isSuccess());
    }

    @Test
    void duplicateReportsChangeNothing() throws Exception {

        FakeMqttAgent agent = f.agent("android-a1");
        agent.mode = FakeMqttAgent.Mode.ACK_ONLY;

        Future<TaskResult> attempt = runAsync(agent, "task-001");
        awaitTask(agent, 1);
        JsonObject task = agent.lastTask();

        agent.ack(task);
        agent.ack(task);
        agent.started(task);
        agent.result(task, "SUCCESS", agent.sessionToken);
        agent.result(task, "FAILED", agent.sessionToken);

        TaskResult result = attempt.get(3, TimeUnit.SECONDS);
        assertTrue(result.isSuccess());
        assertEquals(TaskLifecycleState.COMPLETED, snapshot("task-001").state());
    }

    @Test
    void aResultThatOvertakesItsAckStillCompletesTheAttempt() throws Exception {

        FakeMqttAgent agent = f.agent("android-a1");
        agent.mode = FakeMqttAgent.Mode.SILENT;

        Future<TaskResult> attempt = runAsync(agent, "task-001");
        awaitTask(agent, 1);

        agent.result(agent.lastTask(), "SUCCESS", agent.sessionToken);

        assertTrue(attempt.get(3, TimeUnit.SECONDS).isSuccess());
        assertTrue(snapshot("task-001").reachedNode());
    }

    @Test
    void aRedeliveredTaskIsAcknowledgedButNotRunTwice() throws Exception {

        FakeMqttAgent agent = f.agent("android-a1");
        agent.mode = FakeMqttAgent.Mode.ACK_ONLY;

        Future<TaskResult> attempt = runAsync(agent, "task-001");
        awaitTask(agent, 1);

        // The broker redelivers the same QoS 1 message.
        f.coreConnection.publish(
                f.link.topics().down("android-a1", MqttTopics.Down.TASK),
                FakeMqttAgent.bytes(agent.lastTask().toString()), 1, false);

        agent.result(agent.lastTask(), "SUCCESS", agent.sessionToken);

        assertTrue(attempt.get(3, TimeUnit.SECONDS).isSuccess());
        assertEquals(2, agent.tasks.size());
    }

    @Test
    void aRetryGetsANewAttemptAndTheOldAttemptsResultCannotCompleteIt() throws Exception {

        FakeMqttAgent agent = f.agent("android-a1");
        agent.mode = FakeMqttAgent.Mode.ACK_ONLY;

        assertEquals(TaskFailureReason.EXECUTION_TIMEOUT, run(agent, "task-001").failureReason());
        JsonObject first = agent.lastTask();

        Future<TaskResult> retry = runAsync(agent, "task-001");
        awaitTask(agent, 2);
        JsonObject second = agent.lastTask();

        assertEquals(2, second.get("attemptNumber").getAsInt());
        assertNotEquals(first.get("attemptId"), second.get("attemptId"));

        // Attempt 1 finally finishes: it must not complete attempt 2.
        agent.result(first, "SUCCESS", agent.sessionToken);
        assertFalse(retry.isDone());

        agent.result(second, "SUCCESS", agent.sessionToken);
        assertTrue(retry.get(3, TimeUnit.SECONDS).isSuccess());
    }

    @Test
    void anAgentThatRegisteredAgainKeepsReportingForItsInFlightAttempt() throws Exception {

        FakeMqttAgent agent = f.agent("android-a1");
        agent.mode = FakeMqttAgent.Mode.ACK_ONLY;

        Future<TaskResult> attempt = runAsync(agent, "task-001");
        awaitTask(agent, 1);
        String oldToken = agent.sessionToken;

        agent.register(MqttFixture.ENROLLMENT_TOKEN);   // e.g. after a reconnect

        agent.result(agent.lastTask(), "SUCCESS", oldToken);
        assertThrowsTimeout(attempt);

        agent.result(agent.lastTask(), "SUCCESS", agent.sessionToken);
        assertTrue(attempt.get(3, TimeUnit.SECONDS).isSuccess());
    }

    private static void assertThrowsTimeout(Future<TaskResult> future) throws Exception {
        try {
            future.get(100, TimeUnit.MILLISECONDS);
            throw new AssertionError("a result with a stale session token was accepted");
        } catch (TimeoutException expected) {
            // still waiting, as required
        }
    }
}
