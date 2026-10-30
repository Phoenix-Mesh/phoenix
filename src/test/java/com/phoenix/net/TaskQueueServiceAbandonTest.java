// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.phoenix.core.AttemptFencing;
import com.phoenix.core.AttemptAbandonedException;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskLifecycleState;
import com.phoenix.model.TaskResult;

/**
 * In-flight reconciliation as seen by the pull queue.
 */
class TaskQueueServiceAbandonTest {

    private final TaskLifecycleService lifecycle = new TaskLifecycleService();
    private final TaskQueueService queue = new TaskQueueService(lifecycle);
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void stop() {
        executor.shutdownNow();
    }

    /**
     * Simulates the narrow race where the node is declared OFFLINE after the
     * lifecycle attempt is created but before TaskQueueService can publish
     * the task to the per-device queue.
     */
    private static final class ImmediatelyAbandoningLifecycle
            extends TaskLifecycleService {

        @Override
        public Attempt accepted(
                String taskId,
                String nodeId,
                AttemptFencing fencing) {

            Attempt attempt = super.accepted(taskId, nodeId, fencing);
            abandonAttemptsOn(nodeId);
            return attempt;
        }
    }

    private static TaskDescriptor task(String id) {
        return new TaskDescriptor(id, "Monte-Carlo-Pi", 1, 128);
    }

    private static TaskResult success(String id) {
        return new TaskResult(id, "SUCCESS", "ok");
    }

    private TaskLifecycleState state(String id) {
        return lifecycle.snapshot(id).orElseThrow().state();
    }

    @Test
    void anAttemptAbandonedBeforeQueuePublicationIsNotLeftQueued()
            throws Exception {

        ImmediatelyAbandoningLifecycle immediate =
                new ImmediatelyAbandoningLifecycle();
        TaskQueueService queue =
                new TaskQueueService(immediate);

        TaskLifecycleService.Attempt attempt =
                queue.submit(
                        "n1",
                        "dev-1",
                        task("task-before-publication"),
                        AttemptFencing.LEGACY
                );

        assertTrue(attempt.isAbandoned());
        assertEquals(0, queue.queueLength("dev-1"));
        assertTrue(queue.poll("dev-1").isEmpty());

        assertThrows(
                AttemptAbandonedException.class,
                () -> queue.awaitDelivery(
                        "task-before-publication",
                        Duration.ofSeconds(1)
                )
        );
    }

    @Test
    void abandoningAQueuedTaskWakesTheDeliveryWaiterPromptly()
            throws Exception {

        queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);

        Future<Object> waiter = executor.submit(() -> {
            try {
                queue.awaitDelivery("task-001", Duration.ofSeconds(30));
                return "returned";
            } catch (AttemptAbandonedException exception) {
                return "abandoned";
            }
        });

        Thread.sleep(100);
        long start = System.nanoTime();
        lifecycle.abandonAttemptsOn("n1");

        assertEquals("abandoned", waiter.get(2, TimeUnit.SECONDS));
        assertTrue(Duration.ofNanos(System.nanoTime() - start)
                .compareTo(Duration.ofSeconds(1)) < 0);
        assertEquals(TaskLifecycleState.ABANDONED, state("task-001"));
    }

    @Test
    void anAbandonedTaskIsWithdrawnSoAReconnectingNodeNeverPullsIt() {

        queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);
        assertEquals(1, queue.queueLength("dev-1"));

        lifecycle.abandonAttemptsOn("n1");

        assertEquals(0, queue.queueLength("dev-1"));
        assertTrue(queue.poll("dev-1").isEmpty());
    }

    @Test
    void abandoningADeliveredTaskWakesTheResultWaiter() throws Exception {

        queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);
        queue.poll("dev-1");
        assertTrue(queue.awaitDelivery("task-001", Duration.ofMillis(10)));

        Future<Object> waiter = executor.submit(() -> {
            try {
                queue.awaitResult("task-001", Duration.ofSeconds(30));
                return "returned";
            } catch (AttemptAbandonedException exception) {
                return "abandoned";
            }
        });

        Thread.sleep(100);
        lifecycle.abandonAttemptsOn("n1");

        assertEquals("abandoned", waiter.get(2, TimeUnit.SECONDS));
    }

    @Test
    void lateResultAndLateStartedAckForAnAbandonedAttemptAreRejected() {

        queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);
        queue.poll("dev-1");
        lifecycle.abandonAttemptsOn("n1");

        assertFalse(queue.completeResult(success("task-001")));
        assertFalse(queue.markStarted("task-001"));
        assertEquals(TaskLifecycleState.ABANDONED, state("task-001"));
    }

    @Test
    void anAttemptOnAnotherNodeIsUntouched() throws Exception {

        queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);
        queue.submit("n2", "dev-2", task("task-002"), AttemptFencing.LEGACY);

        lifecycle.abandonAttemptsOn("n1");

        assertEquals(1, queue.queueLength("dev-2"));
        assertTrue(queue.poll("dev-2").isPresent());
        assertTrue(queue.completeResult(success("task-002")));
    }

    @Test
    void pollDeliveryCarriesTheAttemptIdAndNumber() {

        queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);
        TaskQueueService.Delivery first =
                queue.pollDelivery("dev-1").orElseThrow();
        assertEquals(1, first.attemptNumber());

        lifecycle.abandonAttemptsOn("n1");
        queue.discard("task-001");

        queue.submit("n2", "dev-2", task("task-001"), AttemptFencing.LEGACY);
        TaskQueueService.Delivery second =
                queue.pollDelivery("dev-2").orElseThrow();

        assertEquals(2, second.attemptNumber());
        assertNotEquals(first.attemptId(), second.attemptId());
        assertEquals(
                second.attemptId(),
                lifecycle.snapshot("task-001").orElseThrow().attemptId());
    }

    @Test
    void aResultEchoingAnOldAttemptCannotCompleteTheRetry() {

        // Attempt 1 on n1: delivered, then abandoned (n1 went OFFLINE).
        TaskLifecycleService.Attempt first =
                queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);
        queue.poll("dev-1");
        lifecycle.abandonAttemptsOn("n1");
        queue.discard("task-001");

        // Attempt 2 on n2: delivered and running.
        TaskLifecycleService.Attempt second =
                queue.submit("n2", "dev-2", task("task-001"), AttemptFencing.LEGACY);
        queue.poll("dev-2");

        // n1 comes back and reports attempt 1's result late.
        assertFalse(queue.completeResult(success("task-001"), first.id()));
        assertFalse(queue.markStarted("task-001", first.id()));
        assertEquals(TaskLifecycleState.DELIVERED, state("task-001"));

        // The genuine result of attempt 2 is accepted.
        assertTrue(queue.completeResult(success("task-001"), second.id()));
        assertEquals(TaskLifecycleState.COMPLETED, state("task-001"));
    }

    @Test
    void theNumericAttemptNeverIdentifiesAReport() {

        queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);
        queue.poll("dev-1");

        // "1" is attempt 1's number, but the identity is the opaque id.
        assertFalse(queue.completeResult(success("task-001"), "1"));
        assertFalse(queue.completeResult(success("task-001"), ""));
        assertFalse(queue.completeResult(success("task-001"), "  "));
        assertEquals(TaskLifecycleState.DELIVERED, state("task-001"));
    }

    @Test
    void anIdFromAnEarlierCoreRunCannotCompleteTheSameTaskIdNow() {

        // Two Core runs both start at task-001 / attempt 1. With only the
        // number to go on they were indistinguishable.
        TaskLifecycleService before = new TaskLifecycleService();
        String oldRunId = new TaskQueueService(before)
                .submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY).id();

        queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);
        queue.poll("dev-1");

        assertFalse(queue.completeResult(success("task-001"), oldRunId));
        assertEquals(TaskLifecycleState.DELIVERED, state("task-001"));
    }

    @Test
    void anAgentThatDoesNotEchoTheAttemptStillWorks() {

        queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);
        queue.poll("dev-1");

        assertTrue(queue.markStarted("task-001", null));
        assertTrue(queue.completeResult(success("task-001"), null));
    }

    @Test
    void anOrphanedQueueEntryIsSkippedNotDelivered() {

        queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);
        queue.discard("task-001");           // attempt state gone, entry left

        Optional<TaskDescriptor> polled = queue.poll("dev-1");

        assertTrue(polled.isEmpty());
    }

    @Test
    void abandoningTwiceOrAfterCompletionIsHarmless() {

        queue.submit("n1", "dev-1", task("task-001"), AttemptFencing.LEGACY);
        queue.poll("dev-1");
        assertTrue(queue.completeResult(success("task-001")));

        assertTrue(lifecycle.abandonAttemptsOn("n1").isEmpty());
        assertEquals(TaskLifecycleState.COMPLETED, state("task-001"));
        assertThrows(IllegalStateException.class,
                () -> queue.awaitDelivery("ghost", Duration.ofMillis(5)));
    }
}
