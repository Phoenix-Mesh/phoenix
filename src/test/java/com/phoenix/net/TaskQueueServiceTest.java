// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.phoenix.core.AttemptFencing;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskLifecycleState;
import com.phoenix.model.TaskResult;
import com.phoenix.testsupport.MutableClock;

class TaskQueueServiceTest {

    private static final String DEVICE = "device-1";

    private final TaskLifecycleService lifecycle =
            new TaskLifecycleService();

    private final TaskQueueService queue =
            new TaskQueueService(lifecycle);

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
    void submitAcceptsButDoesNotDeliver() {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);

        assertEquals(TaskLifecycleState.ACCEPTED, state("task-001"));
        assertEquals(1, queue.queueLength(DEVICE));
    }

    @Test
    void pollHandsTheTaskOverAndMarksItDelivered() throws Exception {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);

        Optional<TaskDescriptor> polled = queue.poll(DEVICE);

        assertTrue(polled.isPresent());
        assertEquals("task-001", polled.get().id());
        assertEquals(TaskLifecycleState.DELIVERED, state("task-001"));
        assertTrue(
                queue.awaitDelivery("task-001", Duration.ofMillis(10))
        );
    }

    @Test
    void awaitDeliveryReportsFalseWhileTheTaskIsStillQueued()
            throws Exception {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);

        assertFalse(
                queue.awaitDelivery("task-001", Duration.ofMillis(50))
        );
        assertEquals(1, queue.queueLength(DEVICE));
        assertEquals(TaskLifecycleState.ACCEPTED, state("task-001"));
    }

    @Test
    void awaitDeliveryWakesWhenTheNodePolls() throws Exception {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);

        Thread poller = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
            queue.poll(DEVICE);
        });
        poller.start();

        assertTrue(
                queue.awaitDelivery("task-001", Duration.ofSeconds(3))
        );

        poller.join();
    }

    @Test
    void awaitDeliveryForUnknownTaskIsAnError() {

        assertThrows(
                IllegalStateException.class,
                () -> queue.awaitDelivery("ghost", Duration.ofMillis(10))
        );
    }

    @Test
    void cancelRemovesAnUndeliveredTaskSoTheNodeNeverSeesIt() {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);

        assertTrue(queue.cancelQueuedTask(DEVICE, "task-001"));
        assertEquals(0, queue.queueLength(DEVICE));
        assertTrue(queue.poll(DEVICE).isEmpty());
    }

    @Test
    void cancelIsRefusedOnceTheNodeHasPulledTheTask() {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);
        queue.poll(DEVICE);

        assertFalse(queue.cancelQueuedTask(DEVICE, "task-001"));
    }

    @Test
    void resultForAnUndeliveredAttemptIsRejectedAsStale() {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);

        assertFalse(queue.completeResult(success("task-001")));
        assertEquals(TaskLifecycleState.ACCEPTED, state("task-001"));
    }

    @Test
    void resultAfterDeliveryCompletesTheAttempt() throws Exception {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);
        queue.poll(DEVICE);

        assertTrue(queue.completeResult(success("task-001")));

        TaskResult result =
                queue.awaitResult("task-001", Duration.ofSeconds(1));

        assertTrue(result.isSuccess());
        assertEquals(TaskLifecycleState.COMPLETED, state("task-001"));
    }

    @Test
    void duplicateResultIsRejected() {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);
        queue.poll(DEVICE);

        assertTrue(queue.completeResult(success("task-001")));
        assertFalse(queue.completeResult(success("task-001")));
    }

    @Test
    void lateResultFromAnEarlierAttemptCannotCompleteARequeuedAttempt()
            throws Exception {

        // Attempt 1: delivered, node goes silent, execution times out.
        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);
        queue.poll(DEVICE);

        assertThrows(
                TimeoutException.class,
                () -> queue.awaitResult(
                        "task-001",
                        Duration.ofMillis(30))
        );

        // Attempt 2: recovery re-queues the same task; no node has seen it.
        queue.submit("n2", "device-2", task("task-001"), AttemptFencing.LEGACY);

        // The slow first node finally answers.
        assertFalse(queue.completeResult(success("task-001")));
        assertEquals(TaskLifecycleState.ACCEPTED, state("task-001"));

        // The genuine second delivery still works normally.
        queue.poll("device-2");
        assertTrue(queue.completeResult(success("task-001")));
    }

    @Test
    void startedAckRequiresDelivery() {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);

        assertFalse(queue.markStarted("task-001"));

        queue.poll(DEVICE);

        assertTrue(queue.markStarted("task-001"));
        assertEquals(TaskLifecycleState.STARTED, state("task-001"));
    }

    @Test
    void startedAckForUnknownTaskIsRejected() {
        assertFalse(queue.markStarted("ghost"));
    }

    @Test
    void pollRecordsContactEvenWhenThereIsNoWork() {

        MutableClock clock = new MutableClock();
        TaskQueueService timed =
                new TaskQueueService(new TaskLifecycleService(clock), clock);

        assertTrue(timed.timeSinceLastPoll(DEVICE).isEmpty());

        timed.poll(DEVICE); // empty queue, no work

        clock.advance(Duration.ofSeconds(5));

        assertEquals(
                Optional.of(Duration.ofSeconds(5)),
                timed.timeSinceLastPoll(DEVICE)
        );
    }

    @Test
    void recordPollRefreshesContactWithoutTouchingTheQueue() {

        MutableClock clock = new MutableClock();
        TaskQueueService timed =
                new TaskQueueService(new TaskLifecycleService(clock), clock);

        timed.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);
        timed.recordPoll(DEVICE);

        clock.advance(Duration.ofSeconds(2));
        timed.recordPoll(DEVICE);
        clock.advance(Duration.ofSeconds(1));

        assertEquals(
                Optional.of(Duration.ofSeconds(1)),
                timed.timeSinceLastPoll(DEVICE)
        );
        assertEquals(1, timed.queueLength(DEVICE));
    }

    @Test
    void discardDropsPendingStateAndIsIdempotent() {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);
        queue.discard("task-001");
        queue.discard("task-001");

        assertThrows(
                IllegalStateException.class,
                () -> queue.awaitDelivery("task-001", Duration.ofMillis(5))
        );
    }

    @Test
    void aNodeReportedFailureEndsTheAttemptAsFailedNotCompleted()
            throws Exception {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);
        queue.poll(DEVICE);

        assertTrue(queue.completeResult(
                new TaskResult(
                        "task-001",
                        "FAILED",
                        "boom",
                        com.phoenix.model.TaskFailureReason.EXECUTION_ERROR)));

        // Issue #37: the pull transport used to record COMPLETED here while
        // the push transport recorded FAILED for the same node outcome.
        assertEquals(TaskLifecycleState.FAILED, state("task-001"));
    }

    @Test
    void aNodeReportedSuccessEndsTheAttemptAsCompleted()
            throws Exception {

        queue.submit("n1", DEVICE, task("task-001"), AttemptFencing.LEGACY);
        queue.poll(DEVICE);

        assertTrue(queue.completeResult(success("task-001")));

        assertEquals(TaskLifecycleState.COMPLETED, state("task-001"));
    }
}
