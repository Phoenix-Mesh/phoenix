// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.phoenix.core.AttemptFencing;
import com.phoenix.core.AttemptAbandonedException;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskLifecycleState;

/**
 * The node can be declared OFFLINE between accepted() and the queue insert.
 * An already-abandoned attempt must never be published to the device queue.
 */
class TaskQueueServicePreAbandonedSubmitTest {

    /** Abandons the attempt the instant it is created: the worst-case race. */
    private static final class AbandonImmediately
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

    @Test
    void anAttemptAbandonedDuringSubmitIsNeverPublishedToTheNode() {

        AbandonImmediately lifecycle = new AbandonImmediately();
        TaskQueueService queue = new TaskQueueService(lifecycle);

        TaskLifecycleService.Attempt attempt = queue.submit(
                "n1", "dev-1",
                new TaskDescriptor("task-001", "Monte-Carlo-Pi", 1, 128),
                AttemptFencing.LEGACY);

        assertTrue(attempt.isAbandoned());
        assertEquals(0, queue.queueLength("dev-1"));
        assertTrue(queue.poll("dev-1").isEmpty());
        assertEquals(TaskLifecycleState.ABANDONED,
                lifecycle.snapshot("task-001").orElseThrow().state());
    }

    @Test
    void theWaiterOfAnAttemptAbandonedDuringSubmitIsWokenNotStranded() {

        AbandonImmediately lifecycle = new AbandonImmediately();
        TaskQueueService queue = new TaskQueueService(lifecycle);

        queue.submit(
                "n1", "dev-1",
                new TaskDescriptor("task-001", "Monte-Carlo-Pi", 1, 128),
                AttemptFencing.LEGACY);

        assertThrows(
                AttemptAbandonedException.class,
                () -> queue.awaitDelivery("task-001", Duration.ofSeconds(30)));
    }
}
