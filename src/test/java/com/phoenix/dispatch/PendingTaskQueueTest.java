// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.phoenix.model.TaskDescriptor;

class PendingTaskQueueTest {

    @Test
    void snapshotPreservesFifoInsertionOrder() {

        PendingTaskQueue queue =
                new PendingTaskQueue();

        queue.enqueue(task("task-001"));
        queue.enqueue(task("task-002"));
        queue.enqueue(task("task-003"));

        assertEquals(
                List.of(
                        "task-001",
                        "task-002",
                        "task-003"
                ),
                queue.snapshot()
                        .stream()
                        .map(p -> p.task().id())
                        .toList()
        );
    }

    @Test
    void duplicateEnqueueDoesNotChangeFifoPosition() {

        PendingTaskQueue queue =
                new PendingTaskQueue();

        queue.enqueue(task("task-001"));
        queue.enqueue(task("task-002"));

        /*
         * Re-enqueueing an already pending task must NOT move it
         * to the end of the queue.
         */
        queue.enqueue(task("task-001"));

        assertEquals(
                List.of(
                        "task-001",
                        "task-002"
                ),
                queue.snapshot()
                        .stream()
                        .map(p -> p.task().id())
                        .toList()
        );
    }

    @Test
    void removeAdvancesFifoOrder() {

        PendingTaskQueue queue =
                new PendingTaskQueue();

        queue.enqueue(task("task-001"));
        queue.enqueue(task("task-002"));
        queue.enqueue(task("task-003"));

        assertTrue(
                queue.remove("task-001")
        );

        assertEquals(
                List.of(
                        "task-002",
                        "task-003"
                ),
                queue.snapshot()
                        .stream()
                        .map(p -> p.task().id())
                        .toList()
        );
    }

    @Test
    void removingUnknownTaskReturnsFalse() {

        PendingTaskQueue queue =
                new PendingTaskQueue();

        queue.enqueue(task("task-001"));

        assertFalse(
                queue.remove("task-999")
        );
    }

    private static TaskDescriptor task(
            String id) {

        return new TaskDescriptor(
                id,
                "Monte-Carlo-Pi",
                2,
                512
        );
    }
}
