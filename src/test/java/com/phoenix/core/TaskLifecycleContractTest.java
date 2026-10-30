// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskLifecycleState;
import com.phoenix.model.TaskResult;

/** Issue #37: one place decides how a node-reported result ends an attempt. */
class TaskLifecycleContractTest {

    private final TaskLifecycleService lifecycle =
            new TaskLifecycleService();

    private TaskLifecycleState end(TaskResult result) {
        TaskLifecycleService.Attempt attempt =
                lifecycle.accepted(
                        result.taskId(), "n1", AttemptFencing.LEGACY);
        lifecycle.delivered(attempt);
        lifecycle.reported(attempt, result);
        return lifecycle.snapshot(result.taskId()).orElseThrow().state();
    }

    @Test
    void successCompletes() {
        assertEquals(
                TaskLifecycleState.COMPLETED,
                end(new TaskResult("t1", "SUCCESS", "ok")));
    }

    @Test
    void successIsRecognisedCaseInsensitively() {
        assertEquals(
                TaskLifecycleState.COMPLETED,
                end(new TaskResult("t1", "success", "ok")));
    }

    @Test
    void aNodeReportedFailureFails() {
        assertEquals(
                TaskLifecycleState.FAILED,
                end(new TaskResult(
                        "t1", "FAILED", "boom",
                        TaskFailureReason.EXECUTION_ERROR)));
    }

    @Test
    void anyNonSuccessStatusFails() {
        for (String status : new String[] {"ERROR", "TIMEOUT", "", "UNKNOWN"}) {
            assertEquals(
                    TaskLifecycleState.FAILED,
                    end(new TaskResult(
                            "t-" + status, status, "x",
                            TaskFailureReason.EXECUTION_ERROR)),
                    "status '" + status + "'");
        }
    }

    @Test
    void aReportFromAReplacedAttemptIsIgnored() {

        TaskLifecycleService.Attempt first = lifecycle.accepted("t1", "n1", AttemptFencing.LEGACY);
        lifecycle.delivered(first);

        TaskLifecycleService.Attempt second = lifecycle.accepted("t1", "n2", AttemptFencing.LEGACY);

        lifecycle.reported(first, new TaskResult("t1", "SUCCESS", "late"));

        assertEquals(
                TaskLifecycleState.ACCEPTED,
                lifecycle.snapshot("t1").orElseThrow().state());
        assertEquals(2, second.number());
    }

    @Test
    void aReportCannotResurrectAnAbandonedAttempt() {

        TaskLifecycleService.Attempt attempt = lifecycle.accepted("t1", "n1", AttemptFencing.LEGACY);
        lifecycle.abandonAttemptsOn("n1");

        lifecycle.reported(attempt, new TaskResult("t1", "SUCCESS", "late"));

        assertEquals(
                TaskLifecycleState.ABANDONED,
                lifecycle.snapshot("t1").orElseThrow().state());
    }

    @Test
    void nullArgumentsAreRejected() {
        TaskLifecycleService.Attempt attempt = lifecycle.accepted("t1", "n1", AttemptFencing.LEGACY);

        assertThrows(NullPointerException.class,
                () -> lifecycle.reported(attempt, null));
        assertThrows(NullPointerException.class,
                () -> lifecycle.reported(null, new TaskResult("t1", "SUCCESS", "")));
    }
}
