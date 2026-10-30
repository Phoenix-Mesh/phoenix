// SPDX-License-Identifier: Apache-2.0

package com.phoenix.tui;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.inject.Singleton;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskResult;

/**
 * Display-side record of recent tasks for the operator console.
 *
 * <p>Not authoritative: lifecycle truth lives in
 * {@link com.phoenix.core.TaskLifecycleService}. Retention is bounded so a
 * long-running Core does not accumulate every task it has ever seen; once
 * {@link #MAX_ENTRIES} is exceeded the oldest submissions are dropped.
 */
@Singleton
public class TaskJournal {

    /** Upper bound on retained tasks. */
    public static final int MAX_ENTRIES = 256;

    private final Map<String, TaskState> tasks =
            new ConcurrentHashMap<>();

    public void submitted(TaskDescriptor task) {
        tasks.put(
                task.id(),
                new TaskState(
                        task.id(),
                        task.type(),
                        null,
                        "SUBMITTED",
                        Instant.now(),
                        null,
                        null,
                        1,
                        null
                )
        );

        evictOldest();
    }

    /** Number of tasks currently retained. */
    public int size() {
        return tasks.size();
    }

    private synchronized void evictOldest() {
        int excess = tasks.size() - MAX_ENTRIES;

        if (excess <= 0) {
            return;
        }

        tasks.values()
                .stream()
                .sorted(Comparator.comparing(TaskState::submittedAt))
                .limit(excess)
                .map(TaskState::taskId)
                .toList()
                .forEach(tasks::remove);
    }

    public void assigned(
            String taskId,
            String nodeName) {

        tasks.computeIfPresent(
                taskId,
                (ignored, state) ->
                        state.withAssignment(nodeName)
        );
    }

    public void waiting(
            String taskId,
            String reason) {

        tasks.computeIfPresent(
                taskId,
                (ignored, state) ->
                        state.withWaiting(reason)
        );
    }

    public void retrying(
            String taskId,
            String nodeName,
            int attempt,
            String reason) {

        tasks.computeIfPresent(
                taskId,
                (ignored, state) ->
                        state.withRetry(
                                nodeName,
                                attempt,
                                reason
                        )
        );
    }

    public void completed(
            String taskId,
            TaskResult result) {

        tasks.computeIfPresent(
                taskId,
                (ignored, state) ->
                        state.withResult(
                                result,
                                Instant.now()
                        )
        );
    }

    public List<TaskState> recent(int limit) {
        return tasks.values()
                .stream()
                .sorted(
                        Comparator.comparing(
                                TaskState::submittedAt
                        ).reversed()
                )
                .limit(limit)
                .toList();
    }

    public record TaskState(
            String taskId,
            String type,
            String nodeName,
            String status,
            Instant submittedAt,
            Instant completedAt,
            String output,
            int attempt,
            String failureReason
    ) {

        TaskState withAssignment(String node) {
            return new TaskState(
                    taskId,
                    type,
                    node,
                    "RUNNING",
                    submittedAt,
                    null,
                    output,
                    attempt,
                    failureReason
            );
        }

        TaskState withWaiting(String reason) {
            return new TaskState(
                    taskId,
                    type,
                    nodeName,
                    "WAITING",
                    submittedAt,
                    null,
                    output,
                    attempt,
                    reason
            );
        }

        TaskState withRetry(
                String node,
                int nextAttempt,
                String reason) {
            return new TaskState(
                    taskId,
                    type,
                    node,
                    "RETRYING",
                    submittedAt,
                    null,
                    output,
                    nextAttempt,
                    reason
            );
        }

        TaskState withResult(
                TaskResult result,
                Instant completedAt) {
            return new TaskState(
                    taskId,
                    type,
                    nodeName,
                    result.status(),
                    submittedAt,
                    completedAt,
                    result.outputData(),
                    attempt,
                    result.failureReason().name()
            );
        }

        public long elapsedMs() {
            Instant end =
                    completedAt != null
                            ? completedAt
                            : Instant.now();

            return Duration.between(
                    submittedAt,
                    end
            ).toMillis();
        }
    }
}
