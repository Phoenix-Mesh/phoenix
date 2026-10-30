// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

/**
 * Thrown inside a transport that is waiting on an execution attempt which
 * Core has abandoned because the executing node went OFFLINE.
 *
 * <p>Unchecked on purpose: it is a control-flow signal that wakes a blocked
 * transport thread so it can return a classified
 * {@link com.phoenix.model.TaskFailureReason#NODE_OFFLINE} result. It never
 * escapes the transport's {@code execute()} method.
 */
public class AttemptAbandonedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String taskId;
    private final int attempt;

    public AttemptAbandonedException(
            TaskLifecycleService.Attempt attempt) {

        this(attempt.taskId(), attempt.number());
    }

    public AttemptAbandonedException(
            String taskId,
            int attempt) {

        super("Attempt " + attempt + " of " + taskId + " was abandoned");
        this.taskId = taskId;
        this.attempt = attempt;
    }

    public String taskId() {
        return taskId;
    }

    public int attempt() {
        return attempt;
    }
}
