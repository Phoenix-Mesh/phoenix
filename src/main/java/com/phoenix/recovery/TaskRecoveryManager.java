// SPDX-License-Identifier: Apache-2.0

package com.phoenix.recovery;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.model.TaskResult;

/**
 * Retry policy for a single placement cycle.
 *
 * <p>Owns how many nodes one dispatch may try and which failures may be
 * re-placed. Node selection itself belongs to
 * {@link com.phoenix.core.PlacementReservationService}; the budget across
 * cycles belongs to {@link com.phoenix.dispatch.PendingTaskQueue}.
 *
 * <p>System property: {@code phoenix.maxAttempts} (default 3, at least 1).
 */
@Singleton
public class TaskRecoveryManager {

    private final int maxAttempts;

    @Inject
    public TaskRecoveryManager() {
        this(Integer.getInteger("phoenix.maxAttempts", 3));
    }

    /**
     * @param maxAttempts nodes one placement cycle may try
     * @throws IllegalArgumentException if {@code maxAttempts} is below 1
     */
    public TaskRecoveryManager(int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException(
                    "phoenix.maxAttempts must be at least 1, was " + maxAttempts);
        }
        this.maxAttempts = maxAttempts;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public boolean isRetryable(TaskResult result) {
        return result != null && result.isRetryable();
    }
}
