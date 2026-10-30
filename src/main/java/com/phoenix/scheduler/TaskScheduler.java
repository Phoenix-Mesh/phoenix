// SPDX-License-Identifier: Apache-2.0

package com.phoenix.scheduler;

import java.util.List;
import java.util.Optional;

import com.phoenix.model.NodeRecord;
import com.phoenix.model.TaskDescriptor;

/**
 * Placement policy: chooses which node should run a task.
 *
 * <p>An interface so that the greedy baseline can later be compared against
 * other policies (memetic, sustainability-aware) by changing one binding in
 * {@link com.phoenix.core.PhoenixModule}. Core consumes it only through
 * {@link com.phoenix.core.PlacementReservationService}.
 */
public interface TaskScheduler {

    /**
     * Selects the best node for {@code task}.
     *
     * <p>{@code candidates} are already filtered for liveness and reservable
     * capacity; the scheduler still enforces every hard requirement in
     * {@link com.phoenix.model.TaskRequirements}.
     *
     * @return the chosen node, or empty if no candidate is eligible
     */
    Optional<NodeRecord> selectNode(TaskDescriptor task, List<NodeRecord> candidates);
}
