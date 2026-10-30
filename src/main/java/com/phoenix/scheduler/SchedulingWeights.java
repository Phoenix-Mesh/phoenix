// SPDX-License-Identifier: Apache-2.0

package com.phoenix.scheduler;

/**
 * Weights of the placement-cost formula
 *
 * <pre>
 * C(n) = w1*executionTime + w2*energyCost + w3*loadPenalty + w4*networkLatency
 * </pre>
 *
 * <p>Held separately from {@link WeightedPlacementCostModel} so that weight
 * profiles can be tuned or swept in experiments without touching the model.
 */
public record SchedulingWeights(
        double executionTimeWeight,
        double energyCostWeight,
        double loadPenaltyWeight,
        double networkLatencyWeight
) {

    /** Execution time dominates; energy, load and latency share the rest. */
    public static SchedulingWeights defaults() {
        return new SchedulingWeights(0.4, 0.2, 0.2, 0.2);
    }
}
