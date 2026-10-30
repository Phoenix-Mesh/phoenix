// SPDX-License-Identifier: Apache-2.0

package com.phoenix.scheduler;

import com.google.inject.Inject;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.ResourceStatus;
import com.phoenix.model.TaskDescriptor;

/**
 * Weighted placement cost from the design document:
 *
 * <pre>
 * C(n) = w1*executionTime(n) + w2*energyCost(n) + w3*loadPenalty(n) + w4*networkLatency(n)
 * </pre>
 *
 * <p>Every term is derived directly from the node's latest
 * {@link ResourceStatus}; there is no prediction model. The formula is
 * deliberately transparent so rankings across heterogeneous nodes stay
 * explainable while it serves as the measured baseline.
 *
 * <ul>
 *   <li><b>executionTime</b> - requested cores divided by the node's free
 *       cores. An unknown CPU usage is treated as fully free.</li>
 *   <li><b>energyCost</b> - the node's {@code energyCostEstimate}.</li>
 *   <li><b>loadPenalty</b> - active tasks plus queue length, so a busy node
 *       is penalised even when its instantaneous CPU usage looks low.</li>
 *   <li><b>networkLatency</b> - the node's {@code networkLatencyMs}.</li>
 * </ul>
 */
public class WeightedPlacementCostModel implements PlacementCostModel {

    private final SchedulingWeights weights;

    @Inject
    public WeightedPlacementCostModel() {
        this(SchedulingWeights.defaults());
    }

    public WeightedPlacementCostModel(SchedulingWeights weights) {
        this.weights = weights;
    }

    @Override
    public double cost(NodeRecord node, TaskDescriptor task) {
        return breakdown(node, task).total();
    }

    @Override
    public CostBreakdown breakdown(NodeRecord node, TaskDescriptor task) {
        NodeInfo info = node.info();
        ResourceStatus status = node.status();

        double executionTime = estimateExecutionTime(info, status, task);
        double energyCost = status.energyCostEstimate();
        double loadPenalty = estimateLoadPenalty(status);
        double networkLatency = status.networkLatencyMs();

        double total =
                weights.executionTimeWeight() * executionTime
                        + weights.energyCostWeight() * energyCost
                        + weights.loadPenaltyWeight() * loadPenalty
                        + weights.networkLatencyWeight() * networkLatency;

        return new CostBreakdown(info.nodeId(), executionTime, energyCost, loadPenalty, networkLatency, total);
    }

    /**
     * Rough proxy: baseline demand (requested CPU cores) divided by the
     * node's effective free capacity (its core count discounted by current
     * CPU usage). Busy or weaker nodes report a higher "time".
     */
    private double estimateExecutionTime(NodeInfo info, ResourceStatus status, TaskDescriptor task) {
        double requestedCores = Math.max(task.requirements().cpuCores(), 1);
        double cpu = status.cpuUsagePercent();
        double freeCapacityFraction = cpu < 0
                ? 1.0
                : Math.max(0.05, 1.0 - (cpu / 100.0));
        double effectiveCores = Math.max(0.1, info.cpuCores() * freeCapacityFraction);
        return requestedCores / effectiveCores;
    }

    private double estimateLoadPenalty(ResourceStatus status) {
        return status.activeTasks() * 0.5 + status.queueLength() * 0.25;
    }
}
