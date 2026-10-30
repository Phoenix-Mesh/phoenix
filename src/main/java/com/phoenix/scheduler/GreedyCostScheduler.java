// SPDX-License-Identifier: Apache-2.0

package com.phoenix.scheduler;

import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import com.google.inject.Inject;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskRequirements;

/**
 * Greedy argmin implementation of Phoenix's weighted placement model.
 */
public class GreedyCostScheduler implements TaskScheduler {

    private final PlacementCostModel costModel;

    @Inject
    public GreedyCostScheduler(
            PlacementCostModel costModel) {
        this.costModel = costModel;
    }

    @Override
    public Optional<NodeRecord> selectNode(
            TaskDescriptor task,
            List<NodeRecord> candidates) {

        List<NodeRecord> eligible = candidates.stream()
                .filter(node -> isEligible(node, task))
                .toList();

        if (eligible.isEmpty()) {
            System.out.println(
                    "[Scheduler] No eligible nodes for task "
                            + task.id()
            );
            return Optional.empty();
        }

        NodeRecord best = eligible.get(0);
        double bestCost = Double.NaN;
        boolean firstCandidate = true;
        StringJoiner summary = new StringJoiner(" | ");

        for (NodeRecord node : eligible) {
            PlacementCostModel.CostBreakdown breakdown =
                    costModel.breakdown(node, task);

            /*
             * Double.compare orders NaN above every number, so a node whose
             * cost cannot be computed is chosen only if no node has a real
             * cost. The first candidate is always accepted with its own
             * cost, so an eligible node is always selected.
             */
            if (firstCandidate
                    || Double.compare(breakdown.total(), bestCost) < 0) {
                bestCost = breakdown.total();
                best = node;
            }
            firstCandidate = false;

            summary.add(
                    String.format(
                            "%s{C=%.3f,T=%.2f,E=%.2f,L=%.2f,R=%.2f}",
                            node.info().displayName(),
                            breakdown.total(),
                            breakdown.executionTime(),
                            breakdown.energyCost(),
                            breakdown.loadPenalty(),
                            breakdown.networkLatency()
                    )
            );
        }

        System.out.printf(
                "[Placement] %s candidates=%d | %s | SELECTED=%s cost=%.3f%n",
                task.id(),
                eligible.size(),
                summary,
                best.info().displayName(),
                bestCost
        );

        return Optional.of(best);
    }

    /**
     * A candidate node is eligible only when every hard constraint declared
     * by {@link TaskRequirements} passes: CPU, memory, hardware class,
     * required capabilities, and maximum network latency (issue #14 — these
     * were declared on the model but not all of them were previously
     * enforced here). The cost model only ever ranks nodes that pass all
     * five; it never compensates for a violated hard requirement.
     */
    private boolean isEligible(
            NodeRecord node,
            TaskDescriptor task) {

        TaskRequirements req = task.requirements();

        boolean cpuOk =
                node.info().cpuCores() >= req.cpuCores();

        boolean memOk =
                node.status().availableRamMb()
                        >= req.memoryMb();

        boolean hardwareOk =
                req.requiredHardwareClass()
                        .map(required ->
                                required == node.info().hardwareClass())
                        .orElse(true);

        boolean capabilitiesOk =
                node.info().capabilities()
                        .containsAll(req.requiredCapabilities());

        boolean latencyOk =
                req.maxNetworkLatencyMs()
                        .map(maxLatencyMs ->
                                node.status().networkLatencyMs() <= maxLatencyMs)
                        .orElse(true);

        return cpuOk && memOk && hardwareOk && capabilitiesOk && latencyOk;
    }
}
