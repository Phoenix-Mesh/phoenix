// SPDX-License-Identifier: Apache-2.0

package com.phoenix.scheduler;

import com.phoenix.model.NodeRecord;
import com.phoenix.model.TaskDescriptor;

/**
 * Computes the placement cost C(n) of running a given task on a given node.
 *
 * <p>Deliberately its own interface rather than inline math inside
 * {@link TaskScheduler} implementations (design discussion, item 10):
 *
 * <pre>
 * TaskScheduler -&gt; PlacementCostModel -&gt; cost(node, task)
 * </pre>
 *
 * <p>This means a future Memetic or sustainability-aware scheduler can
 * reuse (or replace) just the cost function without re-deriving eligibility
 * filtering, argmin selection, or logging.
 */
public interface PlacementCostModel {

    /** Lower is better. Implementations should never return NaN for an eligible node. */
    double cost(NodeRecord node, TaskDescriptor task);

    /** A per-term breakdown, useful for the "[Scheduler] node cost=..." style logging. */
    CostBreakdown breakdown(NodeRecord node, TaskDescriptor task);

    /**
     * One term-by-term view of a cost computation, named after the design
     * doc's vocabulary (execution time / energy / load / latency) rather
     * than T/E/L/R.
     */
    record CostBreakdown(
            String nodeId,
            double executionTime,
            double energyCost,
            double loadPenalty,
            double networkLatency,
            double total
    ) {}
}
