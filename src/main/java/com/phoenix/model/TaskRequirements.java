// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

import java.util.Optional;
import java.util.Set;

/**
 * What a task needs from a node, kept separate from {@link TaskDescriptor}
 * per the design discussion (item 11): Oakestra-style capacity requirements
 * plus room to grow (latency budget, required hardware class, capability
 * tags) without turning TaskDescriptor into a 15-argument constructor.
 *
 * <p>All five fields are actively enforced as hard eligibility requirements
 * by {@link com.phoenix.scheduler.GreedyCostScheduler}: {@code cpuCores},
 * {@code memoryMb}, {@code requiredHardwareClass}, {@code requiredCapabilities}
 * and {@code maxNetworkLatencyMs} must all pass before a node is considered
 * eligible. Only eligible nodes are passed to the placement cost model; the
 * cost model ranks eligible candidates but never compensates for a violated
 * hard requirement.
 */
public record TaskRequirements(
        int cpuCores,
        long memoryMb,
        Optional<Double> maxNetworkLatencyMs,
        Optional<HardwareClass> requiredHardwareClass,
        Set<String> requiredCapabilities
) {

    /** Minimal requirements: just CPU + memory, no extra constraints. */
    public static TaskRequirements of(int cpuCores, long memoryMb) {
        return new TaskRequirements(
                cpuCores, memoryMb, Optional.empty(), Optional.empty(), Set.of());
    }
}
