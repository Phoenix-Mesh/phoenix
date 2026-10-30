// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

/**
 * A unit of work submitted to Core, before placement.
 *
 * @param id           task identifier; unique within one Core run only
 * @param type         workload type understood by the node agent
 * @param requirements hard constraints a node must satisfy to be eligible
 */
public record TaskDescriptor(
        String id,
        String type,
        TaskRequirements requirements
) {

    /** A task that only constrains CPU cores and memory. */
    public TaskDescriptor(String id, String type, int cpuRequirement, int memoryRequirementMb) {
        this(id, type, TaskRequirements.of(cpuRequirement, memoryRequirementMb));
    }
}
