// SPDX-License-Identifier: Apache-2.0

package com.phoenix.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.phoenix.model.HardwareClass;
import com.phoenix.model.NodeInfo;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.ResourceStatus;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskRequirements;

/**
 * This test proves that the scheduler filters out ineligible nodes
 * and picks the lowest-cost eligible node, using the weighted-cost
 * formula from the design.
 *
 * <p>Issue #14 adds regression coverage for the two hard constraints
 * that were declared on {@link TaskRequirements} but not previously
 * enforced by {@link GreedyCostScheduler}: required capabilities and
 * maximum network latency.
 */
class GreedyCostSchedulerTest {

    private final GreedyCostScheduler scheduler =
            new GreedyCostScheduler(new WeightedPlacementCostModel());

    private NodeRecord node(String id, String name, int cores, long memMb, ResourceStatus status) {
        return node(id, name, cores, memMb, HardwareClass.LINUX, Set.of(), status);
    }

    private NodeRecord node(
            String id,
            String name,
            int cores,
            long memMb,
            HardwareClass hardwareClass,
            Set<String> capabilities,
            ResourceStatus status) {

        NodeInfo info = new NodeInfo(id, name, hardwareClass, "x86_64", cores, memMb, "linux", capabilities);

        return NodeRecord.newlyRegistered(info, status);
    }

    @Test
    void picksTheLowerCostOfTwoOtherwiseEqualNodes() {
        NodeRecord idle = node(
                "n1",
                "Idle-Linux",
                8,
                16_384,
                ResourceStatus.of(5.0, 16_000, 0, 0, 5.0, 2.0));

        NodeRecord busy = node(
                "n2",
                "Busy-Linux",
                8,
                16_384,
                ResourceStatus.of(90.0, 16_000, 6, 4, 5.0, 2.0));

        TaskDescriptor task = new TaskDescriptor("task-1", "test", 2, 512);
        Optional<NodeRecord> selected = scheduler.selectNode(task, List.of(idle, busy));

        assertTrue(selected.isPresent());
        assertEquals("n1", selected.get().nodeId());
    }

    @Test
    void filtersOutNodesThatDoNotMeetCpuRequirement() {
        NodeRecord underpowered = node(
                "n1",
                "Low-Core",
                1,
                16_000,
                ResourceStatus.of(10.0, 16_000, 0, 0, 5.0, 1.0));

        TaskDescriptor task = new TaskDescriptor("task-1", "test", 4, 512);
        Optional<NodeRecord> selected = scheduler.selectNode(task, List.of(underpowered));

        assertTrue(selected.isEmpty());
    }

    @Test
    void filtersOutNodesThatDoNotMeetMemoryRequirement() {
        NodeRecord tooSmall = node(
                "n1",
                "ESP32-ish",
                1,
                256,
                ResourceStatus.of(10.0, 200, 0, 0, 5.0, 1.0));

        TaskDescriptor task = new TaskDescriptor("task-1", "test", 1, 512);
        Optional<NodeRecord> selected = scheduler.selectNode(task, List.of(tooSmall));

        assertTrue(selected.isEmpty());
    }

    @Test
    void filtersOutNodesWithWrongHardwareClass() {
        NodeRecord android = node(
                "n1",
                "Phone",
                8,
                8_000,
                HardwareClass.ANDROID,
                Set.of(),
                ResourceStatus.of(10.0, 8_000, 0, 0, 5.0, 1.0));

        TaskRequirements req = new TaskRequirements(
                1,
                512,
                Optional.empty(),
                Optional.of(HardwareClass.LINUX),
                Set.of());

        TaskDescriptor task = new TaskDescriptor("task-1", "test", req);
        Optional<NodeRecord> selected = scheduler.selectNode(task, List.of(android));

        assertTrue(selected.isEmpty());
    }

    @Test
    void filtersOutNodesMissingARequiredCapability() {
        NodeRecord noGpu = node(
                "n1",
                "No-GPU",
                8,
                16_000,
                HardwareClass.LINUX,
                Set.of(),
                ResourceStatus.of(10.0, 16_000, 0, 0, 5.0, 1.0));

        TaskRequirements req = new TaskRequirements(
                1,
                512,
                Optional.empty(),
                Optional.empty(),
                Set.of("gpu"));

        TaskDescriptor task = new TaskDescriptor("task-1", "test", req);
        Optional<NodeRecord> selected = scheduler.selectNode(task, List.of(noGpu));

        assertTrue(selected.isEmpty());
    }

    @Test
    void filtersOutNodesWithExcessiveNetworkLatency() {
        NodeRecord laggy = node(
                "n1",
                "Far-Node",
                8,
                16_000,
                ResourceStatus.of(10.0, 16_000, 0, 0, 250.0, 1.0));

        TaskRequirements req = new TaskRequirements(
                1,
                512,
                Optional.of(50.0),
                Optional.empty(),
                Set.of());

        TaskDescriptor task = new TaskDescriptor("task-1", "test", req);
        Optional<NodeRecord> selected = scheduler.selectNode(task, List.of(laggy));

        assertTrue(selected.isEmpty());
    }

    @Test
    void selectsNodeWhenAllRequirementsAreSatisfied() {
        NodeRecord fit = node(
                "n1",
                "Good-Fit",
                8,
                16_000,
                HardwareClass.LINUX,
                Set.of("gpu"),
                ResourceStatus.of(10.0, 16_000, 0, 0, 10.0, 1.0));

        TaskRequirements req = new TaskRequirements(
                2,
                512,
                Optional.of(50.0),
                Optional.of(HardwareClass.LINUX),
                Set.of("gpu"));

        TaskDescriptor task = new TaskDescriptor("task-1", "test", req);
        Optional<NodeRecord> selected = scheduler.selectNode(task, List.of(fit));

        assertTrue(selected.isPresent());
        assertEquals("n1", selected.get().nodeId());
    }

    @Test
    void returnsEmptyWhenNoCandidatesGiven() {
        TaskDescriptor task = new TaskDescriptor("task-1", "test", 1, 128);

        assertTrue(scheduler.selectNode(task, List.of()).isEmpty());
    }

    @Test
    void aNodeIsSelectedEvenWhenEveryCostIsNaN() {
        PlacementCostModel undefined = new PlacementCostModel() {
            @Override
            public double cost(NodeRecord node, TaskDescriptor task) {
                return Double.NaN;
            }

            @Override
            public CostBreakdown breakdown(NodeRecord node, TaskDescriptor task) {
                return new CostBreakdown(node.nodeId(), 0, 0, 0, 0, Double.NaN);
            }
        };

        NodeRecord only = node(
                "n1",
                "Only",
                4,
                8_000,
                ResourceStatus.of(10.0, 8_000, 0, 0, 5.0, 1.0));

        Optional<NodeRecord> selected = new GreedyCostScheduler(undefined)
                .selectNode(new TaskDescriptor("task-1", "test", 1, 128), List.of(only));

        assertEquals("n1", selected.orElseThrow().nodeId());
    }

    @Test
    void aNodeWithARealCostBeatsOneWhoseCostIsNaN() {
        PlacementCostModel partlyUndefined = new PlacementCostModel() {
            @Override
            public double cost(NodeRecord node, TaskDescriptor task) {
                return breakdown(node, task).total();
            }

            @Override
            public CostBreakdown breakdown(NodeRecord node, TaskDescriptor task) {
                double total = node.nodeId().equals("nan") ? Double.NaN : 100.0;
                return new CostBreakdown(node.nodeId(), 0, 0, 0, 0, total);
            }
        };

        ResourceStatus status = ResourceStatus.of(10.0, 8_000, 0, 0, 5.0, 1.0);

        Optional<NodeRecord> selected = new GreedyCostScheduler(partlyUndefined)
                .selectNode(
                        new TaskDescriptor("task-1", "test", 1, 128),
                        List.of(node("nan", "NaN", 4, 8_000, status),
                                node("real", "Real", 4, 8_000, status)));

        assertEquals("real", selected.orElseThrow().nodeId());
    }

    @Test
    void aNodeWithAnInfiniteCostBeatsOneWhoseCostIsNaN() {
        PlacementCostModel model = new PlacementCostModel() {
            @Override
            public double cost(NodeRecord node, TaskDescriptor task) {
                return breakdown(node, task).total();
            }

            @Override
            public CostBreakdown breakdown(NodeRecord node, TaskDescriptor task) {
                double total = node.nodeId().equals("nan")
                        ? Double.NaN
                        : Double.POSITIVE_INFINITY;
                return new CostBreakdown(node.nodeId(), 0, 0, 0, 0, total);
            }
        };

        ResourceStatus status = ResourceStatus.of(10.0, 8_000, 0, 0, 5.0, 1.0);

        Optional<NodeRecord> selected = new GreedyCostScheduler(model)
                .selectNode(
                        new TaskDescriptor("task-1", "test", 1, 128),
                        List.of(node("nan", "NaN", 4, 8_000, status),
                                node("inf", "Inf", 4, 8_000, status)));

        assertEquals("inf", selected.orElseThrow().nodeId());
    }
}
