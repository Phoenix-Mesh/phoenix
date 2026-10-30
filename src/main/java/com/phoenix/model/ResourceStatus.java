// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

import java.time.Instant;

/**
 * Live resource snapshot reported by a node on every heartbeat.
 *
 * <p>Each field feeds one term of the placement cost
 * {@code C(n) = w1*T(n) + w2*E(n) + w3*L(n) + w4*R(n)}:
 *
 * <pre>
 * T(n) execution time  - derived from cpuCores and cpuUsagePercent
 * E(n) energy cost     - energyCostEstimate
 * L(n) load            - activeTasks and queueLength
 * R(n) network latency - networkLatencyMs
 * </pre>
 *
 * @param cpuUsagePercent    0-100, or a negative value when the platform cannot
 *                           measure it; never a fabricated figure
 * @param availableRamMb     memory the node currently reports as available
 * @param activeTasks        tasks the node is executing right now
 * @param queueLength        tasks waiting on the node itself
 * @param networkLatencyMs   round-trip time the node measured to Core
 * @param energyCostEstimate relative, per-hardware-class heuristic
 * @param observedAt         when Core received the snapshot
 */
public record ResourceStatus(
        double cpuUsagePercent,
        long availableRamMb,
        int activeTasks,
        int queueLength,
        double networkLatencyMs,
        double energyCostEstimate,
        Instant observedAt
) {

    /** CPU usage value meaning "the platform cannot measure this". */
    public static final double CPU_UNKNOWN = -1.0;

    /** Creates a snapshot observed now. */
    public static ResourceStatus of(
            double cpuUsagePercent, long availableRamMb, int activeTasks,
            int queueLength, double networkLatencyMs, double energyCostEstimate) {
        return new ResourceStatus(
                cpuUsagePercent, availableRamMb, activeTasks, queueLength,
                networkLatencyMs, energyCostEstimate, Instant.now());
    }
}
