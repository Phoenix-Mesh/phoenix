// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Reconciles in-flight task attempts with node liveness.
 *
 * <p>Phoenix distinguishes <em>suspicion</em> from <em>decision</em>:
 *
 * <ul>
 *     <li>DEGRADED means heartbeats are late. The node may still be running
 *         the task, so nothing in flight is touched.</li>
 *     <li>OFFLINE means {@link NodeHealthMonitor} has declared the node gone
 *         under the configured liveness policy. That is the point at which
 *         Core stops waiting for the node.</li>
 * </ul>
 *
 * <p>On OFFLINE this class asks {@link TaskLifecycleService} to abandon every
 * non-terminal attempt bound to the node. The abandonment wakes each blocked
 * transport thread, which returns a retryable
 * {@code NODE_OFFLINE} failure; the unchanged {@code TaskDispatcher} then
 * releases the placement reservation and re-places the task through the same
 * admission path as every other recovery. This class therefore contains no
 * scheduling, no retry policy and no transport knowledge.
 *
 * <p>OFFLINE does not prove the computation stopped. The abandoned attempt is
 * marked {@code ABANDONED} (outcome unknown) and any late message from the
 * node for it is rejected, so a recovered task can never be completed by its
 * own predecessor.
 */
@Singleton
public class InFlightAttemptReconciler
        implements NodeOfflineListener {

    private final TaskLifecycleService lifecycle;
    private final NodeHealthMonitor healthMonitor;

    private boolean started;

    @Inject
    public InFlightAttemptReconciler(
            TaskLifecycleService lifecycle,
            NodeHealthMonitor healthMonitor) {

        this.lifecycle = lifecycle;
        this.healthMonitor = healthMonitor;
    }

    /** Subscribes to OFFLINE transitions. Safe to call repeatedly. */
    public synchronized void start() {
        if (started) {
            return;
        }

        healthMonitor.addOfflineListener(this);
        started = true;
    }

    @Override
    public void onNodeOffline(
            String nodeId,
            String displayName) {

        List<TaskLifecycleService.Snapshot> abandoned =
                lifecycle.abandonAttemptsOn(nodeId);

        if (abandoned.isEmpty()) {
            return;
        }

        StringBuilder summary = new StringBuilder();

        for (TaskLifecycleService.Snapshot snapshot : abandoned) {

            if (summary.length() > 0) {
                summary.append(", ");
            }

            summary.append(snapshot.taskId())
                    .append(" (attempt ")
                    .append(snapshot.attempt())
                    .append(", ")
                    .append(
                            snapshot.startAcknowledged()
                                    ? "executing"
                                    : snapshot.reachedNode()
                                            ? "delivered"
                                            : "queued"
                    )
                    .append(')');
        }

        System.out.printf(
                "[Reconciler] Node %s is OFFLINE: abandoned %d in-flight "
                        + "attempt(s): %s%n",
                displayName,
                abandoned.size(),
                summary
        );
    }
}
