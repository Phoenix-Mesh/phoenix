// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

/**
 * Observer of the one lifecycle transition that is definitive in Phoenix:
 * a node being declared OFFLINE by {@link NodeHealthMonitor}.
 *
 * <p>DEGRADED is deliberately <em>not</em> observable here. A degraded node
 * is only suspected of being unhealthy; it may still be executing work, so
 * nothing in flight may be cancelled on that evidence.
 *
 * <p>Implementations must be fast and must not throw: they run on the health
 * monitor's thread. They are notified exactly once per transition into
 * OFFLINE.
 */
@FunctionalInterface
public interface NodeOfflineListener {

    void onNodeOffline(
            String nodeId,
            String displayName
    );
}
