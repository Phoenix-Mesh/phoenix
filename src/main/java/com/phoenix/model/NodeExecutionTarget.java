// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

/**
 * The exact physical node the Dispatcher selected, carried into
 * {@link com.phoenix.adapter.IHardwareAdapter#executeTask} so the adapter
 * (and its printed output) can prove which node instance actually ran a
 * task — not just which hardware class/adapter type was resolved.
 *
 * <p>Deliberately small and separate from {@link NodeRecord}: the adapter
 * boundary should see only "who to execute on", not the Registry's
 * internal view (live resource status, lifecycle state, heartbeat). Later,
 * this is also where remote-transport metadata (MQTT topic, REST endpoint)
 * will live without changing the Scheduler or Registry at all.
 */
public record NodeExecutionTarget(
        String nodeId,
        String displayName,
        String adapterType
) {}
