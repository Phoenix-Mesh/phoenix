// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

import java.util.Set;

/**
 * Transport-neutral registration of a node with Core.
 *
 * <p>Each transport gateway decodes its own wire format into this record and
 * hands it to {@link com.phoenix.core.NodeTelemetryService#register}, so the
 * registry never depends on how the node reached Core.
 *
 * <p>{@code existingNodeId} is {@code null} or blank for a first enrollment.
 * When present the registry keeps that identity instead of minting a new one.
 */
public record NodeRegistrationRequest(
        String existingNodeId,
        String displayName,
        HardwareClass hardwareClass,
        String architecture,
        int cpuCores,
        long memoryMb,
        String adapterType,
        Set<String> capabilities,
        ResourceStatus initialStatus
) {

    public boolean isReconnect() {
        return existingNodeId != null && !existingNodeId.isBlank();
    }
}
