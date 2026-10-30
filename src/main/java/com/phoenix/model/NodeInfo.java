// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

import java.util.Set;

/**
 * Identity and static capability of a node, captured at registration.
 *
 * <p>Kept apart from {@link ResourceStatus}, which changes on every
 * heartbeat; the two are combined in {@link NodeRecord}.
 *
 * @param nodeId        stable identity assigned by Core at first enrollment
 * @param displayName   human-facing label; never used for lookups
 * @param hardwareClass physical family of the device
 * @param architecture  free-form CPU ISA such as {@code x86_64} or {@code arm64-v8a}
 * @param cpuCores      declared core count, used for reservations
 * @param memoryMb      declared total memory, used for reservations
 * @param adapterType   key used by {@link com.phoenix.core.AdapterRegistry}
 *                      to resolve the {@link com.phoenix.adapter.IHardwareAdapter}
 * @param capabilities  scheduling tags matched against
 *                      {@link TaskRequirements#requiredCapabilities()}
 */
public record NodeInfo(
        String nodeId,
        String displayName,
        HardwareClass hardwareClass,
        String architecture,
        int cpuCores,
        long memoryMb,
        String adapterType,
        Set<String> capabilities
) {}
