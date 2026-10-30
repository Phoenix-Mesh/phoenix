// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import java.util.Set;

import com.phoenix.model.HardwareClass;

/**
 * Wire representation of a node's initial registration request.
 *
 * <p>This class belongs to the network layer rather than the domain model:
 * it represents JSON received from an external node agent. The HTTP gateway
 * and the MQTT gateway accept the same fields and validate them the same way
 * ({@link #validate()}).
 */
public class DeviceAnnouncement {

    public String deviceId;
    public String displayName;
    public HardwareClass hardwareClass;
    public String architecture;
    public int cpuCores;
    public long memoryMb;
    public String adapterType;
    public Set<String> capabilities;
    /** Optional {@link ProtocolFeatures}; absent for legacy agents. */
    public Set<String> protocolFeatures;
    public Integer callbackPort; // null for pull-based agents (Android)

    /**
     * Checks the fields every transport requires.
     *
     * @throws IllegalArgumentException naming the first invalid field
     */
    public void validate() {

        if (deviceId == null || deviceId.isBlank()) {
            throw new IllegalArgumentException("deviceId is required");
        }

        if (displayName == null || displayName.isBlank()) {
            throw new IllegalArgumentException("displayName is required");
        }

        if (hardwareClass == null) {
            throw new IllegalArgumentException("hardwareClass is required");
        }

        if (adapterType == null || adapterType.isBlank()) {
            throw new IllegalArgumentException("adapterType is required");
        }

        if (callbackPort != null
                && (callbackPort < 1 || callbackPort > 65_535)) {
            throw new IllegalArgumentException(
                    "callbackPort must be between 1 and 65535");
        }

        if (cpuCores <= 0) {
            throw new IllegalArgumentException(
                    "cpuCores must be greater than zero");
        }

        if (memoryMb <= 0) {
            throw new IllegalArgumentException(
                    "memoryMb must be greater than zero");
        }
    }
}
