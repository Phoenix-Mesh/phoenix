// SPDX-License-Identifier: Apache-2.0

package com.phoenix.model;

/**
 * Physical hardware family of a node.
 *
 * <p>Distinct from {@link NodeInfo#adapterType()}, the plugin key that
 * selects an {@link com.phoenix.adapter.IHardwareAdapter}. Two nodes of the
 * same class may differ in architecture yet share an adapter, and the two
 * need not stay one-to-one, so they are kept as separate fields.
 */
public enum HardwareClass {
    LINUX,
    ANDROID,
    ESP32
}
