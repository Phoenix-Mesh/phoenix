// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.adapter.IHardwareAdapter;

/**
 * Resolves a node's {@code adapterType} to the {@link IHardwareAdapter} that
 * executes on that hardware class.
 *
 * <p>Wraps the Guice-built adapter map (see {@link PhoenixModule}) with a
 * fail-fast lookup, so callers never handle a {@code null} adapter.
 */
@Singleton
public class AdapterRegistry {

    private final Map<String, IHardwareAdapter> adaptersByType;

    @Inject
    public AdapterRegistry(Map<String, IHardwareAdapter> adaptersByType) {
        this.adaptersByType = adaptersByType;
    }

    /**
     * @throws IllegalArgumentException if {@code adapterType} is null or no
     *         adapter is registered for it
     */
    public IHardwareAdapter resolve(String adapterType) {
        IHardwareAdapter adapter = adapterType == null
                ? null
                : adaptersByType.get(adapterType.toLowerCase(Locale.ROOT));
        if (adapter == null) {
            throw new IllegalArgumentException(
                    "No adapter registered for type '" + adapterType + "'. Available: " + availableTypes());
        }
        return adapter;
    }

    public Set<String> availableTypes() {
        return adaptersByType.keySet();
    }
}
