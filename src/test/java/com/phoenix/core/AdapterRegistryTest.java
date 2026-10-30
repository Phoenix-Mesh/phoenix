// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.phoenix.adapter.IHardwareAdapter;
import com.phoenix.adapter.impl.AndroidAdapter;
import com.phoenix.adapter.impl.Esp32Adapter;
import com.phoenix.adapter.impl.LinuxAdapter;

/**
 * Every hardware class wired in {@link PhoenixModule} resolves to its adapter
 * within one injector, and unknown types fail fast.
 */
class AdapterRegistryTest {

    private AdapterRegistry registry() {
        Injector injector = Guice.createInjector(new PhoenixModule());
        return injector.getInstance(AdapterRegistry.class);
    }

    @Test
    void resolvesLinuxAdapter() {
        IHardwareAdapter adapter = registry().resolve("linux");
        assertInstanceOf(LinuxAdapter.class, adapter);
    }

    @Test
    void resolvesAndroidAdapter() {
        IHardwareAdapter adapter = registry().resolve("android");
        assertInstanceOf(AndroidAdapter.class, adapter);
    }

    @Test
    void resolvesEsp32Adapter() {
        IHardwareAdapter adapter = registry().resolve("esp32");
        assertInstanceOf(Esp32Adapter.class, adapter);
    }

    @Test
    void rejectsUnknownAdapterTypeWithClearError() {
        AdapterRegistry registry = registry();
        assertThrows(IllegalArgumentException.class, () -> registry.resolve("raspberrypi"));
    }
}
