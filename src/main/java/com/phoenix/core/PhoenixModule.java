// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import com.phoenix.adapter.IHardwareAdapter;
import com.phoenix.adapter.impl.AndroidAdapter;
import com.phoenix.adapter.impl.Esp32Adapter;
import com.phoenix.adapter.impl.LinuxAdapter;
import com.phoenix.dispatch.PendingTaskQueue;
import com.phoenix.dispatch.TaskAdmission;
import com.phoenix.net.BrokerTransport;
import com.phoenix.net.HttpNodeAgentClient;
import com.phoenix.net.HttpPushNodeAgentClient;
import com.phoenix.net.NodeAgentClient;
import com.phoenix.net.NodeTransportRouter;
import com.phoenix.net.PullTransport;
import com.phoenix.net.PushTransport;
import com.phoenix.net.RegistrationServer;
import com.phoenix.net.TaskQueueService;
import com.phoenix.net.mqtt.MqttAttempts;
import com.phoenix.net.mqtt.MqttBrokerLink;
import com.phoenix.net.mqtt.MqttConfig;
import com.phoenix.net.mqtt.MqttNodeAgentClient;
import com.phoenix.net.mqtt.MqttNodeGateway;
import com.phoenix.net.mqtt.MqttSessions;
import com.phoenix.recovery.TaskRecoveryManager;
import com.phoenix.scheduler.GreedyCostScheduler;
import com.phoenix.scheduler.PlacementCostModel;
import com.phoenix.scheduler.TaskScheduler;
import com.phoenix.scheduler.WeightedPlacementCostModel;
import com.phoenix.tui.PhoenixEventLog;
import com.phoenix.tui.PhoenixTui;
import com.phoenix.tui.TaskJournal;

/**
 * Guice wiring for Phoenix Core.
 *
 * <p>Hardware adapters are contributed to a {@code Map<String, IHardwareAdapter>}
 * keyed by adapter type, which {@link AdapterRegistry} consults per task, so
 * nodes of every hardware class can be served by one Core instance.
 *
 * <p>Every stateful service is a singleton: transports, the dispatcher and the
 * health monitor must all observe the same registry, lifecycle ledger and
 * reservation state.
 *
 * <p>Transports: each adapter injects one {@link NodeAgentClient}, and every
 * one of those is a {@link NodeTransportRouter}. A node attached over MQTT is
 * reached over MQTT whatever its hardware class; any other node uses the
 * adapter's direct transport (HTTP pull, HTTP push, or none for ESP32).
 */
public class PhoenixModule extends AbstractModule {

    @Override
    protected void configure() {

        MapBinder<String, IHardwareAdapter> adapters =
                MapBinder.newMapBinder(binder(), String.class, IHardwareAdapter.class);

        adapters.addBinding("linux").to(LinuxAdapter.class);
        adapters.addBinding("android").to(AndroidAdapter.class);
        adapters.addBinding("esp32").to(Esp32Adapter.class);

        bind(AdapterRegistry.class).in(Singleton.class);

        // Placement policy. Replacing the greedy baseline is a one-line change.
        bind(TaskScheduler.class).to(GreedyCostScheduler.class).in(Singleton.class);
        bind(PlacementCostModel.class).to(WeightedPlacementCostModel.class).in(Singleton.class);

        // Node registry, telemetry and liveness.
        bind(DeviceRegistry.class).in(Singleton.class);
        bind(NodeTelemetryService.class).in(Singleton.class);
        bind(NodeHealthMonitor.class).in(Singleton.class);

        // Attempt lifecycle, timeouts and reconciliation with node liveness.
        bind(TaskLifecycleService.class).in(Singleton.class);
        bind(TaskTimeoutPolicy.class).in(Singleton.class);
        bind(InFlightAttemptReconciler.class).in(Singleton.class);

        // Admission, placement and recovery.
        bind(PlacementReservationService.class).in(Singleton.class);
        bind(TaskRecoveryManager.class).in(Singleton.class);
        bind(PendingTaskQueue.class).in(Singleton.class);
        bind(TaskAdmission.class).in(Singleton.class);

        // Which device is which node, and through which transport.
        bind(NodeAttachments.class).in(Singleton.class);

        // HTTP transports.
        bind(RegistrationServer.class).in(Singleton.class);
        bind(TaskQueueService.class).in(Singleton.class);
        bind(HttpNodeAgentClient.class).in(Singleton.class);
        bind(HttpPushNodeAgentClient.class).in(Singleton.class);

        // MQTT/TLS transport (active only when phoenix.mqtt.url is set).
        bind(MqttBrokerLink.class).in(Singleton.class);
        bind(MqttSessions.class).in(Singleton.class);
        bind(MqttAttempts.class).in(Singleton.class);
        bind(MqttNodeGateway.class).in(Singleton.class);
        bind(MqttNodeAgentClient.class).in(Singleton.class);

        // Operator console.
        bind(PhoenixEventLog.class).in(Singleton.class);
        bind(TaskJournal.class).in(Singleton.class);
        bind(PhoenixTui.class).in(Singleton.class);
    }

    /** Read once; invalid values stop Core at startup. */
    @Provides
    @Singleton
    MqttConfig mqttConfig() {
        return MqttConfig.fromSystemProperties();
    }

    @Provides
    @Singleton
    @PullTransport
    NodeAgentClient pullTransport(
            NodeAttachments attachments,
            MqttNodeAgentClient mqtt,
            HttpNodeAgentClient http) {

        return new NodeTransportRouter(
                attachments, Map.of(MqttNodeGateway.TRANSPORT_ID, mqtt), http);
    }

    @Provides
    @Singleton
    @PushTransport
    NodeAgentClient pushTransport(
            NodeAttachments attachments,
            MqttNodeAgentClient mqtt,
            HttpPushNodeAgentClient http) {

        return new NodeTransportRouter(
                attachments, Map.of(MqttNodeGateway.TRANSPORT_ID, mqtt), http);
    }

    @Provides
    @Singleton
    @BrokerTransport
    NodeAgentClient brokerTransport(
            NodeAttachments attachments,
            MqttNodeAgentClient mqtt) {

        return new NodeTransportRouter(
                attachments,
                Map.of(MqttNodeGateway.TRANSPORT_ID, mqtt),
                Esp32Adapter::noTransport);
    }
}
