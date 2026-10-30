// SPDX-License-Identifier: Apache-2.0

package com.phoenix;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.phoenix.core.InFlightAttemptReconciler;
import com.phoenix.core.NodeHealthMonitor;
import com.phoenix.core.PhoenixModule;
import com.phoenix.dispatch.TaskAdmission;
import com.phoenix.net.RegistrationServer;
import com.phoenix.net.mqtt.MqttNodeGateway;
import com.phoenix.tui.PhoenixTui;

/**
 * Phoenix Core entry point: wires the module, starts the node-agent gateway,
 * health monitoring and task admission, then runs the operator console.
 *
 * <p>The HTTP gateway listens on {@code phoenix.port} (default 8080). The
 * MQTT gateway connects to the broker in {@code phoenix.mqtt.url}, if set.
 */
public class App {

    private static final int DEFAULT_PORT = 8080;

    public static void main(String[] args)
            throws Exception {

        Injector injector =
                Guice.createInjector(
                        new PhoenixModule()
                );

        RegistrationServer server =
                injector.getInstance(
                        RegistrationServer.class
                );

        TaskAdmission taskAdmission =
                injector.getInstance(
                        TaskAdmission.class
                );

        NodeHealthMonitor nodeHealthMonitor =
                injector.getInstance(
                        NodeHealthMonitor.class
                );

        InFlightAttemptReconciler inFlightReconciler =
                injector.getInstance(
                        InFlightAttemptReconciler.class
                );

        // Resolved first so an invalid phoenix.mqtt.* setting stops Core
        // before anything starts.
        MqttNodeGateway mqttGateway =
                injector.getInstance(
                        MqttNodeGateway.class
                );

        // Subscribe before any node can be declared OFFLINE.
        inFlightReconciler.start();

        server.start(
                Integer.getInteger("phoenix.port", DEFAULT_PORT)
        );

        mqttGateway.start();

        nodeHealthMonitor.start();

        taskAdmission.start();

        System.out.println(
                "Phoenix Core initialized."
        );

        try {

            PhoenixTui tui =
                    injector.getInstance(
                            PhoenixTui.class
                    );

            tui.run();

        } finally {

            taskAdmission.stop();
            nodeHealthMonitor.stop();
            mqttGateway.stop();
            server.stop();
        }
    }
}
