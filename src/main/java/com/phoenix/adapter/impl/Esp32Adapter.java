// SPDX-License-Identifier: Apache-2.0

package com.phoenix.adapter.impl;

import com.google.inject.Inject;
import com.phoenix.adapter.IHardwareAdapter;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;
import com.phoenix.net.BrokerTransport;
import com.phoenix.net.NodeAgentClient;

/**
 * Adapter for ESP32 nodes.
 *
 * <p>ESP32 devices have no direct transport: they cannot be polled over the
 * HTTP pull protocol in a practical way and are not addressable for push.
 * They run tasks only when they registered over MQTT ({@link BrokerTransport}).
 * An ESP32 node that reached Core any other way fails every attempt with
 * {@link TaskFailureReason#TRANSPORT_ERROR}, so the task is re-placed and Core
 * never records a result that no device computed.
 */
public class Esp32Adapter implements IHardwareAdapter {

    private final NodeAgentClient client;

    /** An adapter with no transport at all: every attempt fails. */
    public Esp32Adapter() {
        this(Esp32Adapter::noTransport);
    }

    @Inject
    public Esp32Adapter(@BrokerTransport NodeAgentClient client) {
        this.client = client;
    }

    @Override
    public TaskResult executeTask(
            TaskDescriptor task,
            NodeExecutionTarget target) {

        return client.execute(target, task);
    }

    @Override
    public void reportResult(TaskResult result) {
        System.out.println(
                "[Esp32Adapter] Result reported: "
                        + result.status()
        );
    }

    /** Fallback for ESP32 nodes that are not attached through the broker. */
    public static TaskResult noTransport(
            NodeExecutionTarget target,
            TaskDescriptor task) {

        return new TaskResult(
                task.id(),
                "FAILED",
                "No execution transport for ESP32 node "
                        + target.displayName(),
                TaskFailureReason.TRANSPORT_ERROR
        );
    }
}
