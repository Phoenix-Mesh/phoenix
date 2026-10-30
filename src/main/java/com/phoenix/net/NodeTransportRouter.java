// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import java.util.Map;
import java.util.Objects;

import com.phoenix.core.NodeAttachments;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskResult;

/**
 * Picks the transport for each execution: the one the target node is
 * currently attached through.
 *
 * <p>Adapters are written per hardware class and inject one
 * {@link NodeAgentClient}. The router is that client: a node that registered
 * through a brokered transport (MQTT) is reached through it whatever its
 * hardware class, and every other node keeps the adapter's direct transport
 * (HTTP pull for Android, HTTP push for Linux, none for ESP32). The
 * scheduler, the dispatcher and the adapters stay unaware of which transport
 * carried the task.
 */
public final class NodeTransportRouter implements NodeAgentClient {

    private final NodeAttachments attachments;
    private final Map<String, NodeAgentClient> byTransport;
    private final NodeAgentClient fallback;

    /**
     * @param byTransport clients keyed by the transport id their gateway
     *                    records in {@link NodeAttachments}
     * @param fallback    client for nodes attached through any other
     *                    transport (the adapter's direct transport)
     */
    public NodeTransportRouter(
            NodeAttachments attachments,
            Map<String, NodeAgentClient> byTransport,
            NodeAgentClient fallback) {

        this.attachments = Objects.requireNonNull(attachments, "attachments");
        this.byTransport = Map.copyOf(byTransport);
        this.fallback = Objects.requireNonNull(fallback, "fallback");
    }

    @Override
    public TaskResult execute(NodeExecutionTarget target, TaskDescriptor task) {
        return attachments.transportOf(target.nodeId())
                .map(byTransport::get)
                .orElse(fallback)
                .execute(target, task);
    }
}
