// SPDX-License-Identifier: Apache-2.0

package com.phoenix.adapter.impl;

import com.google.inject.Inject;
import com.phoenix.adapter.IHardwareAdapter;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskResult;
import com.phoenix.net.NodeAgentClient;
import com.phoenix.net.PushTransport;

/**
 * Linux nodes are directly addressable on the LAN, so Phoenix pushes tasks
 * to them (@PushTransport) rather than making them poll like Android does.
 * The adapter itself is transport-agnostic — it just calls execute();
 * PhoenixModule decides which transport implementation this is.
 */
public class LinuxAdapter implements IHardwareAdapter {

    private final NodeAgentClient client;

    @Inject
    public LinuxAdapter(@PushTransport NodeAgentClient client) {
        this.client = client;
    }

    @Override
    public TaskResult executeTask(TaskDescriptor task, NodeExecutionTarget target) {
        System.out.printf("[LinuxAdapter] Pushing %s to real node %s%n", task.id(), target.displayName());
        return client.execute(target, task);
    }

    @Override
    public void reportResult(TaskResult result) {
        System.out.println("[LinuxAdapter] Result reported: " + result.status());
    }
}