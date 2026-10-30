// SPDX-License-Identifier: Apache-2.0

package com.phoenix.adapter.impl;

import com.google.inject.Inject;
import com.phoenix.adapter.IHardwareAdapter;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskResult;
import com.phoenix.net.NodeAgentClient;
import com.phoenix.net.PullTransport;

public class AndroidAdapter implements IHardwareAdapter {

    private final NodeAgentClient client;

    @Inject
    public AndroidAdapter(@PullTransport NodeAgentClient client) {
        this.client = client;
    }

    @Override
    public TaskResult executeTask(
            TaskDescriptor task,
            NodeExecutionTarget target) {

        System.out.printf(
                "[AndroidAdapter] Submitting %s to real node %s%n",
                task.id(),
                target.displayName()
        );

        return client.execute(
                target,
                task
        );
    }

    @Override
    public void reportResult(TaskResult result) {
        System.out.println(
                "[AndroidAdapter] Result reported: "
                        + result.status()
        );
    }
}