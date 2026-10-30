// SPDX-License-Identifier: Apache-2.0

package com.phoenix.adapter;

import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskResult;

/**
 * The plugin contract every hardware class implements to join the Phoenix
 * mesh. Registration and telemetry come from the network layer directly
 * off wire payloads (RegistrationServer) — an adapter is only consulted
 * once the Scheduler has already picked a node and a task needs to run.
 *
 * <p>Different adapters may use different NodeAgentClient implementations
 * (push vs pull) depending on whether their hardware class is directly
 * addressable. See HttpPushNodeAgentClient and HttpNodeAgentClient.
 */
public interface IHardwareAdapter {
    TaskResult executeTask(TaskDescriptor task, NodeExecutionTarget target);
    void reportResult(TaskResult result);
}