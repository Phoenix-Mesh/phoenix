// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskResult;

/**
 * Transport-independent Core -> node execution boundary.
 *
 * <h2>Task lifecycle contract (Issue #28)</h2>
 *
 * <p>Every implementation (HTTP pull, HTTP push, and the planned MQTT
 * transport) honours the same contract, so Core's timeout and recovery
 * semantics never depend on the wire protocol.
 *
 * <p><b>1. Report the lifecycle you can observe.</b> Report transitions to
 * {@link com.phoenix.core.TaskLifecycleService}:
 * {@code accepted} (transport took the task), {@code delivered} (it reached
 * the node), {@code started} (node acknowledged execution, optional). Every
 * report is made through the {@code Attempt} handle returned by
 * {@code accepted}. When the node returns a result, report it with
 * {@code reported(attempt, result)}; use {@code failed(attempt)} only when
 * the attempt ended without a node result. A transport reports only what it
 * can truly observe and never invents transitions.
 *
 * <p><b>2. Return a classified {@link TaskResult}.</b> When the attempt does
 * not produce a node result, the failure reason must say which phase failed:
 *
 * <ul>
 *     <li>never delivered, node still in contact ->
 *         {@code DELIVERY_TIMEOUT} (backlog; does not implicate the node)</li>
 *     <li>never delivered, node not in contact ->
 *         {@code NODE_UNREACHABLE}</li>
 *     <li>delivered, no result in time -> {@code EXECUTION_TIMEOUT}</li>
 *     <li>transport broke -> {@code TRANSPORT_ERROR}</li>
 *     <li>Core abandoned the attempt because the node went OFFLINE ->
 *         {@code NODE_OFFLINE}</li>
 * </ul>
 *
 * <p><b>3. Use the shared policy.</b> Timeout values and the
 * backlog-versus-unreachable decision come from
 * {@link com.phoenix.core.TaskTimeoutPolicy}, not from transport constants.
 *
 * <p><b>3b. Honour abandonment.</b> Bind the attempt to the node
 * ({@code accepted(taskId, nodeId)}) and register a cancellation hook with
 * {@link com.phoenix.core.TaskLifecycleService.Attempt#onAbandon(Runnable)}
 * that releases whatever the transport is blocked on. When Core declares the
 * node OFFLINE the hook runs; the transport then returns promptly with
 * {@code NODE_OFFLINE} rather than waiting for a timeout. A late message from
 * the node for an abandoned attempt must be ignored.
 *
 * <p><b>4. Degrade gracefully.</b> A transport that cannot distinguish the
 * phases returns {@code TIMEOUT} (unclassified). Core treats that
 * conservatively, as a failure of the node, so detection of unresponsive
 * nodes is never weakened.
 */
public interface NodeAgentClient {

    TaskResult execute(
            NodeExecutionTarget target,
            TaskDescriptor task
    );
}
