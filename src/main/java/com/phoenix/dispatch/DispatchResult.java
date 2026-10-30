// SPDX-License-Identifier: Apache-2.0

package com.phoenix.dispatch;

import com.phoenix.model.TaskResult;

/**
 * A {@link TaskResult} together with the node context that produced it.
 * Kept separate from TaskResult itself so {@link com.phoenix.adapter.IHardwareAdapter}
 * stays a pure capability contract that never needs to know which node
 * instance it's being invoked for. See TaskResult's javadoc for the full
 * reasoning.
 */
public record DispatchResult(
        String nodeId,
        String nodeDisplayName,
        TaskResult result
) {}
