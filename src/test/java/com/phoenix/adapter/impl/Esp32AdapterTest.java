// SPDX-License-Identifier: Apache-2.0

package com.phoenix.adapter.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;

/** Core must never record a result that no device computed. */
class Esp32AdapterTest {

    @Test
    void neverFabricatesASuccessAndLetsTheTaskBeRePlaced() {

        TaskResult result = new Esp32Adapter().executeTask(
                new TaskDescriptor("task-001", "Monte-Carlo-Pi", 1, 128),
                new NodeExecutionTarget("n1", "esp32-node", "esp32"));

        assertFalse(result.isSuccess());
        assertEquals(TaskFailureReason.TRANSPORT_ERROR, result.failureReason());
        assertTrue(result.isRetryable());
    }
}
