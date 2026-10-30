// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import com.phoenix.core.AttemptFencing;
import com.phoenix.model.TaskDescriptor;

/**
 * The task body Core sends to a node, shared by the pull and push transports.
 *
 * <pre>
 * {"taskId":"task-001","type":"Monte-Carlo-Pi","samples":2000000}
 * </pre>
 *
 * <p>Attempt identity is added only for attempts that are
 * {@link AttemptFencing#ENFORCED}, i.e. for nodes that announced the
 * {@link ProtocolFeatures#ATTEMPT_ID} feature at registration:
 *
 * <ul>
 *     <li>{@code "attemptId"} - opaque, globally unique identity of the
 *         attempt. The node echoes it verbatim in {@code /tasks/started} and
 *         {@code /results}; a report with a different id, or none, is
 *         rejected as stale.</li>
 *     <li>{@code "attemptNumber"} - 1, 2, 3 ... for the task. Informational
 *         only; Core never matches a report by it.</li>
 * </ul>
 *
 * <p>A {@link AttemptFencing#LEGACY} node never sees either field.
 *
 * <p>Public because every transport, including the MQTT one in
 * {@code com.phoenix.net.mqtt}, must describe a task identically.
 */
public final class TaskWire {

    /** Monte-Carlo sample count until task payloads become part of the model. */
    public static final long DEFAULT_SAMPLES = 2_000_000L;

    private TaskWire() {
    }

    /**
     * The complete JSON body for one attempt of {@code task}.
     *
     * @param task          the task
     * @param fencing       whether the attempt identity is included
     * @param attemptId     opaque id of the attempt
     * @param attemptNumber 1, 2, 3 ... for the task
     * @return the JSON body
     */
    public static String taskBody(
            TaskDescriptor task,
            AttemptFencing fencing,
            String attemptId,
            int attemptNumber) {

        return "{\"taskId\":\"" + JsonText.escape(task.id())
                + "\",\"type\":\"" + JsonText.escape(task.type())
                + "\",\"samples\":" + DEFAULT_SAMPLES
                + attemptFields(fencing, attemptId, attemptNumber)
                + "}";
    }

    /**
     * {@code ,"attemptId":"...","attemptNumber":N} for an enforced attempt,
     * otherwise the empty string.
     */
    static String attemptFields(
            AttemptFencing fencing,
            String attemptId,
            int attemptNumber) {

        return fencing == AttemptFencing.ENFORCED
                ? ",\"attemptId\":\"" + JsonText.escape(attemptId) + "\""
                        + ",\"attemptNumber\":" + attemptNumber
                : "";
    }
}
