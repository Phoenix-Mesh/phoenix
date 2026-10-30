// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.phoenix.core.AttemptFencing;
import com.phoenix.model.TaskDescriptor;

/** What Core puts on the wire to describe a task and identify its attempt. */
class TaskWireTest {

    private static final TaskDescriptor TASK =
            new TaskDescriptor("task-001", "Monte-Carlo-Pi", 1, 128);

    @Test
    void aLegacyNodeNeverSeesAnAttemptField() {
        assertEquals(
                "",
                TaskWire.attemptFields(AttemptFencing.LEGACY, "abc", 3));
    }

    @Test
    void anEnforcedNodeGetsTheIdAsIdentityAndTheNumberAsInformation() {
        assertEquals(
                ",\"attemptId\":\"abc\",\"attemptNumber\":3",
                TaskWire.attemptFields(AttemptFencing.ENFORCED, "abc", 3));
    }

    @Test
    void theRetiredNumericFieldIsNeverEmitted() {
        assertFalse(
                TaskWire.attemptFields(AttemptFencing.ENFORCED, "abc", 3)
                        .contains("\"attempt\":"));
    }

    @Test
    void idsAreEscapedSoTheyCannotBreakTheJson() {
        String fields =
                TaskWire.attemptFields(AttemptFencing.ENFORCED, "a\"b\\c", 1);

        assertTrue(fields.contains("\"attemptId\":\"a\\\"b\\\\c\""));
    }

    @Test
    void theTaskBodyIsValidJsonWithTheDocumentedFields() {
        JsonObject body = JsonParser.parseString(
                TaskWire.taskBody(TASK, AttemptFencing.ENFORCED, "id-1", 2))
                .getAsJsonObject();

        assertEquals("task-001", body.get("taskId").getAsString());
        assertEquals("Monte-Carlo-Pi", body.get("type").getAsString());
        assertEquals(TaskWire.DEFAULT_SAMPLES, body.get("samples").getAsLong());
        assertEquals("id-1", body.get("attemptId").getAsString());
        assertEquals(2, body.get("attemptNumber").getAsInt());
    }

    @Test
    void aLegacyTaskBodyHasExactlyTheThreeBaseFields() {
        JsonObject body = JsonParser.parseString(
                TaskWire.taskBody(TASK, AttemptFencing.LEGACY, "id-1", 2))
                .getAsJsonObject();

        assertEquals(3, body.size());
    }
}
