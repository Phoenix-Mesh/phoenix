// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import com.phoenix.net.TaskQueueService;

/**
 * Architectural guard (Issue #35).
 *
 * <p>Every report into the lifecycle ledger must be made through an
 * {@link TaskLifecycleService.Attempt} handle, never by task id: a task id
 * does not say <em>which attempt</em> is reporting, so such a report could
 * corrupt the ledger of a retry. This test pins the public surface so the
 * taskId-based overloads cannot quietly return.
 */
class TaskLifecycleApiShapeTest {

    private static Set<String> publicMethods(Class<?> type) {
        Set<String> shapes = new TreeSet<>();

        for (Method method : type.getDeclaredMethods()) {
            if (Modifier.isPublic(method.getModifiers())
                    && !method.isSynthetic()) {
                shapes.add(
                        method.getName()
                                + Arrays.stream(method.getParameterTypes())
                                        .map(Class::getSimpleName)
                                        .toList()
                );
            }
        }

        return shapes;
    }

    @Test
    void lifecycleServiceOnlyAcceptsAttemptBoundReports() {

        assertEquals(
                new TreeSet<>(Set.of(
                        "accepted[String, String, AttemptFencing]",
                        "delivered[Attempt]",
                        "started[Attempt]",
                        "completed[Attempt]",
                        "failed[Attempt]",
                        "reported[Attempt, TaskResult]",
                        "abandonAttemptsOn[String]",
                        "snapshot[String]",
                        "currentAttempt[String]",
                        "forget[String]"
                )),
                publicMethods(TaskLifecycleService.class)
        );
    }

    @Test
    void taskQueueServiceOnlySubmitsNodeBoundAttempts() {

        Set<String> submits = new TreeSet<>();

        for (String shape : publicMethods(TaskQueueService.class)) {
            if (shape.startsWith("submit")) {
                submits.add(shape);
            }
        }

        assertEquals(
                Set.of("submit[String, String, TaskDescriptor, AttemptFencing]"),
                submits
        );
    }
}
