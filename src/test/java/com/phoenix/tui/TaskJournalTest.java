// SPDX-License-Identifier: Apache-2.0

package com.phoenix.tui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.phoenix.model.TaskDescriptor;

class TaskJournalTest {

    private static TaskDescriptor task(int n) {
        return new TaskDescriptor(String.format("task-%04d", n), "Monte-Carlo-Pi", 1, 128);
    }

    @Test
    void retentionIsBoundedAndKeepsTheNewestTasks() throws Exception {

        TaskJournal journal = new TaskJournal();
        int total = TaskJournal.MAX_ENTRIES + 40;

        for (int i = 1; i <= total; i++) {
            journal.submitted(task(i));
            if (i % 50 == 0) {
                Thread.sleep(2);   // distinct submission instants
            }
        }

        assertEquals(TaskJournal.MAX_ENTRIES, journal.size());
        // The first 50 submissions are strictly older than the rest, so the
        // 40 evicted entries all come from that first batch.
        assertTrue(journal.recent(TaskJournal.MAX_ENTRIES).stream()
                .noneMatch(state -> state.taskId().equals("task-0001")));
        assertTrue(journal.recent(TaskJournal.MAX_ENTRIES).stream()
                .anyMatch(state -> state.taskId().equals(String.format("task-%04d", total))));
    }

    @Test
    void belowTheBoundNothingIsEvicted() {

        TaskJournal journal = new TaskJournal();

        for (int i = 1; i <= 10; i++) {
            journal.submitted(task(i));
        }

        assertEquals(10, journal.size());
    }
}
