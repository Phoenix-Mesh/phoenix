// SPDX-License-Identifier: Apache-2.0

package com.phoenix.tui;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import com.google.inject.Singleton;

@Singleton
public class PhoenixEventLog {

    private static final int MAX_LINES = 1000;

    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss");

    private final Deque<String> lines = new ArrayDeque<>();

    public synchronized void append(String line) {
        if (line == null || line.isBlank()) {
            return;
        }

        String normalized = line.strip();

        // Heartbeat telemetry already has a dedicated visual representation
        // in the Nodes panel. Suppress routine heartbeat spam here.
        if (normalized.contains("Heartbeat from")) {
            return;
        }

        lines.addLast(
                "[" + LocalTime.now().format(TIME) + "] " + normalized
        );

        while (lines.size() > MAX_LINES) {
            lines.removeFirst();
        }
    }

    public synchronized List<String> snapshot() {
        return List.copyOf(
                new ArrayList<>(lines)
        );
    }

    public synchronized List<String> recent(int limit) {
        List<String> snapshot =
                new ArrayList<>(lines);

        int from = Math.max(
                0,
                snapshot.size() - limit
        );

        return List.copyOf(
                snapshot.subList(
                        from,
                        snapshot.size()
                )
        );
    }

    public synchronized void clear() {
        lines.clear();
    }

    /**
     * Hard-wrap a stored event for a TUI panel while keeping continuation
     * lines indented beneath the original event.
     */
    public static List<String> wrap(
            String line,
            int width) {

        List<String> out = new ArrayList<>();

        if (width < 12) {
            out.add(line);
            return out;
        }

        String remaining = line;
        String indent = "";

        while (remaining.length() > width) {
            int cut = remaining.lastIndexOf(
                    ' ',
                    width
            );

            if (cut <= indent.length()) {
                cut = width;
            }

            out.add(
                    remaining.substring(0, cut)
            );

            remaining =
                    indent
                            + remaining.substring(cut).stripLeading();

            indent = "           ";
        }

        out.add(remaining);
        return out;
    }
}
