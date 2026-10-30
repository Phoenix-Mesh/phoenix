// SPDX-License-Identifier: Apache-2.0

package com.phoenix.tui;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.graphics.TextGraphics;
import com.googlecode.lanterna.input.KeyStroke;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.Terminal;
import com.phoenix.core.DeviceRegistry;
import com.phoenix.dispatch.DispatchResult;
import com.phoenix.dispatch.TaskAdmission;
import com.phoenix.dispatch.TaskDispatcher;
import com.phoenix.model.NodeRecord;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.net.mqtt.MqttBrokerLink;

/**
 * Terminal dashboard for Phoenix Core: nodes, recent tasks and the event log.
 *
 * <p>While running it redirects {@code System.out} and {@code System.err}
 * into {@link PhoenixEventLog}, so Core's log lines appear in the events
 * panel. Keys: D dispatch, C clear, H help, G jump to latest, arrows and
 * PgUp/PgDn scroll, Q or Esc quit.
 */
@Singleton
public class PhoenixTui {

    /** Matches the health monitor's DEGRADED threshold. */
    private static final int HEARTBEAT_WINDOW_MS = 20_000;

    private static final long FRAME_INTERVAL_MS = 150;

    private final DeviceRegistry registry;
    private final TaskAdmission taskAdmission;
    private final TaskJournal taskJournal;
    private final PhoenixEventLog eventLog;
    private final MqttBrokerLink broker;

    private final AtomicInteger taskCounter = new AtomicInteger(1);
    private final AtomicBoolean running = new AtomicBoolean(true);

    private int eventScrollOffset = 0;

    @Inject
    public PhoenixTui(
            DeviceRegistry registry,
            TaskAdmission taskAdmission,
            TaskJournal taskJournal,
            PhoenixEventLog eventLog,
            MqttBrokerLink broker) {

        this.registry = registry;
        this.taskAdmission = taskAdmission;
        this.taskJournal = taskJournal;
        this.eventLog = eventLog;
        this.broker = broker;
    }

    /** Runs the dashboard on the calling thread until the operator quits. */
    public void run() throws Exception {
        DefaultTerminalFactory factory =
                new DefaultTerminalFactory().setForceTextTerminal(true);

        Terminal terminal;
        try {
            terminal = factory.createTerminal();
        } catch (IOException exception) {
            terminal = factory.setPreferTerminalEmulator(true).createTerminal();
        }

        Screen screen = new TerminalScreen(terminal);
        screen.startScreen();

        PrintStream oldOut = System.out;
        PrintStream oldErr = System.err;

        try {
            System.setOut(PhoenixOutputCapture.createPrintStream(eventLog));
            System.setErr(PhoenixOutputCapture.createPrintStream(eventLog));
            System.out.println("[TUI] Phoenix dashboard started");

            while (running.get()) {
                handleInput(screen);
                render(screen);
                Thread.sleep(FRAME_INTERVAL_MS);
            }
        } finally {
            System.setOut(oldOut);
            System.setErr(oldErr);
            screen.stopScreen();
        }
    }

    private void handleInput(Screen screen) throws IOException {
        KeyStroke key;

        while ((key = screen.pollInput()) != null) {
            switch (key.getKeyType()) {
                case Escape -> {
                    running.set(false);
                    return;
                }
                case ArrowUp -> eventScrollOffset++;
                case ArrowDown -> eventScrollOffset = Math.max(0, eventScrollOffset - 1);
                case PageUp -> eventScrollOffset += 10;
                case PageDown -> eventScrollOffset = Math.max(0, eventScrollOffset - 10);
                case Character -> handleCharacter(Character.toLowerCase(key.getCharacter()));
                default -> {
                    // Other keys are ignored.
                }
            }
        }
    }

    private void handleCharacter(char c) {
        switch (c) {
            case 'q' -> running.set(false);
            case 'd' -> submitTask();
            case 'c' -> eventLog.clear();
            case 'g' -> eventScrollOffset = 0;
            case 'h' -> eventLog.append(
                    "Commands: D=dispatch C=clear G=latest "
                            + "Up/Down=scroll PgUp/PgDn=page Q=quit");
            default -> {
                // Unbound key.
            }
        }
    }

    private void submitTask() {
        TaskDescriptor task = new TaskDescriptor(
                String.format("task-%03d", taskCounter.getAndIncrement()),
                "Monte-Carlo-Pi",
                2,
                512);

        taskAdmission.submitFresh(task).whenComplete((outcome, error) -> {
            if (error != null) {
                eventLog.append("[TUI] " + task.id() + " failed: " + error.getMessage());
                return;
            }

            if (outcome instanceof TaskDispatcher.Outcome.Completed completed) {
                DispatchResult result = completed.result();
                eventLog.append("[TUI] " + task.id()
                        + " -> " + result.result().status()
                        + " on " + result.nodeDisplayName()
                        + " | output=" + result.result().outputData());
            } else {
                eventLog.append("[TUI] " + task.id() + " -> WAITING for an eligible node");
            }
        });
    }

    private void render(Screen screen) throws IOException {
        screen.clear();

        TextGraphics g = screen.newTextGraphics();
        TerminalSize size = screen.getTerminalSize();
        int width = size.getColumns();
        int height = size.getRows();
        int nodesHeight = Math.max(8, height / 3);
        int panelHeight = height - nodesHeight - 6;

        drawHeader(g, width);
        drawNodes(g, width, nodesHeight);
        drawTasks(g, 1, nodesHeight + 1, width / 2 - 2, panelHeight);
        drawEvents(g, width / 2, nodesHeight + 1, width / 2 - 2, panelHeight);
        drawFooter(g, height);

        screen.refresh();
    }

    private void drawHeader(TextGraphics g, int width) {
        g.setBackgroundColor(TextColor.ANSI.BLACK);
        g.setForegroundColor(TextColor.ANSI.CYAN);
        g.putString(2, 0, "PHOENIX CORE");
        g.setForegroundColor(TextColor.ANSI.WHITE);
        g.putString(17, 0, "| Heterogeneous Edge Orchestrator");

        String addressText = "| Core: "
                + System.getProperty("phoenix.advertiseAddress", "127.0.0.1");
        int addressX = 52;

        if (addressX + addressText.length() < width - 23) {
            g.putString(addressX, 0, addressText);
        }

        // Broker link, beside the RUNNING badge so it fits any width.
        String mqtt = broker.statusLabel();
        if (!"off".equals(mqtt)) {
            boolean up = "connected".equals(mqtt);
            g.setForegroundColor(up ? TextColor.ANSI.GREEN : TextColor.ANSI.YELLOW);
            g.putString(Math.max(0, width - 22), 0, up ? "MQTT ●" : "MQTT ○");
        }

        g.setForegroundColor(TextColor.ANSI.GREEN);
        g.putString(Math.max(0, width - 14), 0, "● RUNNING");
    }

    private void drawNodes(TextGraphics g, int width, int height) {
        box(g, 0, 1, width - 1, height);
        g.setForegroundColor(TextColor.ANSI.CYAN);
        g.putString(2, 2, "NODES");

        int y = 4;
        for (NodeRecord node : registry.list()) {
            if (y >= height - 1) {
                break;
            }
            drawNode(g, node, y, width);
            y += 2;
        }
    }

    private void drawNode(TextGraphics g, NodeRecord node, int y, int width) {
        long age = System.currentTimeMillis() - node.lastHeartbeat().toEpochMilli();
        double freshness = Math.max(0.0, Math.min(1.0, 1.0 - (double) age / HEARTBEAT_WINDOW_MS));

        TextColor color = switch (node.state()) {
            case ONLINE -> TextColor.ANSI.GREEN;
            case DEGRADED -> TextColor.ANSI.YELLOW;
            case OFFLINE -> TextColor.ANSI.RED;
        };

        g.setForegroundColor(color);
        g.putString(2, y, truncate(node.info().displayName(), 20));

        g.setForegroundColor(TextColor.ANSI.WHITE);
        g.putString(23, y, String.format("%-8s", node.info().hardwareClass()));
        g.putString(32, y, node.status().cpuUsagePercent() < 0
                ? "CPU   N/A"
                : String.format("CPU %5.1f%%", node.status().cpuUsagePercent()));
        g.putString(46, y, String.format("RAM %6dMB", node.status().availableRamMb()));
        g.putString(60, y, String.format("RTT %5.1fms", node.status().networkLatencyMs()));

        int barStart = 73;
        int barWidth = Math.max(10, width - barStart - 14);
        drawBar(g, barStart, y, barWidth, freshness, color);

        String heartbeatText = switch (node.state()) {
            case ONLINE -> String.format("%4.1fs", age / 1000.0);
            case DEGRADED, OFFLINE -> node.state().name();
        };

        g.setForegroundColor(color);
        g.putString(barStart + barWidth + 2, y, heartbeatText);
    }

    private void drawTasks(TextGraphics g, int x, int y, int width, int height) {
        box(g, x, y, width, height);
        g.setForegroundColor(TextColor.ANSI.CYAN);
        g.putString(x + 2, y + 1, "TASKS");

        int row = y + 3;
        for (TaskJournal.TaskState task : taskJournal.recent(8)) {
            if (row >= y + height - 1) {
                break;
            }

            TextColor statusColor = switch (task.status()) {
                case "SUCCESS" -> TextColor.ANSI.GREEN;
                case "FAILED" -> TextColor.ANSI.RED;
                case "WAITING", "RETRYING" -> TextColor.ANSI.YELLOW;
                case "RUNNING" -> TextColor.ANSI.CYAN;
                default -> TextColor.ANSI.WHITE;
            };

            String displayStatus = "RETRYING".equals(task.status())
                    ? "RETRY " + task.attempt()
                    : task.status();

            g.setForegroundColor(statusColor);
            g.putString(x + 2, row, String.format("%-9s %-10s", task.taskId(), displayStatus));
            g.setForegroundColor(TextColor.ANSI.WHITE);
            g.putString(x + 24, row, truncate(task.nodeName() == null ? "-" : task.nodeName(), 18));
            g.putString(x + 44, row, String.format("%5dms", task.elapsedMs()));
            row++;
        }
    }

    private void drawEvents(TextGraphics g, int x, int y, int width, int height) {
        box(g, x, y, width, height);
        g.setForegroundColor(TextColor.ANSI.CYAN);
        g.putString(x + 2, y + 1, "EVENTS");

        int visibleRows = Math.max(1, height - 4);
        int innerWidth = Math.max(12, width - 4);

        List<String> wrapped = new ArrayList<>();
        for (String event : eventLog.snapshot()) {
            wrapped.addAll(PhoenixEventLog.wrap(event, innerWidth));
        }

        eventScrollOffset = Math.min(eventScrollOffset, Math.max(0, wrapped.size() - visibleRows));
        int offset = eventScrollOffset;
        int end = wrapped.size() - offset;
        int start = Math.max(0, end - visibleRows);

        int row = y + 3;
        for (String line : wrapped.subList(start, end)) {
            if (row >= y + height - 1) {
                break;
            }
            g.setForegroundColor(colorFor(line));
            g.putString(x + 2, row, line);
            row++;
        }

        String indicator = offset > 0 ? "↑" + offset + " [G]=live" : "[G]=live";
        g.setForegroundColor(TextColor.ANSI.CYAN);
        g.putString(Math.max(x + 2, x + width - indicator.length() - 2), y + 1, indicator);
    }

    private static TextColor colorFor(String line) {
        if (line.contains("FAILED") || line.contains("WAITING")) {
            return TextColor.ANSI.RED;
        }
        if (line.contains("DEGRADED") || line.contains("OFFLINE")
                || line.contains("Recovery") || line.contains("RETRY")) {
            return TextColor.ANSI.YELLOW;
        }
        if (line.contains("SUCCESS")) {
            return TextColor.ANSI.GREEN;
        }
        return TextColor.ANSI.WHITE;
    }

    private static void drawFooter(TextGraphics g, int height) {
        int y = height - 2;
        g.setForegroundColor(TextColor.ANSI.CYAN);
        g.putString(2, y, "[D] Dispatch");
        g.setForegroundColor(TextColor.ANSI.WHITE);
        g.putString(18, y, "[C] Clear");
        g.putString(29, y, "[H] Help");
        g.putString(40, y, "[↑↓/PgUp/PgDn] Scroll");
        g.putString(64, y, "[G] Live");
        g.putString(74, y, "[Q] Quit");
    }

    private static void drawBar(
            TextGraphics g, int x, int y, int width, double fraction, TextColor color) {

        int filled = (int) Math.round(width * fraction);
        g.setForegroundColor(color);
        for (int i = 0; i < width; i++) {
            g.putString(x + i, y, i < filled ? "█" : "░");
        }
    }

    private static void box(TextGraphics g, int x, int y, int width, int height) {
        g.setForegroundColor(TextColor.ANSI.WHITE);
        g.putString(x, y, "┌");
        g.putString(x + width, y, "┐");
        g.putString(x, y + height, "└");
        g.putString(x + width, y + height, "┘");

        for (int i = 1; i < width; i++) {
            g.putString(x + i, y, "─");
            g.putString(x + i, y + height, "─");
        }
        for (int i = 1; i < height; i++) {
            g.putString(x, y + i, "│");
            g.putString(x + width, y + i, "│");
        }
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return "";
        }
        if (value.length() <= max) {
            return value;
        }
        return value.substring(0, Math.max(0, max - 1)) + "…";
    }
}
