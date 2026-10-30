// SPDX-License-Identifier: Apache-2.0

package com.phoenix.tui;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Converts line-oriented stdout/stderr into structured TUI events.
 */
public final class PhoenixOutputCapture extends OutputStream {

    private final PhoenixEventLog eventLog;
    private final StringBuilder currentLine = new StringBuilder();

    public PhoenixOutputCapture(PhoenixEventLog eventLog) {
        this.eventLog = eventLog;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public synchronized void write(
            byte[] bytes,
            int offset,
            int length) {

        String text = new String(
                bytes,
                offset,
                length,
                StandardCharsets.UTF_8
        );

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);

            if (c == '\n') {
                flushLine();
            } else if (c != '\r') {
                currentLine.append(c);
            }
        }
    }

    @Override
    public synchronized void flush() {
        // Deliberately do nothing.
        // PrintStream may call flush() in the middle of printf/formatting.
        // Only a real newline should create a TUI event.
    }

    private void flushLine() {
        String line = currentLine.toString().strip();

        if (!line.isEmpty()) {
            eventLog.append(line);
        }

        currentLine.setLength(0);
    }

    public static PrintStream createPrintStream(
            PhoenixEventLog eventLog) {

        return new PrintStream(
                new PhoenixOutputCapture(eventLog),
                false,
                StandardCharsets.UTF_8
        );
    }
}
