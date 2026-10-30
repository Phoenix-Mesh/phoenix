// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

/**
 * Escaping for JSON string values that Core writes by hand.
 *
 * <p>Covers quotes, backslashes and every control character, so text taken
 * from exceptions or from node-supplied ids can never break a response body.
 */
public final class JsonText {

    private JsonText() {
    }

    /**
     * Escapes {@code value} for use inside a JSON string; {@code null} becomes empty.
     *
     * @param value text to escape
     * @return the escaped text, without surrounding quotes
     */
    public static String escape(String value) {
        if (value == null) {
            return "";
        }

        StringBuilder out = new StringBuilder(value.length() + 8);

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }

        return out.toString();
    }
}
