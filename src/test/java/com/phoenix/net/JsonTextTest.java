// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonParser;

class JsonTextTest {

    private static String roundTrip(String value) {
        return JsonParser.parseString("\"" + JsonText.escape(value) + "\"")
                .getAsString();
    }

    @Test
    void nullBecomesEmpty() {
        assertEquals("", JsonText.escape(null));
    }

    @Test
    void quotesAndBackslashesSurviveARoundTrip() {
        assertEquals("a\"b\\c", roundTrip("a\"b\\c"));
    }

    @Test
    void newlinesAndControlCharactersSurviveARoundTrip() {
        String value = "line one\nline two\r\ttab \u0001 bell \u001f";

        assertEquals(value, roundTrip(value));
    }

    @Test
    void ordinaryTextIsUnchanged() {
        assertEquals("task-001 ✓", JsonText.escape("task-001 ✓"));
    }
}
