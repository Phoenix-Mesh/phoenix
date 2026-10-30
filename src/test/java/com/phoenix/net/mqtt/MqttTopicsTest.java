// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.Test;

/** #18: the topic namespace is the contract nodes and the broker ACL rely on. */
class MqttTopicsTest {

    private final MqttTopics topics = new MqttTopics("phoenix");

    @Test
    void topicsFollowTheDocumentedLayout() {
        assertEquals("phoenix/v1/core/status", topics.coreStatus());
        assertEquals("phoenix/v1/nodes/+/up/#", topics.allUplinks());
        assertEquals("phoenix/v1/nodes/android-7f/up/heartbeat",
                topics.up("android-7f", MqttTopics.Up.HEARTBEAT));
        assertEquals("phoenix/v1/nodes/android-7f/down/task",
                topics.down("android-7f", MqttTopics.Down.TASK));
    }

    @Test
    void everyUplinkKindRoundTrips() {
        for (MqttTopics.Up kind : MqttTopics.Up.values()) {
            assertEquals(
                    Optional.of(new MqttTopics.Inbound("linux-01", kind)),
                    topics.parseUplink(topics.up("linux-01", kind)));
        }
    }

    @Test
    void anythingElseIsNotAnUplink() {
        assertTrue(topics.parseUplink("phoenix/v1/nodes/linux-01/down/task").isEmpty());
        assertTrue(topics.parseUplink("phoenix/v1/nodes/linux-01/up/teleport").isEmpty());
        assertTrue(topics.parseUplink("phoenix/v1/nodes/linux-01/up/result/x").isEmpty());
        assertTrue(topics.parseUplink("phoenix/v1/nodes/a/b/up/result").isEmpty());
        assertTrue(topics.parseUplink("phoenix/v2/nodes/linux-01/up/result").isEmpty());
        assertTrue(topics.parseUplink("other/v1/nodes/linux-01/up/result").isEmpty());
        assertTrue(topics.parseUplink(null).isEmpty());
    }

    @Test
    void deviceIdsThatCouldEscapeTheirTopicAreRejected() {
        assertTrue(MqttTopics.isValidDeviceId("android-7fbacc0c-bb51-4230-8790-79be08e3a18b"));
        assertTrue(MqttTopics.isValidDeviceId("esp32_A1.b"));

        for (String bad : new String[] {
                "", "a/b", "+", "#", "dev+1", "dev#", "-leading", ".hidden",
                "with space", "tab\t", "x".repeat(129), "$SYS"}) {
            assertFalse(MqttTopics.isValidDeviceId(bad), bad);
        }

        assertThrows(IllegalArgumentException.class,
                () -> topics.down("a/b", MqttTopics.Down.TASK));
    }

    @Test
    void aCustomPrefixMayHaveSeveralLevels() {
        MqttTopics lab = new MqttTopics("lab/phoenix");

        assertEquals("lab/phoenix/v1/nodes/+/up/#", lab.allUplinks());
        assertTrue(lab.parseUplink("lab/phoenix/v1/nodes/n1/up/ack").isPresent());
        assertThrows(IllegalArgumentException.class, () -> new MqttTopics("lab//x"));
    }
}
