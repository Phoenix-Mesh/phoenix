// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

/** One device, one node, whichever transport it arrives through. */
class NodeAttachmentsTest {

    private final NodeAttachments attachments = new NodeAttachments();

    @Test
    void aNewDeviceIsRegisteredOnceAndRemembered() {

        NodeAttachments.Attachment first = attachments.attach("dev-1", "http", () -> "node-1");
        NodeAttachments.Attachment again = attachments.attach("dev-1", "http", () -> "node-2");

        assertTrue(first.created());
        assertFalse(again.created());
        assertEquals("node-1", again.nodeId());
        assertEquals(Optional.of("node-1"), attachments.nodeIdFor("dev-1"));
        assertEquals(Optional.of("dev-1"), attachments.deviceIdFor("node-1"));
    }

    @Test
    void theLatestTransportWins() {

        attachments.attach("dev-1", "http", () -> "node-1");
        attachments.attach("dev-1", "mqtt", () -> "unused");

        assertEquals(Optional.of("mqtt"), attachments.transportOf("node-1"));

        attachments.attach("dev-1", "http", () -> "unused");
        assertEquals(Optional.of("http"), attachments.transportOf("node-1"));
    }

    @Test
    void anAttachmentReportsTheTransportItReplaced() {

        assertTrue(attachments.attach("dev-1", "http", () -> "node-1").movedFrom().isEmpty());
        assertTrue(attachments.attach("dev-1", "http", () -> "unused").movedFrom().isEmpty());

        assertEquals(Optional.of("http"),
                attachments.attach("dev-1", "mqtt", () -> "unused").movedFrom());
        assertEquals(Optional.of("mqtt"),
                attachments.attach("dev-1", "http", () -> "unused").movedFrom());
    }

    @Test
    void detachingForgetsTheDeviceSoItRegistersAfresh() {

        attachments.attach("dev-1", "mqtt", () -> "node-1");
        attachments.detach("dev-1");

        assertTrue(attachments.nodeIdFor("dev-1").isEmpty());
        assertTrue(attachments.deviceIdFor("node-1").isEmpty());
        assertTrue(attachments.transportOf("node-1").isEmpty());
        assertTrue(attachments.attach("dev-1", "mqtt", () -> "node-2").created());
    }

    @Test
    void twoTransportsRegisteringTheSameDeviceAtOnceCreateOneNode() throws Exception {

        AtomicInteger registrations = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(8);

        try {
            List<Callable<String>> calls = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                String transport = i % 2 == 0 ? "http" : "mqtt";
                calls.add(() -> attachments.attach("dev-1", transport,
                        () -> "node-" + registrations.incrementAndGet()).nodeId());
            }

            Set<String> nodeIds = executor.invokeAll(calls).stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (Exception exception) {
                            throw new IllegalStateException(exception);
                        }
                    })
                    .collect(Collectors.toSet());

            assertEquals(1, registrations.get());
            assertEquals(Set.of("node-1"), nodeIds);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentRegistrationsLeaveOneNodeWithOneOfTheCallersTransports() throws Exception {

        for (int round = 0; round < 500; round++) {

            NodeAttachments table = new NodeAttachments();
            AtomicInteger minted = new AtomicInteger();
            CountDownLatch go = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(2);

            try {
                List<Callable<String>> calls = List.of(
                        () -> {
                            go.await();
                            return table.attach("dev-1", "http",
                                    () -> "node-" + minted.incrementAndGet()).nodeId();
                        },
                        () -> {
                            go.await();
                            return table.attach("dev-1", "mqtt",
                                    () -> "node-" + minted.incrementAndGet()).nodeId();
                        });

                List<Future<String>> futures = new ArrayList<>();
                for (Callable<String> call : calls) {
                    futures.add(executor.submit(call));
                }
                go.countDown();

                String first = futures.get(0).get(10, TimeUnit.SECONDS);
                String second = futures.get(1).get(10, TimeUnit.SECONDS);

                assertEquals(first, second);
                assertEquals(1, minted.get());
                assertEquals(Optional.of("dev-1"), table.deviceIdFor(first));
                assertTrue(Set.of("http", "mqtt").contains(
                        table.transportOf(first).orElseThrow()));
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void aNodeIsNeverVisibleBeforeItsTransportIs() throws Exception {

        int devices = 20_000;
        AtomicInteger attaching = new AtomicInteger(-1);
        AtomicReference<String> violation = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        Thread reader = new Thread(() -> {
            while (done.getCount() > 0 && violation.get() == null) {
                int index = attaching.get();
                if (index < 0) {
                    continue;
                }
                String deviceId = "dev-" + index;
                Optional<String> node = attachments.nodeIdFor(deviceId);
                if (node.isPresent()) {
                    if (attachments.transportOf(node.get()).isEmpty()) {
                        violation.set(deviceId + " resolves to " + node.get()
                                + " but the node has no transport yet");
                    } else if (attachments.deviceIdFor(node.get()).isEmpty()) {
                        violation.set(deviceId + " resolves to " + node.get()
                                + " but the node has no device yet");
                    }
                }
            }
        }, "attachments-reader");

        reader.start();

        try {
            for (int i = 0; i < devices && violation.get() == null; i++) {
                attaching.set(i);
                attachments.attach("dev-" + i, "mqtt", () -> "node-" + System.nanoTime());
            }
        } finally {
            done.countDown();
            reader.join(10_000);
        }

        assertEquals(null, violation.get());
    }

    @Test
    void attachingAndDetachingTheSameDeviceNeverLeavesOrphanedEntries() throws Exception {

        int rounds = 3_000_000;
        AtomicInteger minted = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            var attacher = executor.submit(() -> {
                for (int i = 0; i < rounds; i++) {
                    attachments.attach("dev-1", i % 2 == 0 ? "http" : "mqtt",
                            () -> "node-" + minted.incrementAndGet());
                }
                return null;
            });

            var detacher = executor.submit(() -> {
                for (int i = 0; i < rounds; i++) {
                    attachments.detach("dev-1");
                }
                return null;
            });

            attacher.get(60, TimeUnit.SECONDS);
            detacher.get(60, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        // Every node id ever minted must be either fully attached or fully gone.
        Optional<String> current = attachments.nodeIdFor("dev-1");

        for (int i = 1; i <= minted.get(); i++) {
            String nodeId = "node-" + i;
            boolean isCurrent = current.equals(Optional.of(nodeId));

            assertEquals(isCurrent, attachments.deviceIdFor(nodeId).isPresent(),
                    nodeId + " still maps to a device although it is not attached");
            assertEquals(isCurrent, attachments.transportOf(nodeId).isPresent(),
                    nodeId + " still has a transport although it is not attached");
        }
    }

    @Test
    void aFailedRegistrationLeavesNothingBehindAndCanBeRetried() {

        try {
            attachments.attach("dev-1", "http", () -> {
                throw new IllegalStateException("registry unavailable");
            });
        } catch (IllegalStateException expected) {
            // the failure reaches the caller
        }

        assertTrue(attachments.nodeIdFor("dev-1").isEmpty());
        assertTrue(attachments.attach("dev-1", "http", () -> "node-1").created());
    }

    @Test
    void aNodeIdCannotBelongToTwoDevices() {

        attachments.attach("dev-1", "http", () -> "node-1");

        try {
            attachments.attach("dev-2", "http", () -> "node-1");
            throw new AssertionError("expected the clash to be rejected");
        } catch (IllegalStateException expected) {
            // fine
        }

        assertEquals(Optional.of("dev-1"), attachments.deviceIdFor("node-1"));
        assertTrue(attachments.nodeIdFor("dev-2").isEmpty());
    }

    @Test
    void unknownOrNullLookupsAreEmpty() {
        assertTrue(attachments.nodeIdFor(null).isEmpty());
        assertTrue(attachments.deviceIdFor(null).isEmpty());
        assertTrue(attachments.transportOf("nobody").isEmpty());
        attachments.detach(null);
    }
}
