// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.phoenix.core.DeviceRegistry;
import com.phoenix.core.NodeHealthMonitor;
import com.phoenix.core.NodeTelemetryService;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.core.TaskTimeoutPolicy;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;

/**
 * Deterministic reproduction of a JDK behaviour that a real socket cannot
 * provoke reliably.
 *
 * <p>{@link HttpURLConnection#disconnect()} racing a thread that is still
 * inside {@code getResponseCode()} can make the JDK throw an unchecked
 * {@link NullPointerException} instead of an {@link IOException}. When Core
 * abandoned the attempt, that is the abandonment working, not a failure.
 * Observed in practice as an intermittent failure (about 1 in 150 runs under
 * CPU load) of the real-socket abandon test.
 */
class HttpPushNodeAgentClientJdkDisconnectRaceTest {

    private static final String NODE_ID = "node-1";

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void stop() {
        executor.shutdownNow();
    }

    /** Blocks in getResponseCode() until disconnect(), then throws. */
    private static final class RacyConnection extends HttpURLConnection {

        final CountDownLatch inResponse = new CountDownLatch(1);
        final CountDownLatch disconnected = new CountDownLatch(1);
        private final RuntimeException toThrow;

        RacyConnection(URL url, RuntimeException toThrow) {
            super(url);
            this.toThrow = toThrow;
        }

        @Override public void connect() { }
        @Override public boolean usingProxy() { return false; }
        @Override public OutputStream getOutputStream() {
            return new ByteArrayOutputStream();
        }

        @Override
        public int getResponseCode() throws IOException {
            inResponse.countDown();
            try {
                disconnected.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            throw toThrow;
        }

        @Override
        public void disconnect() {
            disconnected.countDown();
        }
    }

    private HttpPushNodeAgentClient client(
            TaskLifecycleService lifecycle,
            RacyConnection[] holder,
            RuntimeException toThrow) {

        DeviceRegistry registry = new DeviceRegistry();

        RegistrationServer registration = new RegistrationServer(
                new NodeTelemetryService(
                        registry, new NodeHealthMonitor(registry)),
                new TaskQueueService(lifecycle)) {

            @Override
            public Optional<String> callbackAddressForNode(String nodeId) {
                return Optional.of("127.0.0.1:1");
            }
        };

        return new HttpPushNodeAgentClient(
                registration,
                lifecycle,
                new TaskTimeoutPolicy(
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(15))) {

            @Override
            protected HttpURLConnection openConnection(String url)
                    throws IOException {
                holder[0] = new RacyConnection(URI.create(url).toURL(), toThrow);
                return holder[0];
            }
        };
    }

    private static NodeExecutionTarget target() {
        return new NodeExecutionTarget(NODE_ID, "Linux-Test", "linux");
    }

    private static TaskDescriptor task(String id) {
        return new TaskDescriptor(id, "Monte-Carlo-Pi", 1, 128);
    }

    @Test
    void anUncheckedJdkFailureCausedByAbandonmentIsNodeOffline()
            throws Exception {

        TaskLifecycleService lifecycle = new TaskLifecycleService();
        RacyConnection[] holder = new RacyConnection[1];
        HttpPushNodeAgentClient client = client(
                lifecycle, holder, new NullPointerException("jdk race"));

        Future<TaskResult> attempt = executor.submit(
                () -> client.execute(target(), task("push-001")));

        while (holder[0] == null
                || !holder[0].inResponse.await(3, TimeUnit.SECONDS)) {
            Thread.sleep(5);
        }

        lifecycle.abandonAttemptsOn(NODE_ID);

        TaskResult result = attempt.get(3, TimeUnit.SECONDS);

        assertEquals(TaskFailureReason.NODE_OFFLINE, result.failureReason());
        assertTrue(result.isRetryable());
        assertTrue(!result.implicatesNodeHealth());
    }

    @Test
    void anUncheckedFailureThatIsNotAbandonmentStillPropagates()
            throws Exception {

        TaskLifecycleService lifecycle = new TaskLifecycleService();
        RacyConnection[] holder = new RacyConnection[1];
        HttpPushNodeAgentClient client = client(
                lifecycle, holder, new IllegalStateException("real bug"));

        Future<TaskResult> attempt = executor.submit(
                () -> client.execute(target(), task("push-002")));

        while (holder[0] == null
                || !holder[0].inResponse.await(3, TimeUnit.SECONDS)) {
            Thread.sleep(5);
        }

        // Not abandoned: release the blocked call so it throws.
        holder[0].disconnect();

        Exception thrown = assertThrows(
                Exception.class, () -> attempt.get(3, TimeUnit.SECONDS));

        assertTrue(thrown.getCause() instanceof IllegalStateException,
                "genuine programming errors must not be swallowed");
    }
}
