// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phoenix.core.TaskLifecycleService;
import com.phoenix.core.TaskTimeoutPolicy;
import com.phoenix.model.NodeExecutionTarget;
import com.phoenix.model.TaskDescriptor;
import com.phoenix.model.TaskFailureReason;
import com.phoenix.model.TaskResult;

/**
 * Push transport for directly addressable nodes such as Linux machines on
 * the LAN.
 *
 * <p>Core posts the task to the node's {@code /execute} endpoint and the node
 * answers with the result in the same HTTP response. There is no Core-side
 * queue, so the moment the request body has been written is the delivery
 * boundary:
 *
 * <pre>
 * accepted -&gt; connect + write -&gt; delivered -&gt; response
 *                  |                   |              |
 *          NODE_UNREACHABLE    EXECUTION_TIMEOUT   COMPLETED / FAILED
 * </pre>
 *
 * <p>The connect timeout is a transport constant; the execution timeout comes
 * from {@link TaskTimeoutPolicy} so every transport shares it. If Core
 * abandons the attempt because the node went OFFLINE, the connection is
 * closed and the call returns {@link TaskFailureReason#NODE_OFFLINE}.
 */
@Singleton
public class HttpPushNodeAgentClient
        implements NodeAgentClient {

    /** How long Core waits to establish the TCP connection to the node. */
    private static final int CONNECT_TIMEOUT_MS = 5_000;

    private final RegistrationServer registrationServer;
    private final TaskLifecycleService lifecycle;
    private final TaskTimeoutPolicy timeouts;
    private final Gson gson = new Gson();

    @Inject
    public HttpPushNodeAgentClient(
            RegistrationServer registrationServer,
            TaskLifecycleService lifecycle,
            TaskTimeoutPolicy timeouts) {

        this.registrationServer = registrationServer;
        this.lifecycle = lifecycle;
        this.timeouts = timeouts;
    }

    @Override
    public TaskResult execute(
            NodeExecutionTarget target,
            TaskDescriptor task) {

        Optional<String> callbackAddress =
                registrationServer.callbackAddressForNode(target.nodeId());

        if (callbackAddress.isEmpty()) {
            return new TaskResult(
                    task.id(),
                    "FAILED",
                    "Node " + target.displayName()
                            + " has no registered callback address",
                    TaskFailureReason.NODE_UNREACHABLE
            );
        }

        String url = "http://" + callbackAddress.get() + "/execute";

        System.out.printf(
                "[HttpPushNodeAgentClient] Pushing %s directly to %s (%s)%n",
                task.id(),
                target.displayName(),
                callbackAddress.get()
        );

        TaskLifecycleService.Attempt attempt =
                lifecycle.accepted(
                        task.id(),
                        target.nodeId(),
                        registrationServer.fencingFor(target.nodeId()));

        byte[] requestBody =
                TaskWire.taskBody(
                        task,
                        attempt.fencing(),
                        attempt.id(),
                        attempt.number()
                ).getBytes(StandardCharsets.UTF_8);

        // Separates failures before the node received the task from failures after.
        boolean delivered = false;

        try {
            HttpURLConnection conn = openConnection(url);

            // Closing the socket is the only way to wake a thread blocked on the node.
            attempt.onAbandon(conn::disconnect);

            conn.setRequestMethod("POST");
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(Math.toIntExact(timeouts.executionTimeout().toMillis()));
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            if (attempt.isAbandoned()) {
                return nodeOffline(target, task);
            }

            try (OutputStream os = conn.getOutputStream()) {
                os.write(requestBody);
            }

            delivered = true;
            lifecycle.delivered(attempt);

            int code = conn.getResponseCode();
            String response = readBody(code == 200 ? conn.getInputStream() : conn.getErrorStream());

            // The node received the request but did not complete it.
            if (code != 200) {
                lifecycle.failed(attempt);

                return new TaskResult(
                        task.id(),
                        "FAILED",
                        "Node returned HTTP " + code + ": " + response,
                        code >= 500
                                ? TaskFailureReason.TRANSPORT_ERROR
                                : TaskFailureReason.EXECUTION_ERROR
                );
            }

            ResultBody body = parse(response);

            if (body == null || body.status == null) {
                lifecycle.failed(attempt);

                return new TaskResult(
                        task.id(),
                        "FAILED",
                        "Node returned an invalid result body",
                        TaskFailureReason.TRANSPORT_ERROR
                );
            }

            TaskResult reported =
                    new TaskResult(
                            task.id(),
                            body.status,
                            body.outputData,
                            "SUCCESS".equalsIgnoreCase(body.status)
                                    ? TaskFailureReason.NONE
                                    : TaskFailureReason.EXECUTION_ERROR
                    );

            lifecycle.reported(attempt, reported);
            return reported;

        } catch (SocketTimeoutException exception) {

            if (attempt.isAbandoned()) {
                return nodeOffline(target, task);
            }

            lifecycle.failed(attempt);

            // The same exception type covers both the connect and the read phase.
            if (delivered) {
                return new TaskResult(
                        task.id(),
                        "FAILED",
                        "No result from "
                                + target.displayName()
                                + " within "
                                + timeouts.executionTimeout().toSeconds()
                                + "s of delivery",
                        TaskFailureReason.EXECUTION_TIMEOUT
                );
            }

            return new TaskResult(
                    task.id(),
                    "FAILED",
                    "Push to "
                            + target.displayName()
                            + " failed before delivery: "
                            + exception.getMessage(),
                    TaskFailureReason.NODE_UNREACHABLE
            );

        } catch (IOException exception) {

            // A disconnect issued by the abandon hook surfaces as "socket closed".
            if (attempt.isAbandoned()) {
                return nodeOffline(target, task);
            }

            lifecycle.failed(attempt);

            /*
             * Before delivery the node could not be reached. After delivery
             * the node may have executed the task and only the response was
             * lost, which does not make it unreachable.
             */
            return new TaskResult(
                    task.id(),
                    "FAILED",
                    "Push to "
                            + target.displayName()
                            + " failed: "
                            + exception.getMessage(),
                    delivered
                            ? TaskFailureReason.TRANSPORT_ERROR
                            : TaskFailureReason.NODE_UNREACHABLE
            );

        } catch (RuntimeException exception) {

            /*
             * HttpURLConnection.disconnect() racing a thread inside
             * getResponseCode() can surface as an unchecked exception from
             * the JDK. When Core abandoned the attempt that is the requested
             * cancellation; anything else is a programming error.
             */
            if (attempt.isAbandoned()) {
                return nodeOffline(target, task);
            }

            throw exception;
        }
    }

    /**
     * Opens the connection to the node. Overridable so tests can simulate
     * JDK-level behaviour that cannot be provoked deterministically.
     */
    protected HttpURLConnection openConnection(String url)
            throws IOException {

        return (HttpURLConnection) URI.create(url).toURL().openConnection();
    }

    private static String readBody(InputStream stream) throws IOException {
        if (stream == null) {
            return "";
        }

        try (InputStream in = stream) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** The node's result, or {@code null} if the body is not valid JSON. */
    private ResultBody parse(String response) {
        try {
            return gson.fromJson(response, ResultBody.class);
        } catch (JsonParseException exception) {
            return null;
        }
    }

    /** Result for an attempt Core abandoned because its node went OFFLINE. */
    private static TaskResult nodeOffline(
            NodeExecutionTarget target,
            TaskDescriptor task) {

        return new TaskResult(
                task.id(),
                "FAILED",
                "Node "
                        + target.displayName()
                        + " went OFFLINE during push execution; "
                        + "attempt abandoned",
                TaskFailureReason.NODE_OFFLINE
        );
    }

    private static class ResultBody {
        String status;
        String outputData;
    }
}
