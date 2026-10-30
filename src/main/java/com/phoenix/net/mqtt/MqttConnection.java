// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.time.Duration;

/**
 * Core's connection to an MQTT broker, reduced to what the transport needs.
 *
 * <p>This is the seam between the Phoenix MQTT transport and an MQTT client
 * library. {@link PahoMqttConnection} implements it with Eclipse Paho; tests
 * use an in-memory broker. Nothing outside {@code com.phoenix.net.mqtt}
 * depends on it.
 *
 * <p>Contract:
 * <ul>
 *     <li>{@link #subscribe} and {@link #lastWill} are called before
 *         {@link #open}; subscriptions are re-established after every
 *         reconnect.</li>
 *     <li>{@link #open} returns at once and keeps (re)connecting in the
 *         background until {@link #close}.</li>
 *     <li>Inbound messages are delivered one at a time, in arrival order,
 *         on a thread the implementation owns. Handlers must be quick and
 *         must not block on broker round trips.</li>
 * </ul>
 */
public interface MqttConnection extends AutoCloseable {

    /** Receives one inbound message. */
    @FunctionalInterface
    interface MessageHandler {
        void onMessage(String topic, byte[] payload);
    }

    /** Observes the broker link. */
    interface Listener {

        /**
         * The link is up. Called after subscriptions were re-established.
         *
         * @param reconnect false for the first connection
         */
        void connected(boolean reconnect);

        /** The link went down; the connection keeps retrying. */
        void disconnected(String cause);
    }

    /** Message the broker publishes for Core if Core disappears without closing. */
    void lastWill(String topic, byte[] payload, int qos, boolean retained);

    /** Subscribes {@code handler} to every topic matching {@code topicFilter}. */
    void subscribe(String topicFilter, int qos, MessageHandler handler);

    void addListener(Listener listener);

    /** Starts connecting; returns immediately. */
    void open();

    /**
     * Hands a message to the client for delivery.
     *
     * <p>QoS 1 messages published while the link is briefly down are
     * buffered by the client and sent when it reconnects.
     *
     * @return false if the client refused the message (closed, or its
     *         buffer is full)
     */
    boolean publish(String topic, byte[] payload, int qos, boolean retained);

    boolean isConnected();

    /**
     * Waits up to {@code timeout} for the link to be up.
     *
     * @return true if it is up
     */
    boolean awaitConnected(Duration timeout) throws InterruptedException;

    /** Disconnects cleanly (the last will is not published) and stops reconnecting. */
    @Override
    void close();
}
