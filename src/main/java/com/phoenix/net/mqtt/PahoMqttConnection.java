// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * {@link MqttConnection} backed by Eclipse Paho (MQTT 3.1.1).
 *
 * <ul>
 *     <li><b>Session</b>: persistent ({@code cleanSession=false}) under a
 *         stable client id, so results a node publishes while Core is
 *         briefly disconnected are queued by the broker, not lost.</li>
 *     <li><b>Reconnect</b>: the first connection is retried with
 *         exponential backoff (1 s to 30 s) until it succeeds; after that
 *         Paho's automatic reconnect takes over. Core keeps running, and the
 *         HTTP transport keeps working, while the broker is away.</li>
 *     <li><b>Offline buffer</b>: QoS 1 messages published while the link is
 *         down are buffered (up to {@value #BUFFER_SIZE}) and sent on
 *         reconnect.</li>
 *     <li><b>TLS</b>: for {@code ssl://} URLs the socket factory comes from
 *         {@link MqttTls}; the broker certificate must match the broker's
 *         host name unless {@code phoenix.mqtt.tls.verifyHostname=false}.</li>
 *     <li><b>Inbound</b>: messages are handed from Paho's thread to one
 *         Core thread, in order, so a slow handler never stalls the client
 *         and a throwing handler never drops the connection.</li>
 * </ul>
 */
public final class PahoMqttConnection implements MqttConnection {

    static final int BUFFER_SIZE = 1_000;

    private static final long FIRST_RETRY_MS = 1_000;
    private static final long MAX_RETRY_MS = 30_000;

    private final MqttConfig config;
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();
    private final Object state = new Object();

    private MqttAsyncClient client;
    private ExecutorService inbound;
    private Thread connector;
    private Will will;
    private volatile boolean closed;
    private boolean connected;

    private record Subscription(String filter, int qos, MessageHandler handler) {}

    private record Will(String topic, byte[] payload, int qos, boolean retained) {}

    public PahoMqttConnection(MqttConfig config) {
        if (!config.enabled()) {
            throw new IllegalArgumentException("MQTT is not configured");
        }
        this.config = config;
    }

    @Override
    public void lastWill(String topic, byte[] payload, int qos, boolean retained) {
        will = new Will(topic, payload.clone(), qos, retained);
    }

    @Override
    public void subscribe(String topicFilter, int qos, MessageHandler handler) {
        subscriptions.add(new Subscription(topicFilter, qos, handler));
    }

    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public synchronized void open() {

        if (client != null) {
            return;
        }

        MqttConnectOptions options = options();

        try {
            client = new MqttAsyncClient(
                    config.serverUri(), config.clientId(), new MemoryPersistence());
        } catch (MqttException exception) {
            throw new IllegalArgumentException(
                    "MQTT client could not be created for " + config.serverUri()
                            + ": " + exception.getMessage(), exception);
        }

        DisconnectedBufferOptions buffer = new DisconnectedBufferOptions();
        buffer.setBufferEnabled(true);
        buffer.setBufferSize(BUFFER_SIZE);
        buffer.setPersistBuffer(false);
        buffer.setDeleteOldestMessages(false);
        client.setBufferOpts(buffer);

        inbound = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "phoenix-mqtt-inbound");
            thread.setDaemon(true);
            return thread;
        });

        client.setCallback(new Callback());

        connector = new Thread(() -> connectUntilUp(options), "phoenix-mqtt-connect");
        connector.setDaemon(true);
        connector.start();
    }

    private MqttConnectOptions options() {

        MqttConnectOptions options = new MqttConnectOptions();
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        options.setCleanSession(false);
        options.setAutomaticReconnect(true);
        options.setMaxReconnectDelay((int) MAX_RETRY_MS);
        options.setKeepAliveInterval(Math.toIntExact(config.keepAlive().toSeconds()));
        options.setConnectionTimeout(Math.toIntExact(config.connectTimeout().toSeconds()));
        options.setMaxInflight(BUFFER_SIZE);

        config.username().ifPresent(options::setUserName);
        config.password().ifPresent(password -> options.setPassword(password.clone()));

        if (config.usesTls()) {
            options.setSocketFactory(MqttTls.socketFactory(config));
            options.setHttpsHostnameVerificationEnabled(config.verifyHostname());
        }

        if (will != null) {
            options.setWill(will.topic(), will.payload(), will.qos(), will.retained());
        }

        return options;
    }

    private void connectUntilUp(MqttConnectOptions options) {

        long delay = FIRST_RETRY_MS;

        while (!closed && !client.isConnected()) {
            try {
                client.connect(options).waitForCompletion(
                        config.connectTimeout().toMillis() + 5_000);
                return; // Paho reconnects on its own from now on.

            } catch (MqttException exception) {

                // A slow attempt may have completed after the wait gave up.
                if (closed || client.isConnected()) {
                    return;
                }

                System.out.printf(
                        "[MQTT] Broker %s not reachable (%s); retrying in %ds%n",
                        config.serverUri(),
                        describe(exception),
                        delay / 1000);

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }

                delay = Math.min(delay * 2, MAX_RETRY_MS);
            }
        }
    }

    @Override
    public boolean publish(String topic, byte[] payload, int qos, boolean retained) {

        MqttAsyncClient current;

        synchronized (this) {
            current = client;
        }

        if (current == null || closed) {
            return false;
        }

        try {
            current.publish(topic, payload, qos, retained);
            return true;
        } catch (MqttException exception) {
            System.out.printf(
                    "[MQTT] Publish to %s refused: %s%n", topic, describe(exception));
            return false;
        }
    }

    @Override
    public boolean isConnected() {
        synchronized (state) {
            return connected;
        }
    }

    @Override
    public boolean awaitConnected(Duration timeout) throws InterruptedException {

        long deadline = System.nanoTime() + timeout.toNanos();

        synchronized (state) {
            while (!connected) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(state, remaining);
            }
            return true;
        }
    }

    @Override
    public synchronized void close() {

        closed = true;

        if (connector != null) {
            connector.interrupt();
        }

        if (client != null) {
            try {
                if (client.isConnected()) {
                    client.disconnect(1_000).waitForCompletion(2_000);
                }
            } catch (MqttException exception) {
                // Closing anyway.
            }

            try {
                client.close();
            } catch (MqttException exception) {
                // Already closed or still busy; the JVM is stopping the transport.
            }
        }

        if (inbound != null) {
            inbound.shutdownNow();
        }

        setConnected(false);
    }

    private void setConnected(boolean value) {
        synchronized (state) {
            connected = value;
            state.notifyAll();
        }
    }

    private void resubscribe() throws MqttException {

        if (subscriptions.isEmpty()) {
            return;
        }

        String[] filters = new String[subscriptions.size()];
        int[] qos = new int[subscriptions.size()];

        for (int i = 0; i < filters.length; i++) {
            filters[i] = subscriptions.get(i).filter();
            qos[i] = subscriptions.get(i).qos();
        }

        client.subscribe(filters, qos);
    }

    private void deliver(String topic, byte[] payload) {
        for (Subscription subscription : subscriptions) {
            if (MqttTopic.isMatched(subscription.filter(), topic)) {
                try {
                    subscription.handler().onMessage(topic, payload);
                } catch (RuntimeException exception) {
                    System.err.println(
                            "[MQTT] Handler for " + topic + " failed: " + exception);
                }
            }
        }
    }

    private static String describe(MqttException exception) {
        Throwable cause = exception.getCause();
        return exception.getMessage()
                + " (code " + exception.getReasonCode() + ")"
                + (cause != null ? ": " + cause.getMessage() : "");
    }

    private final class Callback implements MqttCallbackExtended {

        @Override
        public void connectComplete(boolean reconnect, String serverUri) {

            try {
                resubscribe();
            } catch (MqttException exception) {
                System.err.println("[MQTT] Subscribing failed: " + describe(exception));
            }

            setConnected(true);

            System.out.printf(
                    "[MQTT] %s broker %s as %s%n",
                    reconnect ? "Reconnected to" : "Connected to",
                    serverUri,
                    config.clientId());

            for (Listener listener : listeners) {
                try {
                    listener.connected(reconnect);
                } catch (RuntimeException exception) {
                    System.err.println("[MQTT] Listener failed: " + exception);
                }
            }
        }

        @Override
        public void connectionLost(Throwable cause) {

            setConnected(false);

            String reason = cause == null ? "unknown" : String.valueOf(cause.getMessage());

            System.out.printf("[MQTT] Connection to broker lost: %s%n", reason);

            for (Listener listener : listeners) {
                try {
                    listener.disconnected(reason);
                } catch (RuntimeException exception) {
                    System.err.println("[MQTT] Listener failed: " + exception);
                }
            }
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {

            byte[] payload = message.getPayload();

            try {
                inbound.execute(() -> deliver(topic, payload));
            } catch (RejectedExecutionException exception) {
                // Closing.
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            // QoS 1 acknowledgements from the broker carry no Phoenix meaning.
        }
    }
}
