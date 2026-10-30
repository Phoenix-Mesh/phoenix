// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * A deterministic, in-process MQTT broker for tests.
 *
 * <p>It implements what the Phoenix transport relies on: topic filters with
 * {@code +} and {@code #}, retained messages, last wills, link loss and
 * reconnection, and in-order delivery. Deliveries are made one at a time and
 * never re-entrantly: a message published from inside a handler is delivered
 * after that handler returns, the way a real client hands messages to one
 * inbound thread.
 *
 * <p>QoS is accepted but not modelled: every message is delivered exactly
 * once to every connected matching subscriber, and messages to a
 * disconnected client are dropped. Duplicates are injected explicitly by
 * tests that need them.
 */
final class InMemoryMqttBroker {

    private final List<Connection> connections = new CopyOnWriteArrayList<>();
    private final Map<String, byte[]> retained = new ConcurrentHashMap<>();
    private final List<Published> log = new CopyOnWriteArrayList<>();

    private final Deque<Runnable> deliveries = new ArrayDeque<>();
    private boolean draining;

    /** One message as it passed through the broker. */
    record Published(String clientId, String topic, byte[] payload, boolean retained) {
        String text() {
            return new String(payload, java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    Connection connect(String clientId) {
        Connection connection = new Connection(clientId);
        connections.add(connection);
        return connection;
    }

    /** Every message published so far, by any client. */
    List<Published> published() {
        return List.copyOf(log);
    }

    List<Published> publishedOn(String topic) {
        return log.stream().filter(p -> p.topic().equals(topic)).toList();
    }

    byte[] retainedOn(String topic) {
        return retained.get(topic);
    }

    private void route(String from, String topic, byte[] payload, boolean retain) {

        log.add(new Published(from, topic, payload, retain));

        if (retain) {
            if (payload.length == 0) {
                retained.remove(topic);
            } else {
                retained.put(topic, payload);
            }
        }

        for (Connection connection : connections) {
            if (!connection.connected) {
                continue;
            }
            for (Subscription subscription : connection.subscriptions) {
                if (matches(subscription.filter(), topic)) {
                    enqueue(() -> subscription.handler().onMessage(topic, payload));
                }
            }
        }

        drain();
    }

    private void enqueue(Runnable delivery) {
        synchronized (deliveries) {
            deliveries.add(delivery);
        }
    }

    private void drain() {
        synchronized (deliveries) {
            if (draining) {
                return;
            }
            draining = true;
        }

        while (true) {
            Runnable next;
            synchronized (deliveries) {
                next = deliveries.poll();
                if (next == null) {
                    draining = false;
                    return;
                }
            }
            try {
                next.run();
            } catch (RuntimeException exception) {
                System.err.println("[InMemoryMqttBroker] handler failed: " + exception);
            }
        }
    }

    /** MQTT 3.1.1 topic filter matching. */
    static boolean matches(String filter, String topic) {
        String[] f = filter.split("/", -1);
        String[] t = topic.split("/", -1);

        for (int i = 0; i < f.length; i++) {
            if (f[i].equals("#")) {
                return true;
            }
            if (i >= t.length) {
                return false;
            }
            if (!f[i].equals("+") && !f[i].equals(t[i])) {
                return false;
            }
        }
        return f.length == t.length;
    }

    private record Subscription(String filter, MqttConnection.MessageHandler handler) {}

    /** One client connection. */
    final class Connection implements MqttConnection {

        final String clientId;
        private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
        private final List<Listener> listeners = new CopyOnWriteArrayList<>();
        private final Object state = new Object();
        private volatile boolean connected;
        private volatile boolean everConnected;
        private volatile boolean closed;
        private String willTopic;
        private byte[] willPayload;
        private boolean willRetained;

        Connection(String clientId) {
            this.clientId = clientId;
        }

        @Override
        public void lastWill(String topic, byte[] payload, int qos, boolean retain) {
            willTopic = topic;
            willPayload = payload.clone();
            willRetained = retain;
        }

        @Override
        public void subscribe(String topicFilter, int qos, MessageHandler handler) {
            subscriptions.add(new Subscription(topicFilter, handler));
        }

        @Override
        public void addListener(Listener listener) {
            listeners.add(listener);
        }

        @Override
        public void open() {
            goOnline();
        }

        /** Simulates the link coming (back) up. */
        void goOnline() {
            if (closed) {
                return;
            }

            boolean reconnect = everConnected;

            synchronized (state) {
                connected = true;
                everConnected = true;
                state.notifyAll();
            }

            // Retained messages for every subscription, as on a real subscribe.
            for (Subscription subscription : subscriptions) {
                retained.forEach((topic, payload) -> {
                    if (matches(subscription.filter(), topic)) {
                        enqueue(() -> subscription.handler().onMessage(topic, payload));
                    }
                });
            }

            listeners.forEach(listener -> listener.connected(reconnect));
            drain();
        }

        /** Simulates the link dropping; the broker publishes the last will. */
        void dropLink() {
            synchronized (state) {
                connected = false;
            }

            listeners.forEach(listener -> listener.disconnected("simulated link loss"));

            if (willTopic != null) {
                route(clientId, willTopic, willPayload, willRetained);
            }
        }

        @Override
        public boolean publish(String topic, byte[] payload, int qos, boolean retain) {
            if (!connected || closed) {
                return false;
            }
            route(clientId, topic, payload.clone(), retain);
            return true;
        }

        @Override
        public boolean isConnected() {
            return connected;
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
        public void close() {
            closed = true;
            synchronized (state) {
                connected = false;
            }
            connections.remove(this);
        }
    }
}
