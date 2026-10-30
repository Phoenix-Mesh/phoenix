// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import com.google.inject.Singleton;

/**
 * Transport-neutral record of which device is which node, and through which
 * transport the node can currently be reached.
 *
 * <p>A node agent keeps a stable {@code deviceId}; Core assigns the
 * {@code nodeId}. Every transport gateway (HTTP today, MQTT, and any later
 * one) resolves a device through this one table, so a device keeps a single
 * node identity even when it moves from one transport to another, and the
 * scheduler never sees two registry entries for one physical device.
 *
 * <p>The transport is recorded as an opaque id chosen by the gateway (for
 * example {@code "http"} or {@code "mqtt"}). Core only stores it; deciding
 * which client executes on the node is the job of the transport routing in
 * the network layer. The latest registration wins: a device that registers
 * again through another transport is reached through that transport from
 * then on.
 *
 * <h2>Concurrency</h2>
 *
 * <p>{@link #attach} and {@link #detach} are mutually exclusive, so they take
 * effect one after the other in a single order. "Latest" means the attach
 * that ran last in that order; two registrations that truly overlap have no
 * other order to be latest in.
 *
 * <p>Everything known about a node (its device and its transport) is one
 * immutable value that is replaced in a single step, so a lookup can never
 * observe a node with a device but without a transport, or with half of a
 * change. Lookups take no lock, because they run on every dispatch and every
 * heartbeat while registrations are rare. They are individually consistent;
 * two separate lookups (for instance {@code nodeIdFor} followed by
 * {@code transportOf}) can still straddle a concurrent change, and callers
 * treat an empty answer as "not attached".
 *
 * <p>A node becomes visible only after everything about it is in place, and
 * stops being visible before any of it is removed, so a node id obtained
 * from {@link #nodeIdFor} always has a device and a transport unless it was
 * detached in the meantime.
 */
@Singleton
public class NodeAttachments {

    /** Everything known about one node; replaced as a whole, never edited. */
    private record Entry(String deviceId, String transport) {}

    /** Serialises {@link #attach} and {@link #detach}; lookups do not take it. */
    private final Object writeLock = new Object();

    /** The source of truth, by node. */
    private final Map<String, Entry> byNodeId = new ConcurrentHashMap<>();

    /** Index from device to its node; written after, removed before, {@link #byNodeId}. */
    private final Map<String, String> nodeIdByDeviceId = new ConcurrentHashMap<>();

    /**
     * Attaches a device through {@code transport}, registering it as a new
     * node only if the device is not known yet.
     *
     * <p>Atomic: two gateways registering the same device at the same moment
     * cannot mint two nodes for it, and a concurrent {@link #detach} runs
     * entirely before or entirely after this call. {@code registerNew} runs
     * while the table is locked, so it must be quick and must not call back
     * into code that waits for another thread's attach or detach.
     *
     * @param registerNew registers the device and returns the new node id;
     *        called at most once, and only for an unknown device. If it
     *        throws, nothing is recorded and the exception reaches the caller.
     * @return the node id, whether this call created it, and the transport it
     *          moved away from, read in the same step as the change
     * @throws IllegalStateException if {@code registerNew} returns a node id
     *         that already belongs to another device
     */
    public Attachment attach(
            String deviceId,
            String transport,
            Supplier<String> registerNew) {

        Objects.requireNonNull(deviceId, "deviceId");
        Objects.requireNonNull(transport, "transport");
        Objects.requireNonNull(registerNew, "registerNew");

        synchronized (writeLock) {

            String existing = nodeIdByDeviceId.get(deviceId);

            if (existing != null) {
                // One replacement: device and transport change together,
                // and the previous transport is the one actually replaced.
                Entry before = byNodeId.put(existing, new Entry(deviceId, transport));

                Optional<String> movedFrom = Optional.ofNullable(before)
                        .map(Entry::transport)
                        .filter(previous -> !previous.equals(transport));

                return new Attachment(existing, false, movedFrom);
            }

            String nodeId = Objects.requireNonNull(
                    registerNew.get(), "registered node id");

            if (byNodeId.putIfAbsent(nodeId, new Entry(deviceId, transport)) != null) {
                throw new IllegalStateException(
                        "node id " + nodeId + " already belongs to another device");
            }

            // Published last, so whoever finds the node by its device finds
            // it complete.
            nodeIdByDeviceId.put(deviceId, nodeId);

            return new Attachment(nodeId, true, Optional.empty());
        }
    }

    /** The node a device is attached as, if any. */
    public Optional<String> nodeIdFor(String deviceId) {
        return deviceId == null
                ? Optional.empty()
                : Optional.ofNullable(nodeIdByDeviceId.get(deviceId));
    }

    /** The device behind a node, if any. */
    public Optional<String> deviceIdFor(String nodeId) {
        return entry(nodeId).map(Entry::deviceId);
    }

    /** The transport the node registered through most recently, if any. */
    public Optional<String> transportOf(String nodeId) {
        return entry(nodeId).map(Entry::transport);
    }

    /**
     * Forgets a device, for example when its node no longer exists in the
     * registry. The next registration of the device creates a new node.
     */
    public void detach(String deviceId) {

        if (deviceId == null) {
            return;
        }

        synchronized (writeLock) {

            // Hidden from lookups by device first, then dropped.
            String nodeId = nodeIdByDeviceId.remove(deviceId);

            if (nodeId != null) {
                byNodeId.remove(nodeId);
            }
        }
    }

    private Optional<Entry> entry(String nodeId) {
        return nodeId == null
                ? Optional.empty()
                : Optional.ofNullable(byNodeId.get(nodeId));
    }

    /**
     * Result of {@link #attach}.
     *
     * @param nodeId    the node the device is attached as
     * @param created   true if this call registered the node
     * @param movedFrom the transport the node was reached through until this
     *                  call, if this call changed it; empty for a new node or
     *                  an unchanged transport. Taken under the same lock as
     *                  the change, so it is never stale.
     */
    public record Attachment(
            String nodeId,
            boolean created,
            Optional<String> movedFrom
    ) {}
}
