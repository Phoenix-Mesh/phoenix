// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

/**
 * Configuration of Core's MQTT transport, read from {@code phoenix.mqtt.*}
 * system properties.
 *
 * <p>The transport is off unless {@code phoenix.mqtt.url} is set. Invalid
 * values stop Core at startup with a message naming the property.
 *
 * <p>Secrets are never read from system properties, which are visible to
 * every local user in the process list. Each secret is read from a file named
 * by a property, or from an environment variable:
 *
 * <table>
 *   <caption>Where each secret comes from</caption>
 *   <tr><th>Secret</th><th>File property</th><th>Environment variable</th></tr>
 *   <tr><td>broker password</td><td>{@code phoenix.mqtt.passwordFile}</td>
 *       <td>{@code PHOENIX_MQTT_PASSWORD}</td></tr>
 *   <tr><td>client key store password</td>
 *       <td>{@code phoenix.mqtt.tls.keyStorePasswordFile}</td>
 *       <td>{@code PHOENIX_MQTT_KEYSTORE_PASSWORD}</td></tr>
 *   <tr><td>node enrollment token</td><td>{@code phoenix.mqtt.enrollmentTokenFile}</td>
 *       <td>{@code PHOENIX_MQTT_ENROLLMENT_TOKEN}</td></tr>
 * </table>
 *
 * <p>A plain {@code tcp://} broker URL is accepted only for a loopback
 * broker (the recommended deployment runs Mosquitto next to Core and lets the
 * nodes connect to it over TLS), unless {@code phoenix.mqtt.allowInsecure}
 * is set.
 *
 * @param enabled             whether the MQTT transport runs at all
 * @param serverUri           {@code ssl://host:8883} or {@code tcp://host:1883}
 * @param clientId            Core's MQTT client id
 * @param topicPrefix         first topic level of the Phoenix namespace
 * @param username            broker user name, if any
 * @param password            broker password, if any
 * @param caFile              PEM file with the CA certificate(s) Core trusts
 *                            for the broker; empty means the JDK trust store
 * @param keyStore            PKCS#12 key store with Core's client certificate
 *                            for mutual TLS, if any
 * @param keyStorePassword    password of {@code keyStore}
 * @param verifyHostname      whether the broker certificate must match its host
 * @param enrollmentToken     token nodes must present to register, if any
 * @param keepAlive           MQTT keep-alive interval
 * @param connectTimeout      how long one connection attempt may take
 * @param heartbeatInterval   heartbeat interval Core advises nodes to use
 */
public record MqttConfig(
        boolean enabled,
        String serverUri,
        String clientId,
        String topicPrefix,
        Optional<String> username,
        Optional<char[]> password,
        Optional<Path> caFile,
        Optional<Path> keyStore,
        Optional<char[]> keyStorePassword,
        boolean verifyHostname,
        Optional<String> enrollmentToken,
        Duration keepAlive,
        Duration connectTimeout,
        Duration heartbeatInterval
) {

    public static final String DEFAULT_CLIENT_ID = "phoenix-core";
    public static final String DEFAULT_TOPIC_PREFIX = "phoenix";

    /** Configuration of a Core that does not use MQTT. */
    public static MqttConfig disabled() {
        return new MqttConfig(
                false, "", DEFAULT_CLIENT_ID, DEFAULT_TOPIC_PREFIX,
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), true, Optional.empty(),
                Duration.ofSeconds(30), Duration.ofSeconds(10),
                Duration.ofSeconds(5));
    }

    /** True for {@code ssl://} broker URLs. */
    public boolean usesTls() {
        return serverUri.toLowerCase(Locale.ROOT).startsWith("ssl://");
    }

    /** Reads the configuration from system properties and the environment. */
    public static MqttConfig fromSystemProperties() {
        return from(System::getProperty, System::getenv);
    }

    /**
     * Reads the configuration from the given sources.
     *
     * @param properties system-property lookup
     * @param environment environment-variable lookup
     * @return the validated configuration
     * @throws IllegalArgumentException naming the invalid property
     */
    public static MqttConfig from(
            Function<String, String> properties,
            Function<String, String> environment) {

        String url = trimmed(properties.apply("phoenix.mqtt.url"));

        if (url == null) {
            return disabled();
        }

        URI uri = parseServerUri(url);

        boolean allowInsecure = flag(properties, "phoenix.mqtt.allowInsecure", false);

        if ("tcp".equals(uri.getScheme())
                && !allowInsecure
                && !isLoopback(uri.getHost())) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.url " + url + " is unencrypted and not on this "
                            + "machine; use ssl:// or set phoenix.mqtt.allowInsecure=true");
        }

        String clientId = orDefault(
                properties.apply("phoenix.mqtt.clientId"), DEFAULT_CLIENT_ID);

        if (clientId.length() > 64 || !MqttTopics.isValidSegment(clientId)) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.clientId must be 1-64 characters of "
                            + "[A-Za-z0-9._-], was '" + clientId + "'");
        }

        String prefix = orDefault(
                properties.apply("phoenix.mqtt.topicPrefix"), DEFAULT_TOPIC_PREFIX);

        if (!MqttTopics.isValidPrefix(prefix)) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.topicPrefix must be topic levels of "
                            + "[A-Za-z0-9._-] separated by '/', was '" + prefix + "'");
        }

        Optional<Path> caFile = readablePath(properties, "phoenix.mqtt.tls.caFile");
        Optional<Path> keyStore = readablePath(properties, "phoenix.mqtt.tls.keyStore");

        if (!uri.getScheme().equals("ssl")
                && (caFile.isPresent() || keyStore.isPresent())) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.tls.* is set but phoenix.mqtt.url " + url
                            + " does not use ssl://");
        }

        Optional<char[]> keyStorePassword = secret(
                properties, environment,
                "phoenix.mqtt.tls.keyStorePasswordFile",
                "PHOENIX_MQTT_KEYSTORE_PASSWORD");

        if (keyStore.isPresent() && keyStorePassword.isEmpty()) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.tls.keyStore needs a password: set "
                            + "phoenix.mqtt.tls.keyStorePasswordFile or "
                            + "PHOENIX_MQTT_KEYSTORE_PASSWORD");
        }

        Optional<String> username =
                Optional.ofNullable(trimmed(properties.apply("phoenix.mqtt.username")));

        Optional<char[]> password = secret(
                properties, environment,
                "phoenix.mqtt.passwordFile", "PHOENIX_MQTT_PASSWORD");

        if (password.isPresent() && username.isEmpty()) {
            throw new IllegalArgumentException(
                    "a broker password is configured but phoenix.mqtt.username is not");
        }

        Optional<String> enrollmentToken = secret(
                properties, environment,
                "phoenix.mqtt.enrollmentTokenFile", "PHOENIX_MQTT_ENROLLMENT_TOKEN")
                .map(String::new);

        if (enrollmentToken.isPresent() && enrollmentToken.get().length() < 16) {
            throw new IllegalArgumentException(
                    "the MQTT enrollment token must be at least 16 characters");
        }

        boolean verifyHostname = flag(properties, "phoenix.mqtt.tls.verifyHostname", true);

        return new MqttConfig(
                true,
                uri.toString(),
                clientId,
                prefix,
                username,
                password,
                caFile,
                keyStore,
                keyStorePassword,
                verifyHostname,
                enrollmentToken,
                seconds(properties, "phoenix.mqtt.keepAliveSeconds", 30, 5, 600),
                seconds(properties, "phoenix.mqtt.connectTimeoutSeconds", 10, 1, 120),
                seconds(properties, "phoenix.mqtt.heartbeatIntervalSeconds", 5, 1, 60));
    }

    /** Never prints secrets. */
    @Override
    public String toString() {
        return "MqttConfig[enabled=" + enabled
                + ", serverUri=" + serverUri
                + ", clientId=" + clientId
                + ", topicPrefix=" + topicPrefix
                + ", username=" + username.orElse("-")
                + ", password=" + (password.isPresent() ? "***" : "-")
                + ", caFile=" + caFile.map(Path::toString).orElse("-")
                + ", keyStore=" + keyStore.map(Path::toString).orElse("-")
                + ", verifyHostname=" + verifyHostname
                + ", enrollmentToken=" + (enrollmentToken.isPresent() ? "***" : "-")
                + "]";
    }

    private static URI parseServerUri(String url) {

        URI uri;

        try {
            uri = new URI(url);
        } catch (URISyntaxException exception) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.url is not a valid URI: " + url);
        }

        String scheme = uri.getScheme() == null
                ? ""
                : uri.getScheme().toLowerCase(Locale.ROOT);

        if (!scheme.equals("ssl") && !scheme.equals("tcp")) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.url must start with ssl:// or tcp://, was " + url);
        }

        if (uri.getHost() == null || uri.getPort() < 1 || uri.getPort() > 65_535) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.url must name a host and a port, was " + url);
        }

        if (uri.getRawPath() != null && !uri.getRawPath().isEmpty()) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.url must not have a path, was " + url);
        }

        return URI.create(scheme + "://" + uri.getRawAuthority());
    }

    private static boolean isLoopback(String host) {
        if ("localhost".equalsIgnoreCase(host)) {
            return true;
        }

        try {
            // Only literal addresses are checked, so this never does a DNS lookup.
            return host.matches("[0-9.]+|\\[?[0-9a-fA-F:]+\\]?")
                    && InetAddress.getByName(host).isLoopbackAddress();
        } catch (UnknownHostException exception) {
            return false;
        }
    }

    private static Optional<Path> readablePath(
            Function<String, String> properties,
            String property) {

        String value = trimmed(properties.apply(property));

        if (value == null) {
            return Optional.empty();
        }

        Path path = Path.of(value);

        if (!Files.isReadable(path)) {
            throw new IllegalArgumentException(
                    property + " names a file that cannot be read: " + value);
        }

        return Optional.of(path);
    }

    private static Optional<char[]> secret(
            Function<String, String> properties,
            Function<String, String> environment,
            String fileProperty,
            String environmentVariable) {

        Optional<Path> file = readablePath(properties, fileProperty);

        if (file.isPresent()) {
            try {
                String value = Files.readString(file.get(), StandardCharsets.UTF_8).strip();

                if (value.isEmpty()) {
                    throw new IllegalArgumentException(
                            fileProperty + " names an empty file: " + file.get());
                }

                return Optional.of(value.toCharArray());

            } catch (IOException exception) {
                throw new IllegalArgumentException(
                        fileProperty + " could not be read: " + exception.getMessage());
            }
        }

        String value = trimmed(environment.apply(environmentVariable));

        return value == null
                ? Optional.empty()
                : Optional.of(value.toCharArray());
    }

    /**
     * A true/false setting. Anything else is a configuration error rather
     * than a silent {@code false}: for {@code phoenix.mqtt.tls.verifyHostname}
     * a typo such as {@code tru} would otherwise switch verification off.
     */
    private static boolean flag(
            Function<String, String> properties,
            String property,
            boolean defaultValue) {

        String value = trimmed(properties.apply(property));

        if (value == null) {
            return defaultValue;
        }

        if ("true".equalsIgnoreCase(value)) {
            return true;
        }

        if ("false".equalsIgnoreCase(value)) {
            return false;
        }

        throw new IllegalArgumentException(
                property + " must be true or false, not \"" + value + "\"");
    }

    private static Duration seconds(
            Function<String, String> properties,
            String property,
            int defaultValue,
            int min,
            int max) {

        String value = trimmed(properties.apply(property));

        if (value == null) {
            return Duration.ofSeconds(defaultValue);
        }

        try {
            int parsed = Integer.parseInt(value);

            if (parsed < min || parsed > max) {
                throw new NumberFormatException();
            }

            return Duration.ofSeconds(parsed);

        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    property + " must be a whole number of seconds between "
                            + min + " and " + max + ", was '" + value + "'");
        }
    }

    private static String orDefault(String value, String defaultValue) {
        String trimmed = trimmed(value);
        return trimmed == null ? defaultValue : trimmed;
    }

    private static String trimmed(String value) {
        if (value == null) {
            return null;
        }

        String trimmed = value.strip();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
