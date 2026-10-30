// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** #18/#19: configuration of the MQTT transport is validated at startup. */
class MqttConfigTest {

    @TempDir
    Path dir;

    private final Map<String, String> properties = new HashMap<>();
    private final Map<String, String> environment = new HashMap<>();

    private MqttConfig config() {
        return MqttConfig.from(properties::get, environment::get);
    }

    private String rejection() {
        return assertThrows(IllegalArgumentException.class, this::config).getMessage();
    }

    private Path file(String name, String content) throws IOException {
        return Files.writeString(dir.resolve(name), content);
    }

    @Test
    void withoutAUrlMqttIsOff() {
        assertFalse(config().enabled());
    }

    @Test
    void aLoopbackBrokerMayBePlainTcp() {
        properties.put("phoenix.mqtt.url", "tcp://127.0.0.1:1883");
        MqttConfig config = config();

        assertTrue(config.enabled());
        assertFalse(config.usesTls());
        assertEquals("phoenix-core", config.clientId());
        assertEquals("phoenix", config.topicPrefix());
        assertEquals(Duration.ofSeconds(30), config.keepAlive());
        assertEquals(Duration.ofSeconds(5), config.heartbeatInterval());
        assertTrue(config.verifyHostname());

        properties.put("phoenix.mqtt.url", "tcp://localhost:1883");
        assertTrue(config().enabled());
    }

    @Test
    void aRemoteBrokerMustUseTlsUnlessExplicitlyAllowed() {
        properties.put("phoenix.mqtt.url", "tcp://192.0.2.10:1883");
        assertTrue(rejection().contains("phoenix.mqtt.allowInsecure"));

        properties.put("phoenix.mqtt.allowInsecure", "true");
        assertTrue(config().enabled());
    }

    @Test
    void theUrlMustBeSslOrTcpWithHostAndPort() {
        properties.put("phoenix.mqtt.url", "http://broker:1883");
        assertTrue(rejection().startsWith("phoenix.mqtt.url"));

        properties.put("phoenix.mqtt.url", "ssl://broker");
        assertTrue(rejection().contains("host and a port"));

        properties.put("phoenix.mqtt.url", "ssl://broker:8883/path");
        assertTrue(rejection().contains("path"));

        properties.put("phoenix.mqtt.url", "ssl://broker:8883");
        assertTrue(config().usesTls());
    }

    @Test
    void tlsFilesMustBeReadableAndOnlyUsedWithSsl() throws IOException {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");
        properties.put("phoenix.mqtt.tls.caFile", dir.resolve("missing.pem").toString());
        assertTrue(rejection().startsWith("phoenix.mqtt.tls.caFile"));

        properties.put("phoenix.mqtt.tls.caFile", file("ca.pem", "x").toString());
        assertEquals(dir.resolve("ca.pem"), config().caFile().orElseThrow());

        properties.put("phoenix.mqtt.url", "tcp://127.0.0.1:1883");
        assertTrue(rejection().contains("does not use ssl://"));
    }

    @Test
    void aClientKeyStoreNeedsAPassword() throws IOException {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");
        properties.put("phoenix.mqtt.tls.keyStore", file("core.p12", "x").toString());
        assertTrue(rejection().contains("keyStorePasswordFile"));

        environment.put("PHOENIX_MQTT_KEYSTORE_PASSWORD", "changeit");
        assertArrayEquals("changeit".toCharArray(), config().keyStorePassword().orElseThrow());
    }

    @Test
    void secretsComeFromFilesOrTheEnvironmentNeverFromProperties() throws IOException {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");
        properties.put("phoenix.mqtt.username", "phoenix-core");
        properties.put("phoenix.mqtt.passwordFile", file("pw", "s3cret\n").toString());
        environment.put("PHOENIX_MQTT_ENROLLMENT_TOKEN", "a-long-enrollment-token");

        MqttConfig config = config();

        assertArrayEquals("s3cret".toCharArray(), config.password().orElseThrow());
        assertEquals("a-long-enrollment-token", config.enrollmentToken().orElseThrow());
        assertFalse(config.toString().contains("s3cret"));
        assertFalse(config.toString().contains("a-long-enrollment-token"));
    }

    @Test
    void aFileWinsOverTheEnvironment() throws IOException {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");
        properties.put("phoenix.mqtt.enrollmentTokenFile",
                file("token", "token-from-the-file-1234").toString());
        environment.put("PHOENIX_MQTT_ENROLLMENT_TOKEN", "token-from-the-environment");

        assertEquals("token-from-the-file-1234", config().enrollmentToken().orElseThrow());
    }

    @Test
    void aShortEnrollmentTokenOrAnEmptySecretFileIsRefused() throws IOException {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");
        environment.put("PHOENIX_MQTT_ENROLLMENT_TOKEN", "short");
        assertTrue(rejection().contains("at least 16"));

        environment.clear();
        properties.put("phoenix.mqtt.enrollmentTokenFile", file("empty", "\n").toString());
        assertTrue(rejection().contains("empty file"));
    }

    @Test
    void aPasswordWithoutAUserIsRefused() {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");
        environment.put("PHOENIX_MQTT_PASSWORD", "s3cret");
        assertTrue(rejection().contains("phoenix.mqtt.username"));
    }

    @Test
    void clientIdAndPrefixMustBeSafeTopicLevels() {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");

        properties.put("phoenix.mqtt.topicPrefix", "phoenix/#");
        assertTrue(rejection().startsWith("phoenix.mqtt.topicPrefix"));

        properties.put("phoenix.mqtt.topicPrefix", "lab/phoenix");
        assertEquals("lab/phoenix", config().topicPrefix());

        properties.put("phoenix.mqtt.clientId", "core 1");
        assertTrue(rejection().startsWith("phoenix.mqtt.clientId"));
    }

    @Test
    void numericSettingsAreRangeChecked() {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");

        properties.put("phoenix.mqtt.keepAliveSeconds", "0");
        assertTrue(rejection().startsWith("phoenix.mqtt.keepAliveSeconds"));

        properties.put("phoenix.mqtt.keepAliveSeconds", "ten");
        assertTrue(rejection().startsWith("phoenix.mqtt.keepAliveSeconds"));

        properties.put("phoenix.mqtt.keepAliveSeconds", "60");
        properties.put("phoenix.mqtt.heartbeatIntervalSeconds", "3");
        assertEquals(Duration.ofSeconds(60), config().keepAlive());
        assertEquals(Duration.ofSeconds(3), config().heartbeatInterval());
    }

    @Test
    void hostnameVerificationCanOnlyBeTurnedOffExplicitly() {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");
        properties.put("phoenix.mqtt.tls.verifyHostname", "false");
        assertFalse(config().verifyHostname());
    }

    @Test
    void aMistypedHostnameVerificationSettingIsRejectedNotTreatedAsFalse() {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");
        properties.put("phoenix.mqtt.tls.verifyHostname", "tru");

        String message = rejection();

        assertTrue(message.contains("phoenix.mqtt.tls.verifyHostname"));
        assertTrue(message.contains("must be true or false"));
    }

    @Test
    void booleanSettingsIgnoreCaseAndSurroundingSpaceAndBlankMeansDefault() {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");

        properties.put("phoenix.mqtt.tls.verifyHostname", " FALSE ");
        assertFalse(config().verifyHostname());

        properties.put("phoenix.mqtt.tls.verifyHostname", "True");
        assertTrue(config().verifyHostname());

        properties.put("phoenix.mqtt.tls.verifyHostname", "   ");
        assertTrue(config().verifyHostname());
    }

    @Test
    void aMistypedAllowInsecureSettingIsRejected() {
        properties.put("phoenix.mqtt.url", "tcp://broker.example:1883");
        properties.put("phoenix.mqtt.allowInsecure", "ture");

        assertTrue(rejection().contains("must be true or false"));
    }

    @Test
    void anUnusableCaFileIsReportedWhenTlsIsSetUp() throws IOException {
        properties.put("phoenix.mqtt.url", "ssl://broker:8883");
        properties.put("phoenix.mqtt.tls.caFile", file("ca.pem", "not a certificate").toString());

        MqttConfig config = config();

        String message = assertThrows(IllegalArgumentException.class,
                () -> MqttTls.socketFactory(config)).getMessage();
        assertTrue(message.startsWith("phoenix.mqtt.tls.caFile"));
    }
}
