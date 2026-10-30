// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * Builds the TLS socket factory Core uses for an {@code ssl://} broker.
 *
 * <ul>
 *     <li>Trust: the CA certificates in {@code phoenix.mqtt.tls.caFile}
 *         (PEM, the file Mosquitto's {@code cafile} also uses) and nothing
 *         else; without that property, the JDK's default trust store.</li>
 *     <li>Identity (optional, for brokers that require client
 *         certificates): the PKCS#12 key store in
 *         {@code phoenix.mqtt.tls.keyStore}.</li>
 *     <li>Protocols: whatever the JDK enables for clients, which on JDK 21
 *         is TLS 1.3 and 1.2; older versions are disabled by the JDK.</li>
 * </ul>
 *
 * <p>Host name verification is configured on the client, not here (see
 * {@link PahoMqttConnection}).
 */
public final class MqttTls {

    private MqttTls() {
    }

    /**
     * @throws IllegalArgumentException naming the property whose file could
     *         not be used
     */
    public static SSLSocketFactory socketFactory(MqttConfig config) {
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keyManagers(config), trustManagers(config), null);
            return context.getSocketFactory();

        } catch (GeneralSecurityException exception) {
            throw new IllegalArgumentException(
                    "TLS for the MQTT broker could not be set up: "
                            + exception.getMessage(), exception);
        }
    }

    private static TrustManager[] trustManagers(MqttConfig config)
            throws GeneralSecurityException {

        if (config.caFile().isEmpty()) {
            return null; // the JDK default trust store
        }

        Path caFile = config.caFile().get();
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());

        try {
            trustStore.load(null, null);
        } catch (IOException exception) {
            throw new IllegalStateException(exception);
        }

        Collection<? extends Certificate> certificates;

        try (InputStream in = Files.newInputStream(caFile)) {
            certificates = CertificateFactory.getInstance("X.509")
                    .generateCertificates(in);
        } catch (IOException | GeneralSecurityException exception) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.tls.caFile " + caFile
                            + " is not a PEM certificate file: " + exception.getMessage(),
                    exception);
        }

        if (certificates.isEmpty()) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.tls.caFile " + caFile + " contains no certificate");
        }

        int index = 0;
        for (Certificate certificate : certificates) {
            trustStore.setCertificateEntry("phoenix-mqtt-ca-" + index++, certificate);
        }

        TrustManagerFactory factory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(trustStore);
        return factory.getTrustManagers();
    }

    private static KeyManager[] keyManagers(MqttConfig config)
            throws GeneralSecurityException {

        if (config.keyStore().isEmpty()) {
            return null;
        }

        Path path = config.keyStore().get();
        char[] password = config.keyStorePassword().orElseThrow();
        KeyStore keyStore = KeyStore.getInstance("PKCS12");

        try (InputStream in = Files.newInputStream(path)) {
            keyStore.load(in, password);
        } catch (IOException | GeneralSecurityException exception) {
            throw new IllegalArgumentException(
                    "phoenix.mqtt.tls.keyStore " + path
                            + " could not be opened as PKCS#12 with the given password: "
                            + exception.getMessage(),
                    exception);
        }

        KeyManagerFactory factory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(keyStore, password);
        return factory.getKeyManagers();
    }
}
