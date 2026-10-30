// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net.mqtt;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assumptions;

/**
 * A real Mosquitto broker for integration tests, configured the way
 * {@code deploy/mosquitto} configures production: certificates from
 * {@code gen-certs.sh}, password authentication, and the shipped
 * {@code acl} file.
 *
 * <p>Two TLS listeners: {@link #tlsPort()} (server authentication only) and
 * {@link #mutualTlsPort()} (client certificate required).
 *
 * <p>Tests using it are skipped when {@code mosquitto}, {@code mosquitto_passwd},
 * {@code openssl} or {@code bash} is not installed. CI installs them.
 */
final class MosquittoBroker implements AutoCloseable {

    static final String CORE_USER = "phoenix-core";

    private static final Path DEPLOY = Path.of("deploy", "mosquitto");

    private final Path dir;
    private final Path certs;
    private final Map<String, String> passwords = new LinkedHashMap<>();
    private final int tlsPort;
    private final int mutualTlsPort;
    private Process process;

    /** Skips the calling test unless every tool the broker needs is installed. */
    static void assumeInstalled() {
        for (String tool : List.of("mosquitto", "mosquitto_passwd", "openssl", "bash")) {
            Assumptions.assumeTrue(
                    executable(tool).isPresent(),
                    tool + " is not installed; skipping the Mosquitto integration test");
        }
    }

    /**
     * Generates certificates and users and starts the broker.
     *
     * @param brokerHosts names in the broker certificate
     * @param users       broker users besides {@link #CORE_USER}
     */
    static MosquittoBroker start(Path dir, List<String> brokerHosts, List<String> users)
            throws Exception {

        MosquittoBroker broker = new MosquittoBroker(dir, freePort(), freePort());
        broker.generate(brokerHosts, users);
        broker.launch();
        return broker;
    }

    private MosquittoBroker(Path dir, int tlsPort, int mutualTlsPort) {
        this.dir = dir;
        this.certs = dir.resolve("certs");
        this.tlsPort = tlsPort;
        this.mutualTlsPort = mutualTlsPort;
    }

    int tlsPort() {
        return tlsPort;
    }

    int mutualTlsPort() {
        return mutualTlsPort;
    }

    Path caFile() {
        return certs.resolve("ca.pem");
    }

    Path coreKeyStore() {
        return certs.resolve("core.p12");
    }

    Path coreKeyStorePasswordFile() {
        return certs.resolve("core.p12.password");
    }

    String password(String user) {
        return passwords.get(user);
    }

    /** Generates a second, unrelated CA (for "untrusted broker" tests). */
    static Path otherCa(Path dir) throws Exception {
        run(List.of("bash", DEPLOY.resolve("gen-certs.sh").toString(),
                dir.toString(), "localhost"));
        return dir.resolve("ca.pem");
    }

    private void generate(List<String> brokerHosts, List<String> users) throws Exception {

        List<String> command = new ArrayList<>(List.of(
                "bash", DEPLOY.resolve("gen-certs.sh").toString(), certs.toString()));
        command.addAll(brokerHosts);
        run(command);

        Path passwd = dir.resolve("passwd");
        List<String> everyone = new ArrayList<>(users);
        everyone.add(0, CORE_USER);

        for (String user : everyone) {
            String password = UUID.randomUUID().toString();
            passwords.put(user, password);
            List<String> add = new ArrayList<>(List.of(executable("mosquitto_passwd").orElseThrow().toString()));
            if (!Files.exists(passwd)) {
                add.add("-c");
            }
            add.addAll(List.of("-b", passwd.toString(), user, password));
            run(add);
        }

        Path acl = DEPLOY.resolve("acl").toAbsolutePath();

        String tls = """
                cafile %s
                certfile %s
                keyfile %s
                tls_version tlsv1.2
                """.formatted(caFile(), certs.resolve("server.pem"), certs.resolve("server.key"));

        Files.writeString(dir.resolve("mosquitto.conf"), """
                per_listener_settings false
                allow_anonymous false
                password_file %s
                acl_file %s
                persistence false
                max_packet_size 70000
                log_dest stdout
                log_type error
                log_type warning
                log_type notice
                user %s

                listener %d
                %s
                listener %d
                %s
                require_certificate true
                """.formatted(
                        passwd, acl, System.getProperty("user.name"),
                        tlsPort, tls, mutualTlsPort, tls));
    }

    private void launch() throws Exception {

        process = new ProcessBuilder(
                executable("mosquitto").orElseThrow().toString(),
                "-c", dir.resolve("mosquitto.conf").toString())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(dir.resolve("mosquitto.log").toFile()))
                .start();

        long deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline) {
            if (!process.isAlive()) {
                throw new IllegalStateException("mosquitto exited: " + log());
            }
            if (accepts(tlsPort) && accepts(mutualTlsPort)) {
                return;
            }
            Thread.sleep(50);
        }

        throw new IllegalStateException("mosquitto did not start: " + log());
    }

    /** Stops the broker; {@link #restart()} brings it back on the same ports. */
    void stop() throws InterruptedException {
        if (process != null) {
            process.destroy();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly().waitFor();
            }
            process = null;
        }
    }

    void restart() throws Exception {
        stop();
        launch();
    }

    String log() {
        try {
            return Files.readString(dir.resolve("mosquitto.log"));
        } catch (IOException exception) {
            return "(no log)";
        }
    }

    @Override
    public void close() {
        try {
            stop();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean accepts(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 200);
            return true;
        } catch (IOException exception) {
            return false;
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void run(List<String> command) throws Exception {
        Process step = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(step.getInputStream().readAllBytes());
        if (step.waitFor() != 0) {
            throw new IllegalStateException(String.join(" ", command) + " failed:\n" + output);
        }
    }

    static Optional<Path> executable(String name) {
        List<String> directories = new ArrayList<>();
        String path = System.getenv("PATH");
        if (path != null) {
            directories.addAll(List.of(path.split(File.pathSeparator)));
        }
        // Debian and Ubuntu install the broker in sbin, often not on a user's PATH.
        directories.addAll(List.of("/usr/sbin", "/usr/local/sbin", "/opt/homebrew/sbin"));

        for (String directory : directories) {
            Path candidate = Path.of(directory, name);
            if (Files.isExecutable(candidate)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }
}
