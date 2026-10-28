import java.time.Duration
import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    application
    java
}

group = "com.phoenix"
version = "0.3.0"

java {
    // The design document targets Java 17 for JIVE instrumentation. The
    // baseline stays on 21 until JIVE has been validated against both.
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.google.inject:guice:7.0.0")
    implementation("com.google.code.gson:gson:2.14.0")
    implementation("com.googlecode.lanterna:lanterna:3.1.5")
    implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("com.phoenix.App")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-Xlint:all,-processing", "-Werror"))
}

tasks.javadoc {
    options {
        this as StandardJavadocDocletOptions
        encoding = "UTF-8"
        addBooleanOption("Xdoclint:all,-missing", true)
    }
}

tasks.check {
    dependsOn(tasks.javadoc)
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`

    // Forward every -Dphoenix.* property given to Gradle into the Core JVM.
    System.getProperties()
        .stringPropertyNames()
        .filter { it.startsWith("phoenix.") }
        .forEach { systemProperty(it, System.getProperty(it)) }
}

tasks.test {
    useJUnitPlatform()
    timeout.set(Duration.ofMinutes(10))
    testLogging {
        events("failed", "skipped")
        exceptionFormat = TestExceptionFormat.FULL
    }
}
