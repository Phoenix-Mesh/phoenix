# Changelog

All notable changes to Phoenix Core. Versions follow [Semantic Versioning](https://semver.org/);
while the major version is 0, minor versions may change the node-agent protocol.

## [Unreleased]

## [0.3.0] - 2026-10-28

MQTT/TLS node-agent transport (#17: #18-#24). The HTTP transports are unchanged
apart from sharing the device-to-node table.

### Validated

- Android node agent over MQTT/TLS on two physical devices, a legacy tablet (Android 5.1)
  and a current phone, with the real Core and broker (#23):
  [phoenix-android-agent](https://github.com/Phoenix-Mesh/phoenix-android-agent).
- Linux node agent over MQTT/TLS with the real Core and broker, alone and next to an
  Android node (#24): [phoenix-linux-agent](https://github.com/Phoenix-Mesh/phoenix-linux-agent).
  A device that registered over HTTP keeps its node identity when it moves to MQTT.

### Added

- MQTT/TLS transport on Eclipse Paho 1.2.5 (`com.phoenix.net.mqtt`), enabled with
  `phoenix.mqtt.url`. Protocol version 1 under `phoenix/v1/...` is specified in
  `docs/mqtt-agent-protocol.md`.
- Transport contract (#18): node-scoped topic namespace (`nodes/{deviceId}/up|down/{kind}`)
  designed for per-device broker ACLs; JSON bodies shared with HTTP; `MqttConnection`
  seam between the transport and the MQTT library.
- Broker connectivity (#19): TLS with a PEM CA file and host name verification,
  optional PKCS#12 client certificate, broker credentials from files or environment
  variables, persistent session, background reconnect with an offline buffer, refusal
  of plain TCP to a non-local broker. Core's presence is announced retained on
  `core/status` with a last will.
- Registration and telemetry (#20): optional enrollment token, a session token per
  registration (stale agent copies are rejected), idempotent registration by
  `requestId`, `reregister` prompts, heartbeats into the same telemetry and health path.
- Task delivery and result correlation (#21): application-level `ack` as the delivery
  boundary, so delivery/execution timeouts keep their HTTP meaning; mandatory attempt
  fencing (attempt id, sending device and session token must all match); duplicate
  reports are ignored; `cancel` when Core stops waiting.
- Lifecycle integration (#22): abandonment on node `OFFLINE` wakes MQTT attempts at
  once; the unchanged dispatcher re-places them.
- `NodeTransportRouter`: a node is executed through the transport it registered with,
  so an Android or Linux node can use MQTT and an ESP32 node can finally run tasks.
- `TaskFailureReason.TRANSPORT_UNAVAILABLE`: Core's own broker link was down; retryable
  and never held against the node.
- `deploy/mosquitto`: Mosquitto configuration (loopback, TLS and optional TLS-PSK
  listeners), ACL, certificate and device-account scripts.
- `agents/python`: reference Linux agent for the MQTT/TLS protocol (paho-mqtt 2).
- Console header shows the broker link (`MQTT ●`/`MQTT ○`).
- Tests: in-memory broker unit tests, and integration tests against a real Mosquitto
  with TLS, authentication, the shipped ACL, mutual TLS and a broker restart.

### Changed

- Which device is which node is kept in the transport-neutral `NodeAttachments`
  table shared by every gateway, so a device keeps one node identity across transports.
- `DeviceAnnouncement.validate()` and `HeartbeatMessage.toResourceStatus()` hold
  validation and conversion shared by the HTTP and MQTT gateways.
- `TaskWire` and `JsonText` are public so every transport builds the same task body.
- CI installs Mosquitto so the broker integration tests run.
- README: node agents table linking the Android and Linux agent repositories, and
  how to run the Linux agent; `agents/python` is the protocol reference.

## [0.2.1] - 2026-10-14

Hardening release that closes the v0.2 Core milestone before the MQTT/TLS transport work.

### Fixed

- `./gradlew run -Dphoenix.*` now forwards every `phoenix.*` property to Core.
  Previously only `phoenix.advertiseAddress` reached it, so every other documented
  setting was silently ignored when Core was started through Gradle.
- A push node that answers with a malformed result body now fails the attempt as
  `TRANSPORT_ERROR`. Previously the parse error escaped the transport and left the
  attempt non-terminal in the lifecycle ledger.
- `GreedyCostScheduler` always selects an eligible node, even when a cost is `NaN`,
  instead of failing with a `NullPointerException`.
- The ESP32 adapter no longer reports a fabricated `SUCCESS`; with no ESP32 agent
  available it fails the attempt with `TRANSPORT_ERROR` so the task is re-placed.
- A node registered with an adapter type Core cannot resolve fails its own attempt
  and the task moves to another node, instead of the error escaping the placement cycle.
- A heartbeat without `cpuUsagePercent` is recorded as unknown (`-1`), not as 0 % load.
- Error responses from the gateway are valid JSON even when the message spans lines.

### Changed

- Request bodies on every endpoint are limited to 64 KiB; larger bodies get `413`.
- Registration requires `adapterType` and rejects a `callbackPort` outside 1-65535.
- `phoenix.maxAttempts`, `phoenix.maxRecoveryCycles` and `phoenix.noNodeWaitSeconds`
  are validated at startup.
- The gateway port is configurable with `phoenix.port` (default `8080`).
- The console's task journal keeps at most 256 tasks.
- Task bodies for both HTTP transports are built in one place (`TaskWire`), with one
  JSON escaper (`JsonText`).

### Removed

- Unused `NoEligibleNodeException`, `TaskDispatcher.dispatch(...)` and
  `TaskRecoveryManager.chooseAlternative(...)`.
- The unused `NEW` and `REGISTERING` node states; registration is synchronous.

### Build and documentation

- Compilation runs with `-Xlint:all -Werror`; the code base compiles without warnings.
- `./gradlew check` builds the Javadoc with doclint, which previously failed.
- Tagged releases are built and published by the `Release` workflow.
- README: configuration reference, operator console keys, package overview, corrected
  task body (`taskId`). Push protocol document rewritten to match the implementation.

## [0.2.0]

Core hardening milestone: placement reservations, delivery versus execution timeout
semantics, transport-neutral attempt lifecycle, in-flight recovery when a node goes
`OFFLINE`, bounded recovery with a fresh waiting window, and attempt identity fencing
negotiated per node (`attempt-id`).

[Unreleased]: https://github.com/Phoenix-Mesh/phoenix/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/Phoenix-Mesh/phoenix/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/Phoenix-Mesh/phoenix/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Phoenix-Mesh/phoenix/releases/tag/v0.2.0
