# Phoenix

[![CI](https://github.com/Phoenix-Mesh/phoenix/actions/workflows/ci.yml/badge.svg)](https://github.com/Phoenix-Mesh/phoenix/actions/workflows/ci.yml)

Phoenix is a lightweight middleware and orchestration system for heterogeneous, resource-constrained edge devices.

The project explores a simple idea: instead of discarding older phones, tablets, laptops, and other devices, can we turn them into a cooperative edge-computing mesh and intelligently place work across them?

Phoenix is built around inversion of control, hardware adapters, a device registry, resource-aware scheduling, and pluggable node-agent transports.

## Current milestone

Phoenix currently has a working end-to-end path from the orchestrator to a **real Android device**:

```text
Android Agent
     |
     | REGISTER
     v
Phoenix Core
     |
     | HEARTBEAT
     v
Device Registry
     |
     v
Scheduler
     |
     | selects real Android node
     v
Task Queue
     |
     | Android polls
     v
Android Agent
     |
     | executes Monte-Carlo pi
     v
Result
     |
     | POST /results
     v
Phoenix Core
```

Remote tasks travel over one of two transports:

- **HTTP**: **pull** for Android nodes (behind NAT, they poll Core) and **push** for directly addressable Linux nodes (Core posts to them). The validated baseline (`http-node-agent-v0.2`).
- **MQTT/TLS**: every node, whatever its hardware class, keeps one outbound TLS connection to a Mosquitto broker and Core pushes work to the node's own topic. Authenticated registration, attempt-fenced results, idempotent delivery. See [docs/mqtt-agent-protocol.md](docs/mqtt-agent-protocol.md).

A node is reached through the transport it registered with; the scheduler and dispatcher do not know which one that is.

## What works as of now:

### Phoenix Core

- **Device Registry** — tracks real nodes, identity, live resource status, and lifecycle state.
- **Adapter Registry** — resolves hardware-class adapters without hard-coding concrete implementations into the dispatcher.
- **Task Dispatcher** — coordinates task submission, scheduling, execution, and result handling.
- **Greedy Cost Scheduler** — selects the currently lowest-cost eligible node.
- **Placement Cost Model** — combines execution time, energy, load, and network-related factors into a placement cost.
- **Node lifecycle monitoring** — nodes can move from `ONLINE` to `DEGRADED` when heartbeats stop and return to `ONLINE` when heartbeats resume.
- **Node-agent HTTP protocol** — real devices can register and report live state.
- **Node-agent MQTT/TLS protocol** — the same registration, telemetry and task lifecycle over an MQTT broker, with per-device broker ACLs, an enrollment token, session tokens and mandatory attempt fencing.

### Native Android Node Agent

The standalone Android application can:

- persist a stable device identity
- register itself with Phoenix
- report device metadata and memory information
- send periodic heartbeats
- poll Phoenix for assigned work
- execute Monte-Carlo pi estimation on the physical device
- send the real execution result back to Phoenix
- participate in heartbeat-based lifecycle monitoring

### End-to-end execution

A task can travel through the complete path:

```text
Phoenix dispatch
    -> Scheduler
    -> real Android node selected
    -> task queued for that node
    -> Android agent polls
    -> real computation on device
    -> result returned
    -> Phoenix completes task
```

No simulated node is required for this path.

## Architecture

```text
                         PHOENIX CORE
+------------------------------------------------------------+
|                                                            |
|  DeviceRegistry                                            |
|       |                                                    |
|       +--------------- Node state / identity               |
|       |                                                    |
|  TaskDispatcher ------> TaskScheduler                      |
|                              |                             |
|                              v                             |
|                       Placement Cost                       |
|                              |                             |
|                              v                             |
|                        Selected Node                       |
|                              |                             |
|                       Hardware Adapter                     |
|                              |                             |
|                       NodeAgentClient                      |
|                              |                             |
|                        HTTP transport                      |
|                              |                             |
+------------------------------+-----------------------------+
                               |
                               | REGISTER / HEARTBEAT
                               | TASK POLL / RESULT
                               v
                     +----------------------+
                     |    Android Agent     |
                     |                      |
                     | telemetry            |
                     | heartbeat            |
                     | task polling         |
                     | task execution       |
                     +----------------------+
```

The important transport boundary is:

```text
IHardwareAdapter
        |
        v
NodeAgentClient = NodeTransportRouter   (picks the node's transport)
        |
        +-- HttpNodeAgentClient       HTTP pull  (Android default)
        +-- HttpPushNodeAgentClient   HTTP push  (Linux default)
        +-- MqttNodeAgentClient       MQTT/TLS   (any node that registered over MQTT)
```

The scheduler and dispatcher do not know whether a node is reached through HTTP, MQTT, or another future transport. Which device is which node, and through which transport it registered, is kept in one transport-neutral table (`NodeAttachments`), so a device keeps its node identity when it moves from HTTP to MQTT.

## Node identity and lifecycle

### Device identity

A node agent keeps a persistent `deviceId`, for example:

```text
android-7fbacc0c-bb51-4230-8790-79be08e3a18b
```

This identity belongs to the agent/device.

### Phoenix node identity

Phoenix assigns its own `nodeId` when the device registers. Phoenix remains authoritative over the node registry while the device keeps a stable identity across reconnects.

### Lifecycle

```text
              heartbeat
        +------------------+
        |                  v
      ONLINE ----------> ONLINE
        |
        | heartbeat timeout
        v
    DEGRADED
        |
        | heartbeat resumes
        v
      ONLINE
```

## Node Agent HTTP protocol

The current experimental transport uses the following HTTP operations.

### Register

`POST /register`

Example:

```json
{
    "deviceId": "android-<stable-id>",
    "displayName": "samsung-SM-S916B",
    "hardwareClass": "ANDROID",
    "architecture": "arm64-v8a",
    "cpuCores": 8,
    "memoryMb": 7072,
    "adapterType": "android",
    "capabilities": []
}
```

Phoenix responds with its assigned node identity.

### Heartbeat

`POST /heartbeat`

Example:

```json
{
    "deviceId": "android-...",
    "cpuUsagePercent": -1.0,
    "availableRamMb": 1878,
    "activeTasks": 0,
    "queueLength": 0,
    "networkLatencyMs": 40.0,
    "energyCostEstimate": 3.0
}
```

`-1` means the platform cannot provide trustworthy CPU telemetry. Phoenix should not treat a fabricated number as a measurement; an omitted `cpuUsagePercent` is treated the same way.

### Task polling

`GET /tasks/poll?deviceId=android-...`

When work is waiting:

```json
{
    "taskId": "task-001",
    "type": "Monte-Carlo-Pi",
    "samples": 2000000
}
```

Nodes that announced `attempt-id` also receive `attemptId` and `attemptNumber`
(see [Attempt identity and fencing on the wire](#attempt-identity-and-fencing-on-the-wire)).

When no work is waiting, the normal response is `204 No Content`.

### Result submission

`POST /results`

Example:

```json
{
    "taskId": "task-001",
    "status": "SUCCESS",
    "outputData": "Computed Monte-Carlo pi ~= 3.1417",
    "executionTimeMs": 230
}
```

A result is accepted only for a task whose current attempt has actually been
delivered to a node. A late result for an attempt Phoenix already gave up on
(or re-queued elsewhere) is answered with `404 unknown_task`.

### Execution start acknowledgement (optional)

`POST /tasks/started`

```json
{ "taskId": "task-001" }
```

Sent by the agent right after it begins executing a task it polled. It is
**optional**: agents that never send it keep working, and Phoenix simply
reports the start as unacknowledged. Returns `404 unknown_task` for a task
that has not been delivered.

## Task lifecycle and timeout semantics

Phoenix no longer uses one end-to-end timer from queue submission to result.
A timeout can mean three different things, and Core now tells them apart
through a transport-neutral lifecycle contract (see `NodeAgentClient`):

```text
ACCEPTED -> DELIVERED -> STARTED -> COMPLETED
    \            \           \
     +------------+-----------+--> FAILED
```

| Situation                                     | Failure reason      | Re-placed? | Node marked `DEGRADED`? |
| --------------------------------------------- | ------------------- | ---------- | ----------------------- |
| Never delivered, node still polling (backlog) | `DELIVERY_TIMEOUT`  | Yes        | **No**                  |
| Never delivered, node not in contact          | `NODE_UNREACHABLE`  | Yes        | Yes                     |
| Delivered, no result in time                  | `EXECUTION_TIMEOUT` | Yes        | Yes                     |
| Transport that cannot tell the phases apart   | `TIMEOUT`           | Yes        | Yes (conservative)      |

Ordinary queue backlog on a healthy node is therefore no longer treated as
evidence that the node failed, while genuinely unreachable or silent nodes
are detected exactly as before. Node health itself is still owned by
heartbeat monitoring (`NodeHealthMonitor`); task timeouts only influence it
when the failure actually implicates the node.

Tunable system properties (seconds):

| Property                         | Default | Meaning                                                              |
| -------------------------------- | ------- | -------------------------------------------------------------------- |
| `phoenix.deliveryTimeoutSeconds` | 60      | Max time a task may wait for the node to take it                     |
| `phoenix.taskTimeoutSeconds`     | 12      | Max time **after delivery** for a result (was end-to-end before #28) |
| `phoenix.contactLivenessSeconds` | 15      | How recently a node must have polled to count as "in contact"        |

### Lifecycle contract: how an attempt ends

Every transport ends an attempt the same way:

| What happened                                    | Lifecycle state | `TaskFailureReason`     |
| ------------------------------------------------ | --------------- | ----------------------- |
| Node reported `SUCCESS`                          | `COMPLETED`     | `NONE`                  |
| Node reported a failure                          | `FAILED`        | `EXECUTION_ERROR`       |
| Never delivered, node still in contact           | `FAILED`        | `DELIVERY_TIMEOUT`      |
| Never delivered, node silent                     | `FAILED`        | `NODE_UNREACHABLE`      |
| Delivered, no result in time                     | `FAILED`        | `EXECUTION_TIMEOUT`     |
| Transport broke                                  | `FAILED`        | `TRANSPORT_ERROR`       |
| Node declared `OFFLINE`                          | `ABANDONED`     | `NODE_OFFLINE`          |
| Core's own transport link was down (MQTT broker) | `FAILED`        | `TRANSPORT_UNAVAILABLE` |

`COMPLETED` therefore always means "the node succeeded". `TRANSPORT_UNAVAILABLE`
is re-placed but never held against the node: the failure was on Core's side. The mapping from a
node-reported result lives in one place,
`TaskLifecycleService.reported(attempt, result)`.

### Node `OFFLINE` and in-flight tasks

Timeouts decide what to do when a node is merely _slow_. A node that
`NodeHealthMonitor` declares **OFFLINE** is different: Core has decided it is
gone, so it stops waiting for it.

```text
ONLINE --(20s no heartbeat)--> DEGRADED --(60s no heartbeat)--> OFFLINE
                                  |                                |
                     suspicion only: nothing                 decision: every
                     in flight is touched                    in-flight attempt on
                                                             the node is abandoned
```

When a node goes `OFFLINE`, every attempt bound to it that has not finished
(queued, delivered or executing) is **abandoned** immediately instead of
waiting for its delivery/execution timeout. The blocked transport wakes up and
returns `NODE_OFFLINE`; the unchanged dispatcher then releases the placement
reservation and re-places the task through the same `TaskAdmission` boundary as
every other recovery.

| Aspect                        | Behaviour                                                                                                        |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Trigger                       | `OFFLINE` only. `DEGRADED` never cancels work: the node may still be running it                                  |
| Retry                         | A **new attempt starts from the beginning** with its own fresh delivery/execution timeouts. No resume/checkpoint |
| Priority                      | No special scheduler. The task keeps its FIFO place in `PendingTaskQueue` and competes through normal admission  |
| Other nodes                   | Untouched. Only attempts bound to the OFFLINE node are abandoned                                                 |
| Single node                   | The task waits in `PendingTaskQueue` with a **fresh** `phoenix.noNodeWaitSeconds` window                         |
| Node health                   | `NODE_OFFLINE` is derived from node health, so it is never fed back into it                                      |
| Outcome of the old attempt    | `ABANDONED` = _unknown_. OFFLINE does not prove the computation stopped                                          |
| Late result from the old node | Rejected (`404`); the abandoned task is also withdrawn from the node's queue                                     |

Lifecycle with the new terminal state:

```text
ACCEPTED -> DELIVERED -> STARTED -> COMPLETED
    \            \           \
     +------------+-----------+--> FAILED      (known failure)
     +------------+-----------+--> ABANDONED   (outcome unknown; node OFFLINE)
```

#### Recovery window and budget

A task returned to waiting after a **failed attempt** gets a _fresh_
`phoenix.noNodeWaitSeconds` window (time spent at a node that then failed is not
time without a node) and keeps its place in line. A task that merely finds no
node on a recovery tick keeps its original window, so genuinely unplaceable
tasks still expire. Because a fresh window would otherwise allow endless
bouncing, failure-driven returns to waiting are budgeted:

| Property                    | Default | Meaning                                                          |
| --------------------------- | ------- | ---------------------------------------------------------------- |
| `phoenix.maxRecoveryCycles` | 3       | Failed-attempt cycles a task may go through before Core gives up |

An expired task is given up only after one fresh placement attempt also found no
eligible node.

#### Attempt identity and fencing on the wire

Core tracks every execution attempt internally. To reject a late message from
an old attempt even after the task was re-delivered to _another_ node (or after
a Core restart reused the same task id), the node has to echo an identity back.
Whether it can is **negotiated per node at registration**:

```json
POST /register
{ "deviceId": "...", "displayName": "...", "capabilities": [],
  "protocolFeatures": ["attempt-id"] }
```

| Node announced   | Fencing mode | What Core does                                                                                             |
| ---------------- | ------------ | ---------------------------------------------------------------------------------------------------------- |
| `attempt-id`     | `ENFORCED`   | Sends `attemptId` with each task; **rejects** any `/tasks/started` or `/results` without the right id      |
| nothing (legacy) | `LEGACY`     | Sends no extra fields (the agent never sees an unknown field); accepts reports without an id, **unfenced** |

```json
{
    "taskId": "task-001",
    "type": "Monte-Carlo-Pi",
    "samples": 2000000,
    "attemptId": "3f6c1b7e-9a1d-4c52-8a0e-5d2b7f61c9aa",
    "attemptNumber": 2
}
```

```json
{ "taskId": "task-001", "attemptId": "3f6c1b7e-9a1d-4c52-8a0e-5d2b7f61c9aa", "status": "SUCCESS", "outputData": "..." }
```

| Field           | Role                                                                                                             |
| --------------- | ---------------------------------------------------------------------------------------------------------------- |
| `attemptId`     | The identity: an opaque, globally unique string. Agents must echo it back **verbatim** and must not interpret it |
| `attemptNumber` | 1, 2, 3 ... for the task. Informational only; Core never uses it to match a report                               |

Task ids restart at `task-001` every time Core starts and attempt numbers
restart at 1 for every task, so neither is unique; the `attemptId` is.
`protocolFeatures` is separate from `capabilities` (scheduling tags), unknown
feature names are ignored, and registering again replaces the announced
features (a downgraded agent is treated as legacy again).

**What is and is not guaranteed.** The mode belongs to the _current attempt_:

- `ENFORCED` current attempt: a late report from any earlier attempt is
  rejected, whether it carries an old id or none. A legacy agent's zombie
  result therefore cannot complete a retry that runs on an upgraded node.
- `LEGACY` current attempt: a report with a **wrong** id is still rejected, but
  a report with **no** id cannot be told apart from the current attempt's.
  This is the one remaining gap; it exists only while an agent has not been
  upgraded.

Legacy agents are visible: Core logs a `WARNING ... LEGACY agent` at
registration, a `WARNING accepted UNFENCED result` for every result it accepts
without an id, and counts them (`TaskQueueService.unfencedResultCount()`). The
push transport is fenced by the HTTP connection itself (the result is the
response to that very request); it receives the id for the node's own logs.

To forbid the gap outright, start Core with `-Dphoenix.requireAttemptId=true`:
agents that do not announce `attempt-id` are then refused at registration
(`400 attempt_id_required`). It is **off by default**.

Over MQTT there is no legacy mode: every attempt is fenced and agents must
echo the attempt id ([details](docs/mqtt-agent-protocol.md#tasks)).

## Why HTTP pull was used first

For the first milestone, pull was chosen because the Android device reliably initiates outbound connections:

```text
Android  ------>  Phoenix
```

This avoids depending on the laptop being able to open a new inbound connection to the phone across hotspot/NAT/firewall configurations.

The scheduler still performs the orchestration decision:

```text
Scheduler
   |
   v
select node X
   |
   v
queue task for node X
```

The Android agent only controls how it retrieves that already-assigned work.
MQTT keeps the outbound-only connection and adds push delivery through the broker.

## MQTT/TLS transport

Nodes connect out to a Mosquitto broker over TLS and Core pushes each task to
the node's own topic. Phones behind NAT, Linux machines and ESP32 boards all
use the same path, and no node needs to be reachable from Core.

```text
                 Phoenix Core
                      |  loopback (same control node) or TLS
                      v
                 MQTT broker  ── TLS :8883, one account and ACL per device
                 /    |    \
          Android   Linux   ESP32 (TLS or TLS-PSK)
```

| Concern            | How                                                                                                                                                  |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Delivery           | `down/task` (QoS 1); the node acknowledges on `up/ack`, which is the delivery boundary, so delivery and execution timeouts mean the same as for HTTP |
| Result correlation | every MQTT attempt is fenced: a report must echo the attempt id, come from the device the attempt was sent to, and carry the current session token   |
| Idempotence        | duplicates (QoS 1 is at-least-once) change nothing on either side; agents never run one attempt twice                                                |
| Cancellation       | `down/cancel` when Core stops waiting (timeout, node `OFFLINE`)                                                                                      |
| Authentication     | TLS, a broker account per device, an ACL confining each device to its own topics, an optional enrollment token, a session token per registration     |
| Liveness           | the same heartbeats feed the same `NodeHealthMonitor`; Core and nodes announce presence with retained status messages and last wills                 |
| Core restarts      | a new `coreInstanceId` on `core/status` tells agents to register again                                                                               |

The transport is off unless `phoenix.mqtt.url` is set. Details:

- Wire protocol and agent rules: [docs/mqtt-agent-protocol.md](docs/mqtt-agent-protocol.md)
- Broker configuration, certificates and accounts: [deploy/mosquitto](deploy/mosquitto/README.md)
- Linux node agent (Java, MQTT/TLS and HTTP push): [phoenix-linux-agent](https://github.com/Phoenix-Mesh/phoenix-linux-agent)
- Android node agent (MQTT/TLS and HTTP pull): [phoenix-android-agent](https://github.com/Phoenix-Mesh/phoenix-android-agent)
- Reference Linux agent in Python: [agents/python](agents/python/README.md)

## Scheduling

The current scheduler is intentionally simple:

$$
n^* = \arg\min_{n \in E(t)} C(n,t)
$$

where `E(t)` is the set of eligible nodes and `C(n,t)` is the placement cost for node `n`.

The current cost model combines dimensions including:

- estimated execution time
- energy cost
- current load
- network latency

The present implementation is a **greedy argmin heuristic**. It is the practical baseline we can measure on real heterogeneous hardware before introducing a more sophisticated optimizer.

## Research direction

The intended progression is:

```text
Real node measurements
        |
        v
Greedy scheduling baseline
        |
        v
Normalized / calibrated cost model
        |
        v
Heterogeneous workload experiments
        |
        v
Advanced placement optimization
        |
        v
Memetic / research-backed scheduling comparison
```

The goal is to evaluate advanced scheduling against measured behavior rather than implement an optimization algorithm in isolation.

## Current status

| Component                                            | Status                        |
| ---------------------------------------------------- | ----------------------------- |
| IoC / Guice adapter architecture                     | Working                       |
| Device registry                                      | Working                       |
| Greedy scheduler                                     | Working                       |
| Task dispatcher                                      | Working                       |
| Node registration                                    | Working                       |
| Heartbeat monitoring                                 | Working                       |
| `ONLINE -> DEGRADED -> ONLINE` lifecycle             | Working                       |
| Native Android node agent                            | Working                       |
| Android live telemetry                               | Working                       |
| Android task execution                               | Working                       |
| HTTP task polling                                    | Working                       |
| Result return to Phoenix                             | Working                       |
| Linux native/remote agent                            | Working (MQTT/TLS, HTTP push) |
| Failure-aware task rescheduling                      | Working                       |
| Delivery vs execution timeout semantics              | Working                       |
| In-flight recovery on node OFFLINE                   | Working                       |
| Attempt fencing (`attempt-id`)                       | Working                       |
| MQTT transport                                       | Working                       |
| TLS-secured transport                                | Working                       |
| Per-device broker ACL, enrollment and session tokens | Working                       |
| ESP32 execution over MQTT (Core side)                | Working                       |
| ESP32 real execution agent (firmware)                | Future                        |
| Memetic scheduling                                   | Future                        |

## Repository layout

| Package     | Responsibility                                                                        |
| ----------- | ------------------------------------------------------------------------------------- |
| `adapter`   | `IHardwareAdapter` and the per-hardware-class adapters                                |
| `core`      | Registry, health monitoring, attempt lifecycle, timeouts, reservations, Guice wiring  |
| `dispatch`  | Admission, placement cycles and the pending (waiting) queue                           |
| `model`     | Immutable domain records and enums                                                    |
| `net`       | HTTP gateway, pull and push transports, wire format, transport routing                |
| `net.mqtt`  | MQTT/TLS gateway, agent client, broker link (Eclipse Paho), sessions, attempt fencing |
| `recovery`  | Per-cycle retry policy                                                                |
| `scheduler` | Placement policy and cost model                                                       |
| `tui`       | Operator console                                                                      |

Protocol details for push agents live in [`docs/linux-agent-protocol.md`](docs/linux-agent-protocol.md),
for MQTT agents in [`docs/mqtt-agent-protocol.md`](docs/mqtt-agent-protocol.md).
Broker configuration lives in [`deploy/mosquitto`](deploy/mosquitto/README.md) and the
reference MQTT agent in [`agents/python`](agents/python/README.md).
Release history is in [`CHANGELOG.md`](CHANGELOG.md).

### Node agents

Each agent lives in its own repository, with its own releases.

| Agent             | Repository                                                                         | Transports                     |
| ----------------- | ---------------------------------------------------------------------------------- | ------------------------------ |
| Android           | [phoenix-android-agent](https://github.com/Phoenix-Mesh/phoenix-android-agent)     | MQTT/TLS, HTTP pull            |
| Linux             | [phoenix-linux-agent](https://github.com/Phoenix-Mesh/phoenix-linux-agent)         | MQTT/TLS, HTTP push            |
| Linux (reference) | [`agents/python`](agents/python/README.md)                                         | MQTT/TLS                       |

## Running the Core

Build and verify first (compiles with lint as errors, runs the tests and builds the Javadoc):

```bash
./gradlew clean build
```

Start Phoenix using the LAN address reachable by your node devices:

```bash
./gradlew --console=plain \
  -Dphoenix.advertiseAddress=<YOUR_LAN_IP> \
  run
```

For example:

```bash
./gradlew --console=plain \
  -Dphoenix.advertiseAddress=192.0.2.10 \
  run
```

Phoenix exposes the node-agent API at:

```text
http://<YOUR_LAN_IP>:8080
```

To accept MQTT/TLS agents as well, run a Mosquitto broker next to Core and
point Core at it; step-by-step in [deploy/mosquitto](deploy/mosquitto/README.md):

```bash
./gradlew --console=plain run \
  -Dphoenix.advertiseAddress=<YOUR_LAN_IP> \
  -Dphoenix.mqtt.url=tcp://127.0.0.1:1883 \
  -Dphoenix.mqtt.username=phoenix-core \
  -Dphoenix.mqtt.passwordFile=$HOME/.phoenix/core-broker-password \
  -Dphoenix.mqtt.enrollmentTokenFile=$HOME/.phoenix/enrollment-token
```

Any `-Dphoenix.*` property given to `./gradlew run` is forwarded to Core (see [Configuration](#configuration)).

### Operator console

Core starts a terminal dashboard showing nodes, recent tasks and the event log.
With MQTT configured, the header shows `MQTT ●` (connected) or `MQTT ○` (connecting).

| Key                     | Action                         |
| ----------------------- | ------------------------------ |
| `D`                     | Submit a Monte-Carlo-Pi task   |
| `C`                     | Clear the event log            |
| `H`                     | Show the key reference         |
| `↑` `↓` / `PgUp` `PgDn` | Scroll the event log           |
| `G`                     | Jump back to the latest events |
| `Q` / `Esc`             | Stop Phoenix Core              |

### Configuration

All settings are JVM system properties.

| Property                         | Default     | Meaning                                                         |
| -------------------------------- | ----------- | --------------------------------------------------------------- |
| `phoenix.advertiseAddress`       | `127.0.0.1` | Address shown in the console for agents to connect to           |
| `phoenix.port`                   | `8080`      | Port of the node-agent HTTP gateway                             |
| `phoenix.deliveryTimeoutSeconds` | `60`        | Max time a task may wait for a pull node to take it             |
| `phoenix.taskTimeoutSeconds`     | `12`        | Max time after delivery for a result                            |
| `phoenix.contactLivenessSeconds` | `15`        | How recently a node must have polled to count as in contact     |
| `phoenix.maxAttempts`            | `3`         | Nodes one placement cycle may try (at least 1)                  |
| `phoenix.noNodeWaitSeconds`      | `60`        | How long a task may wait with no eligible node (positive)       |
| `phoenix.maxRecoveryCycles`      | `3`         | Failed-attempt cycles before a task is given up (at least 1)    |
| `phoenix.requireAttemptId`       | `false`     | Refuse registration of agents that do not announce `attempt-id` |

MQTT/TLS transport (off unless `phoenix.mqtt.url` is set):

| Property                                | Default         | Meaning                                                                                                            |
| --------------------------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------ |
| `phoenix.mqtt.url`                      | (off)           | `ssl://host:8883`, or `tcp://127.0.0.1:1883` for a broker on this machine                                          |
| `phoenix.mqtt.allowInsecure`            | `false`         | Allow `tcp://` to a broker that is not on this machine                                                             |
| `phoenix.mqtt.clientId`                 | `phoenix-core`  | Core's MQTT client id                                                                                              |
| `phoenix.mqtt.topicPrefix`              | `phoenix`       | First level of the topic namespace (`phoenix/v1/...`)                                                              |
| `phoenix.mqtt.username`                 | (none)          | Broker user name                                                                                                   |
| `phoenix.mqtt.passwordFile`             | (none)          | File with the broker password (or `PHOENIX_MQTT_PASSWORD`)                                                         |
| `phoenix.mqtt.tls.caFile`               | JDK trust store | PEM CA certificate(s) for the broker                                                                               |
| `phoenix.mqtt.tls.verifyHostname`       | `true`          | Require the broker certificate to match the host name (`true` or `false`; anything else is rejected)               |
| `phoenix.mqtt.tls.keyStore`             | (none)          | PKCS#12 client certificate for brokers that require one                                                            |
| `phoenix.mqtt.tls.keyStorePasswordFile` | (none)          | Its password (or `PHOENIX_MQTT_KEYSTORE_PASSWORD`)                                                                 |
| `phoenix.mqtt.enrollmentTokenFile`      | (none)          | Token nodes must present to register (or `PHOENIX_MQTT_ENROLLMENT_TOKEN`); open enrollment with a warning if unset |
| `phoenix.mqtt.keepAliveSeconds`         | `30`            | MQTT keep-alive                                                                                                    |
| `phoenix.mqtt.connectTimeoutSeconds`    | `10`            | Timeout of one connection attempt                                                                                  |
| `phoenix.mqtt.heartbeatIntervalSeconds` | `5`             | Heartbeat interval Core advises nodes to use                                                                       |

Secrets are read from files or environment variables, never from `-D`
properties, which every local user can see in the process list.

Invalid values stop Core at startup with a message naming the property.
Request bodies on every endpoint are limited to 64 KiB (`413` above that).

## Running the Android Agent

Clone [phoenix-android-agent](https://github.com/Phoenix-Mesh/phoenix-android-agent) and open it in Android Studio.

Build:

```bash
./gradlew clean assembleDebug
```

Install onto a connected device:

```bash
./gradlew installDebug
```

The device and Phoenix Core must be able to reach each other over the chosen network.

In the Android app:

1. Enter the Core URL, for example `http://192.0.2.10:8080`.
2. Connect to Phoenix.
3. Wait for registration and heartbeat activity.
4. Confirm the device appears in the console's **NODES** panel.
5. Press `D` to dispatch a task.

A successful run shows a real task executed on the Android device and a real result returned to Phoenix.

## Running the Linux Agent

Download `phoenix-linux-agent-v<version>.jar` from the
[phoenix-linux-agent releases](https://github.com/Phoenix-Mesh/phoenix-linux-agent/releases/latest)
and run it on the Linux machine (Java 11 or newer).

Over MQTT/TLS (broker account and `ca.pem` from [deploy/mosquitto](deploy/mosquitto/README.md);
the broker user name is the id printed by `--print-device-id`):

```bash
java -jar phoenix-linux-agent-v0.3.0.jar --broker <broker-host-or-ip> --ca ca.pem \
    --password-file ~/.phoenix/broker-password \
    --enrollment-token-file ~/.phoenix/enrollment-token
```

Over HTTP push (Core must reach the machine on the callback port):

```bash
java -jar phoenix-linux-agent-v0.3.0.jar http://192.0.2.10:8080 9090
```

The node appears in the console's **NODES** panel; press `D` to dispatch a task.
Setup, troubleshooting and the validation runbook are in the agent's
[docs](https://github.com/Phoenix-Mesh/phoenix-linux-agent/tree/main/docs).

## Design principles

- **Real nodes are authoritative** — Production Core does not manufacture a fake fleet. Nodes enter the registry by registering with Phoenix.

- **Hardware details stay behind adapters** — Core orchestration depends on `IHardwareAdapter`, not directly on concrete hardware implementations.

- **Transport is replaceable** — HTTP (pull and push) and MQTT/TLS share one lifecycle contract, one timeout policy and one node identity table; adding a transport touches neither the scheduler nor the dispatcher.

- **Scheduling is separate from execution** — The scheduler chooses a node. Adapters and node-agent transports perform execution.

- **Measure before optimizing** — The greedy scheduler is the current baseline. More advanced placement algorithms should be evaluated against real heterogeneous-device measurements.

## Milestone boundary

Phoenix has moved beyond a skeleton. It can now:

```text
real device
    |
    v
register
    |
    v
heartbeat
    |
    v
be considered by scheduler
    |
    v
receive assigned work
    |
    v
execute real computation
    |
    v
return real result
```

The same model now also runs over a secure, message-oriented MQTT/TLS transport without changes to the orchestration abstractions, validated end to end with real Android and Linux agents. The next steps are ESP32 firmware and the scheduling research below.
