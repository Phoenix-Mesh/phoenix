# Phoenix reference agent (Linux, MQTT/TLS)

`phoenix_mqtt_agent.py` is a complete node agent for the
[MQTT/TLS protocol](../../docs/mqtt-agent-protocol.md). It runs on any Linux
machine with Python 3.9+ (a Raspberry Pi, an old laptop) and doubles as the
reference implementation for the Android and ESP32 agents: every rule in the
protocol's agent checklist is implemented here.

It executes real work: Monte-Carlo pi with the requested number of samples,
and reports real telemetry from `/proc`.

## Run

```bash
python3 -m venv ~/.phoenix-agent/venv
~/.phoenix-agent/venv/bin/pip install "paho-mqtt>=2,<3"

~/.phoenix-agent/venv/bin/python phoenix_mqtt_agent.py \
  --broker broker.lan \
  --ca ca.pem \
  --device-id linux-pi-01 \
  --password-file ~/.phoenix-agent/broker-password \
  --enrollment-token-file ~/.phoenix-agent/enrollment-token
```

| Option                     | Meaning                                                        |
| -------------------------- | -------------------------------------------------------------- |
| `--broker`, `--port`       | Broker host (must match its certificate) and port (8883)       |
| `--ca`                     | CA certificate that signed the broker certificate              |
| `--device-id`              | Default: generated once, stored in `~/.phoenix-agent/device-id` |
| `--password-file`          | Broker password (or `PHOENIX_MQTT_PASSWORD`); user name = device id |
| `--enrollment-token-file`  | Core's enrollment token (or `PHOENIX_MQTT_ENROLLMENT_TOKEN`)   |
| `--capability`             | Scheduling tag, repeatable                                     |
| `--insecure`               | Plain TCP; only for a broker on the same machine               |

The broker account comes from `deploy/mosquitto/add-device.sh`, with the
device id as the user name.

## Trying it out

The Linux node agent that is released and validated (issue #24) is the Java
agent in [phoenix-linux-agent](https://github.com/Phoenix-Mesh/phoenix-linux-agent);
its `docs/mqtt-tls-validation.md` is the full runbook. This script stays the
compact reference for the protocol. A quick check with it:

1. Start Mosquitto and Core as in [`deploy/mosquitto`](../../deploy/mosquitto/README.md).
2. Start the agent on the Linux machine. Core's event log shows
   `[MQTT] Device registered: <name>` and the node appears in **NODES**.
3. Press `D`. The agent logs `task-001 (attempt 1) -> SUCCESS`, Core logs
   `[MQTT] Result received` and the task turns green.
4. Stop the agent with `kill -9`: the node goes `DEGRADED` after 20 s and
   `OFFLINE` after 60 s; a task in flight on it is re-placed at once.
