# Operations

**Use this page to:** monitor a running Fujin process, reload connector configuration safely, stop or replace the process, and choose a supported deployment shape. Configure the service first with [Configuration](configuration.md).

## Health checks

Enable the dedicated HTTP listener:

```yaml
health:
  enabled: true
  addr: 0.0.0.0:8080
```

Probe it with:

```bash
curl --fail http://127.0.0.1:8080/healthz
curl --fail http://127.0.0.1:8080/readyz
```

| Endpoint | Meaning |
| --- | --- |
| `GET /healthz` | Liveness. Returns `200` while the process and health listener are running. |
| `GET /readyz` | Readiness. Returns `200` only after every configured native, gRPC, health, and other listener has bound; otherwise `503`. |

When gRPC is enabled, the standard `grpc.health.v1.Health` service reports `fujin.v1.FujinService` as serving while that listener is active.

Readiness is intentionally **not** a broker health check. Connector compilation does no broker I/O, and Kafka and NATS clients open lazily on first route use. A ready Fujin instance can therefore still report an authentication, DNS, or broker error on its first operation. Monitor those operation failures separately.

## Connector reload

On Unix, change the selected file or other startup configurator source, then send SIGHUP:

```bash
kill -HUP <fujin-pid>
```

For example, when exactly one Fujin process is expected:

```bash
kill -HUP "$(pgrep fujin)"
```

SIGHUP performs two actions:

1. reloads `FUJIN_LOG_LEVEL`;
2. asks the retained startup configurator for a complete document, then applies only its complete `connectors` snapshot.

The new connector snapshot is fully compiled before publication. A load, validation, or compile error is logged and leaves the active generation unchanged. An unchanged snapshot is reported in logs with `changed=false`.

Listener sections are bootstrap-only: SIGHUP does not update `fujin.transports`, `grpc`, or `health`. Restart or perform a graceful binary upgrade for those changes. A configurator with its own live connector watcher owns connector state; in that case SIGHUP reloads logging but does not compete with the watcher. The built-in `file` and `env` configurators are startup-only.

### Generation pinning

A successful `BIND` pins that session to one immutable connector generation. After a reload:

- existing bound sessions continue on their original generation;
- a later `BIND` sees the newly published generation;
- a rejected replacement does not disturb either group.

This avoids changing route capabilities or broker clients underneath an active session. Plan a client reconnect when every session must move to the new settings. The authoritative BIND and generation semantics are in the [native protocol specification](../protocol.md#bind); the same Session Core rule applies to gRPC.

## Shutdown

Use SIGTERM in a service manager or container, or Ctrl-C in an interactive terminal:

```bash
kill -TERM <fujin-pid>
```

Fujin stops its listeners, drains session tasks, and closes the connector catalog. The supplied container declares `STOPSIGNAL SIGTERM`; allow the process to finish rather than sending SIGKILL immediately. Native listener `settings.fujin.force_terminate_timeout` controls the final bound on graceful STOP termination for a native session; see the [shared transport settings](../crates/fujin-transport/src/settings.rs).

## Graceful binary upgrade

The standard Unix application enables listener handoff. Start the replacement binary on the same host with the same listener addresses and transport types:

```bash
FUJIN_CONFIGURATOR=file \
FUJIN_CONFIGURATOR_FILE_PATHS=/etc/fujin/config.yaml \
FUJIN_UPGRADE=1 \
FUJIN_UPGRADE_SOCK=/run/fujin/upgrade.sock \
  /path/to/new/fujin
```

`FUJIN_UPGRADE_SOCK` defaults to `/run/fujin/upgrade.sock`. The replacement requests the old process's TCP, WebSocket, gRPC, health, Unix, and QUIC listener descriptors through `SCM_RIGHTS`, starts all configured listeners, reapplies TLS in the new process, and announces readiness. Only then does the old process stop accepting and drain its existing sessions. If the replacement fails or cannot inherit the complete listener set, the old process keeps serving.

Requirements and limits:

- both processes must run on the same Unix host and be able to access the control socket;
- their configured listener addresses and transport types must be compatible;
- Windows supports ordinary operation but not descriptor handoff;
- the shipped container creates writable `/run/fujin` for its numeric non-root user, but an orchestrator must still arrange for old and new processes to overlap and share that socket.

For a normal container or Kubernetes rollout, use the orchestrator's replacement lifecycle unless you have explicitly built that same-host overlap. Listener handoff is not a substitute for configuring readiness probes and termination grace periods.

## Deployment

### Local complete stack

For development, use the tested Kafka + Fujin composition from [Getting started](getting-started.md). It supplies brokers, certificates, connector routes, client examples, and health checks; it is more complete than launching the Fujin image alone.

### Docker image

Build the default deployment image from the repository root:

```bash
docker build --build-arg VERSION=v0.6.0 -t fujin .
```

The default image composition in [`deploy/docker/fujin.build.toml`](../deploy/docker/fujin.build.toml) links `configurator-file`, `connector-kafka`, and `transport-tcp`. Mount a configuration and publish its configured ports:

```bash
docker run --rm \
  -p 4850:4850 -p 8080:8080 \
  -v "$PWD/config.yaml:/config/config.yaml:ro" \
  fujin
```

The image reads `/config/config.yaml`, runs as numeric user `65532`, and has a `scratch` final stage containing no shell. It includes the Fujin binary, CA bundle, and writable `/run/fujin`; use logs and health endpoints rather than shell-based container diagnostics.

A custom image composition can select other built-in transports or the NATS JetStream connector through `FUJIN_BUILD_MANIFEST`; follow the root [Docker deployment reference](../README.md#docker) and [static composition guide](extending.md#compose-with-cargo-fujin). The default image links Kafka only: configuring `type: nats` requires a custom build manifest linking `fujin-connector-nats`. Mount `.creds` files as read-only secrets at a path readable by numeric user `65532`; do not put credential contents in a ConfigMap. Configured plugin types must match what that manifest statically links.

### Helm

Install the chart in standalone mode:

```bash
helm install fujin ./deploy/helm/fujin
```

Standalone mode creates a `Deployment`, `Service`, and configuration `ConfigMap`, with `/healthz` and `/readyz` probes. The default values contain `connectors: {}`; provide broker connector settings under `config.connectors` and use an image that includes the corresponding plugin. Provision JetStream streams/consumers separately and mount NATS credentials as a Secret, not chart configuration. Review all image, replica, port, probe, resource, scheduling, and service settings in [`values.yaml`](../deploy/helm/fujin/values.yaml).

Sidecar mode is selected with:

```bash
helm install fujin ./deploy/helm/fujin --set mode=sidecar
```

This mode creates the configuration `ConfigMap` and exposes reference helper templates; it does not add a sidecar to an existing workload by itself. Include the chart's sidecar container and volume helpers in the owning workload as described by the [chart guide](../deploy/helm/fujin/README.md).

The standalone pod template hashes the rendered ConfigMap, so a Helm upgrade that changes `config` changes the pod template and triggers a Kubernetes rollout. Treat that as a restart of all bootstrap settings, not as SIGHUP connector reload.

## Logs and observability

Fujin writes tracing events to standard output/error through `tracing-subscriber`.

| Variable | Values | Behavior |
| --- | --- | --- |
| `FUJIN_LOG_LEVEL` | `DEBUG`, `INFO`, `WARN`, `ERROR` | Selects the runtime filter; unset or unrecognized values use `INFO`. On Unix, SIGHUP reloads it. |
| `FUJIN_LOG_TYPE` | `json` | Selects structured JSON; any other or absent value uses text formatting. |

Use JSON in containerized environments:

```bash
FUJIN_LOG_LEVEL=INFO FUJIN_LOG_TYPE=json ./bin/fujin
```

Use `/readyz` to confirm listener readiness; reload logs report `revision` and `changed`, while errors identify failed listener, connector, or broker operations. The shipped process does not expose a Prometheus metrics endpoint or a standalone connector-status administration endpoint. Use `/healthz`, `/readyz`, gRPC health, structured logs, and client operation outcomes; do not infer broker health from readiness.

For client-visible error outcomes and retry implications, use the [client interface guide](interfaces.md) and the authoritative [protocol error model](../protocol.md#error-encoding).
