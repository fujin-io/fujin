# Fujin

High-performance message broker gateway. Sits between your applications and message brokers (Kafka, NATS, RabbitMQ, and others), exposing a single efficient protocol and gRPC interface.

Think of it as Envoy, but for message brokers instead of HTTP.

## Why

Broker client libraries are heavy, language-specific, and tightly coupled to your application. Upgrading a Kafka client, adding metrics, or switching from RabbitMQ to NATS means changing and redeploying every service.

Fujin decouples applications from brokers. Your app talks to Fujin over a simple TCP/QUIC connection or gRPC — Fujin handles the rest. This gives you:

- **Any language, any broker.** No need for a native Kafka or NATS client in every language. If your app can open a TCP socket or call gRPC, it can produce and consume messages.
- **Centralized operations.** Observability (Prometheus, OpenTelemetry), auth (API key middleware), and broker client upgrades happen in one place — without touching application code.
- **Minimal overhead.** Zero-allocation protocol parser. TCP transport pushes ~840 MB/s on 32KB payloads through Kafka on Apple M2. The protocol layer adds negligible latency.
- **Zero-downtime deployments.** Graceful binary upgrade via FD passing (Unix). Hot config reload via SIGHUP. No dropped connections.

## Supported Brokers

| Broker | Connector |
|--------|-----------|
| Kafka | `kafka/franz` |
| NATS Core | `nats/core` |
| RabbitMQ | `rabbitmq_amqp09` |
| Azure Service Bus / ActiveMQ | `azure_amqp1` |
| Redis/Valkey (PubSub) | `resp/pubsub` |
| Redis/Valkey (Streams) | `resp/streams` |
| MQTT (EMQX, NanoMQ, etc.) | `mqtt` |
| NSQ | `nsq` |
| ZeroMQ | `zeromq` |

## Client Interfaces

**Fujin Protocol** — Custom binary protocol over TCP, QUIC, or Unix sockets. Zero-allocation parsing, transactions, headers, push and pull delivery. Best for high-throughput scenarios. Go client: [`fujin-go`](https://github.com/fujin-io/fujin-go).

**gRPC** — Standard gRPC interface. Works with any language that has a gRPC library.

### Transports

| Transport | Best for |
|-----------|----------|
| TCP | Maximum single-stream throughput. Optional TLS. |
| QUIC | Multiplexed streams, built-in TLS, connection migration. |
| Unix | Same-host IPC (sidecars, pods). Lowest latency. |

## Quick Start

```bash
# Build
make build

# Run (requires a config file)
FUJIN_CONFIGURATOR=yaml FUJIN_CONFIGURATOR_YAML_PATHS=./config.yaml ./bin/fujin
```

See [`examples/assets/config/config.yaml`](examples/assets/config/config.yaml) for a full configuration example.

## Build Options

Fujin uses build tags and a plugin system. You can build a full binary with all plugins, or a minimal one with only what you need.

```bash
# Full binary (all transports, all connectors, gRPC)
make build

# Minimal binary (only Kafka, only TCP, no gRPC)
go run ./cmd/builder \
  -transport github.com/fujin-io/fujin/public/plugins/transport/tcp \
  -configurator github.com/fujin-io/fujin/public/plugins/configurator/yaml \
  -connector github.com/fujin-io/fujin/public/plugins/connector/kafka/franz \
  -tags "fujin" \
  -output ./bin/fujin-minimal
```

Build tags: `fujin` (native protocol transports), `grpc` (gRPC server).

### Plugin System

Everything is pluggable: transports, connectors, config loaders, and middleware. Plugins self-register via `init()`. The custom binary builder (`cmd/builder`) generates a `main.go` that imports only selected plugins, keeping the binary small.

| Plugin type | Examples |
|-------------|----------|
| Transports | `tcp`, `quic`, `unix` |
| Connectors | `kafka/franz`, `nats/core`, `rabbitmq_amqp09`, ... |
| Configurators | `yaml` |
| Bind middleware | `auth_api_key` |
| Connector middleware | `prom`, `otel` |

Write your own plugins — see [`examples/plugins/`](examples/plugins/) for a complete example with a custom connector and rate-limiting middleware.

### Cross-Platform

Fujin compiles on Linux, macOS, and Windows:

```bash
GOOS=windows GOARCH=amd64 go build -tags=fujin,grpc ./...
```

On Windows, Unix-only features (Unix socket transport, SIGHUP reload, graceful binary upgrade) are unavailable. TCP, QUIC, and gRPC work normally.

## Operations

### Hot Reload

```bash
kill -HUP $(pgrep fujin)
```

Reloads connector configuration and log level from YAML. Existing connections are unaffected; new connections use updated config.

### Graceful Binary Upgrade

Zero-downtime binary replacement on Unix systems. The new process inherits listener file descriptors from the old one via SCM_RIGHTS — no connections are dropped.

```bash
# 1. Old process is running and listening on the upgrade socket

# 2. Build the new binary
make build

# 3. Start new process in upgrade mode
FUJIN_UPGRADE=1 ./bin/fujin
```

The new process connects to the old process's control socket, receives listener FDs, starts serving, signals ready, and the old process drains and exits.

Custom socket path (default: `/run/fujin/upgrade.sock`):
```bash
export FUJIN_UPGRADE_SOCK=/tmp/fujin-upgrade.sock
```

## Benchmarks

Apple M2, macOS arm64, single connection, localhost. Raw results: [`test/bench_test.txt`](test/bench_test.txt).

## Documentation

- [Native Protocol Specification](protocol.md)
- [gRPC Proto Definition](public/proto/grpc/v1/fujin.proto)
- [Configuration Example](examples/assets/config/config.yaml)

## License

MIT. See [LICENSE](LICENSE).
