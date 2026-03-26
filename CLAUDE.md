# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Fujin is a high-performance, unified message broker connector written in Go. It provides a single binary protocol (zero-allocation parsing) and gRPC interface to bridge applications with any message broker (Kafka, NATS, RabbitMQ, Azure Service Bus, Redis, MQTT, NSQ, ZeroMQ).

Module: `github.com/fujin-io/fujin` | Go 1.25.1

## Build & Run

```bash
make build                    # Build with all plugins (tags: fujin,grpc)
make run                      # Run binary (sets FUJIN_CONFIGURATOR=yaml)
make build GO_BUILD_TAGS="fujin,grpc" CONNECTORS=github.com/fujin-io/fujin/public/plugins/connector/kafka/franz
```

Custom minimal binary via builder:
```bash
go run ./cmd/builder -local -transport .../tcp -connector .../kafka/franz -configurator .../yaml -tags "fujin" -output ./bin/fujin
```

## Testing

```bash
make test                                                # All tests with build tags
go test -v -tags=fujin,grpc ./...                        # Same, explicit
go test -v -tags=fujin,grpc ./internal/common/pool/      # Single package
go test -v -tags=fujin,grpc -run TestSpecificName ./...  # Single test
go test -race -tags=fujin,grpc ./...                     # Race detector
```

## Benchmarks

```bash
make bench                                                    # Default: Produce 32KB Nop TCP, 1M iterations
make bench BENCH_FUNC=Benchmark_Produce_1KBPayload_Nop_QUIC  # Specific benchmark
go test -bench=. -benchmem -tags=fujin,grpc ./test            # All benchmarks with memory stats
```

## Broker Infrastructure

```bash
make up-kafka_franz       # Start Kafka + observability stack
make down-kafka_franz     # Stop
# Also: up/down-nats_core, up/down-rabbitmq_amqp09, up/down-azure_amqp1,
#        up/down-mqtt_paho, up/down-redis_rueidis_pubsub, up/down-redis_rueidis_streams, up/down-nsq
```

## Protobuf Generation

```bash
make generate   # Regenerates gRPC code from public/proto/grpc/v1/fujin.proto
```

## Architecture

### Build Tags

Three build tag groups control compilation:
- `fujin` — enables native protocol transports (TCP, QUIC, Unix)
- `grpc` — enables gRPC server
- `unix` (implicit) — Unix-only features: SIGHUP hot reload, graceful binary upgrade (FD passing via SCM_RIGHTS), Unix socket transport. These use `//go:build unix` and compile automatically on Linux/macOS/BSD. On Windows, stubs provide no-op implementations.

Files use `//go:build fujin` / `//go:build !fujin` and `//go:build unix` / `//go:build !unix` patterns for conditional compilation.

### Cross-Platform

The project compiles on all platforms including Windows:
```bash
GOOS=windows GOARCH=amd64 go build -tags=fujin,grpc ./...
```
On Windows: Unix socket transport is disabled, SIGHUP reload is unavailable, binary upgrade is unavailable. TCP, QUIC, and gRPC transports work normally.

### Plugin System

Everything is pluggable via a registry pattern: plugins self-register in `init()`, the server resolves them by name at runtime. Five plugin types:

1. **Transports** (`public/plugins/transport/`) — TCP, QUIC, Unix socket servers
2. **Connectors** (`public/plugins/connector/`) — broker implementations providing Reader/Writer interfaces
3. **Configurators** (`public/plugins/configurator/`) — config loading (YAML)
4. **Bind Middleware** (`public/plugins/middleware/bind/`) — auth at connection time (API key)
5. **Connector Middleware** (`public/plugins/middleware/connector/`) — wraps Reader/Writer (Prometheus, OpenTelemetry)

Each plugin type has an `all/` package that imports every implementation. The `cmd/main.go` imports all `all/` packages. The `cmd/builder/` generates minimal binaries with only selected plugins.

### Request Flow

1. Client connects via transport (TCP/QUIC/Unix) → `internal/proto/inbound.go` parses the byte stream (state machine, zero-alloc)
2. Parsed commands dispatched to `internal/proto/handler.go` (BIND, PRODUCE, SUBSCRIBE, FETCH, ACK, transactions, etc.)
3. Handler resolves connector via `internal/connectors/manager_v2.go` which pools Writers per client name
4. Connector middleware wraps Reader/Writer before use
5. Responses built in `internal/proto/outbound.go` and flushed back via vectored I/O

### Key Internal Packages

- `internal/proto/` — Fujin binary protocol: inbound parser, outbound writer, handler, zero-alloc byte pools (`pool/`)
- `internal/connectors/` — `ManagerV2` manages connector writer pools and applies middleware
- `internal/transport/grpc/` — optional gRPC server (build-tagged)

### Configuration

Env vars drive config loading:
- `FUJIN_CONFIGURATOR` — loader type (e.g., `yaml`)
- `FUJIN_CONFIGURATOR_YAML_PATHS` — path to config file
- `FUJIN_LOG_LEVEL` — DEBUG, INFO, WARN, ERROR
- `FUJIN_LOG_TYPE` — json or text
- `FUJIN_UPGRADE` — set to `1` to start in upgrade mode (inherits FDs from old process)
- `FUJIN_UPGRADE_SOCK` — control socket path (default: `/run/fujin/upgrade.sock`)

Config defines transports, gRPC settings, and connectors (each with type, overridable paths, middlewares, and broker-specific settings).

### Hot Reload & Graceful Binary Upgrade

**Hot Reload (Unix only):** `kill -HUP <pid>` reloads connector config from YAML and updates log level. New connections use updated config; existing connections are unaffected. Implemented via `atomic.Pointer` swap in `server.ReloadConnectors()` and `BaseConfigProvider` callback at BIND time.

**Graceful Binary Upgrade (Unix only):** Zero-downtime binary replacement via FD passing between processes. Implementation in `internal/upgrade/`.

Sequence:
1. Old process listens on control socket (auto-creates directory if needed)
2. New process starts with `FUJIN_UPGRADE=1`, connects to control socket
3. Requests listener FDs via SCM_RIGHTS
4. Starts serving on inherited FDs (TLS is re-applied automatically for TCP)
5. Sends `ready` → old process drains connections and exits

```bash
# Terminal 1: start old process
./bin/fujin

# Terminal 2: build new binary and upgrade
make build
FUJIN_UPGRADE=1 ./bin/fujin

# Custom socket path (default: /run/fujin/upgrade.sock)
FUJIN_UPGRADE_SOCK=/tmp/fujin-upgrade.sock ./bin/fujin
```

### Protocol

Custom binary protocol (big-endian, transport-agnostic). Key opcodes: BIND (0x01), PRODUCE, HPRODUCE (with headers), FETCH, HFETCH, SUBSCRIBE, HSUBSCRIBE, ACK, NACK, TX_BEGIN/COMMIT/ROLLBACK, UNSUBSCRIBE, DISCONNECT, PING (0x63). Full spec in `protocol.md`.
