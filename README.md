# Fujin

Fujin is a blazingly fast ([See Benchmarks](#benchmarks)) broker connector that bridges your applications with any message broker through a unified, efficient protocol. It translates between a custom byte-based protocol and various messaging systems, aiming to provide a seamless experience regardless of the underlying broker.

> **Note**: This project was created mostly for learning purposes, and it might not be usefull for anyone at all. Experimental. Not production ready.

## Pitch

Modern distributed systems often need to work with multiple message brokers, each with its own protocol and quirks. Here are some core features:

- **Unified Protocol**: A single, efficient byte-based protocol for all your messaging needs
- **Zero Broker Lock-in**: Switch between brokers without changing your application code
- **Dual Message Delivery Models**:
  - Push: Server pushes messages to clients
  - Pull: Clients request messages from server (not for all brokers)
- **Transaction Support**: Atomic message production across multiple topics (not for all brokers)
- **Blazing Speed & Efficiency**: Optimized for processing large volumes of messages efficiently, leveraging techniques like zero-allocation parsing
- **Plugin support**: Choose from various available default plugins, or write your own, and compile it in binary
- **Multiple Transports**: QUIC (multiplexed, built-in TLS) and TCP (raw throughput) — same protocol, choose what fits
- **gRPC Interface**: Modern, language-agnostic RPC interface for easy integration
- **Keep-Alive Mechanism**: Automatic connection health monitoring

## Supported Connectors

- Kafka
- NATS Core
- AMQP 0.9.1 (RabbitMQ)
- AMQP 1.0 (Azure Service Bus, Apache ActiveMQ)
- RESP (PubSub/Streams)
- NSQ
- MQTT

## Client Interfaces

Fujin provides multiple client interfaces to suit different needs:

### Fujin Native Protocol

Optimized binary protocol with minimal overhead. Best for high-performance applications. The protocol layer is transport-agnostic and supports two transports:

- **QUIC** — Multiplexed streams over UDP with built-in TLS. Ideal when you need multiple concurrent streams, built-in encryption, and connection migration.
- **TCP** — Plain TCP (with optional TLS). Delivers higher raw throughput (~3x faster than QUIC on large payloads). Best when you need maximum single-stream performance over a reliable network.

Only Go client is supported right now.

### gRPC Interface
Easy to use, works with any language that supports gRPC.

## Use Cases

Fujin is particularly useful for:

- Microservices architectures integrating diverse messaging backends.
- Systems migrating between message brokers with minimal application changes.
- Applications requiring a unified messaging API across different cloud providers or on-premise systems.
- Scenarios demanding high-throughput message processing.

## When to Use Fujin

Fujin is most valuable in these scenarios:

- **Multiple Broker Environment**: When your system needs to work with different message brokers simultaneously
- **Unified Interface Needed**: When you want a single protocol interface across different brokers
- **Security Gateways Involved**: When you can connect to broker only through security gateway, not supporting its protocol

## When Not to Use Fujin

Consider alternatives if:

- **Single Broker**: You're only using one message broker
- **Minimal Layers**: You want to minimize the number of components in your architecture
- **Ultra-Low Latency**: When every microsecond counts and you can't afford additional overhead
- **Broker-Specific Features**: When you need direct access to broker-specific features

## Project Structure

```
fujin/
├── cmd/                        # Entry points
│   ├── main.go                 # Default server (all plugins)
│   └── builder/                # Custom binary builder (selective plugins)
├── public/                     # Public API and plugins
│   ├── plugins/                # Connectors, configurators, middlewares
│   ├── proto/                  # gRPC and Fujin protocol definitions
│   ├── server/                 # Server abstraction and config
│   └── service/                # Core service (RunCLI)
├── internal/                   # Internal implementation (not exported)
│   ├── protocol/fujin/         # Fujin binary protocol (transport-agnostic)
│   │   ├── pool/               # Zero-allocation byte pool
│   │   └── proto/response/     # Response builders
│   └── transport/              # Transport implementations
│       ├── quic/               # QUIC transport (quic-go)
│       ├── tcp/                # TCP transport
│       └── grpc/               # gRPC transport
├── examples/                   # Sample configs and runnable examples
├── resources/                  # Docker Compose, example configs
└── test/                       # Benchmarks and test helpers
```

## Benchmarks

All benchmarks run on **Apple M2, macOS, arm64**, single-connection localhost, using `go test -bench`.
Full raw results: [`test/bench_test.txt`](test/bench_test.txt).

## Documentation

- [Native Protocol Specification](protocol.md)
- [gRPC Protocol Specification](public/proto/grpc/v1/fujin.proto)
- [Configuration Guide](examples/assets/config/config.yaml)

## Build Options

### Server Build

The server uses Go build tags to conditionally compile transports:

**Available Build Tags:**
- **Transports**:
  - `quic` - QUIC transport for the Fujin protocol (multiplexed, built-in TLS)
  - `tcp` - TCP transport for the Fujin protocol (raw throughput)
  - `grpc` - gRPC server (language-agnostic)

The Fujin protocol code compiles when any dependent transport is enabled (`quic` or `tcp`).

**Building the server:**

The Makefile uses the [Custom Binary Builder](#custom-binary-builder) to build Fujin.

```bash
# Build with all transports (default)
make build

# Build with QUIC only
make build GO_BUILD_TAGS="quic"

# Build with TCP only
make build GO_BUILD_TAGS="tcp"

# Build with all transports
make build GO_BUILD_TAGS="quic,tcp,grpc"

# Build minimal (Kafka only, QUIC+gRPC)
make build CONNECTORS=github.com/fujin-io/fujin/public/plugins/connector/kafka/franz

# Build with selected connectors
make build CONNECTORS="github.com/fujin-io/fujin/public/plugins/connector/kafka/franz,github.com/fujin-io/fujin/public/plugins/connector/nats/core"
```

**Binary Size Comparison (With kafka connector):**
- With QUIC only: ~10 MB
- With QUIC + gRPC: ~16 MB (full)

### Custom Binary Builder

The `cmd/builder` tool builds a **minimal Fujin binary** containing only the plugins you need. Instead of compiling all connectors (Kafka, NATS, AMQP, MQTT, NSQ, RESP…), you explicitly choose which ones to include. This yields smaller binaries and fewer dependencies.

**When to use:**
- You need only 1–2 brokers (e.g. Kafka + NATS)
- Smaller Docker images or embedded deployments
- Custom plugins (e.g. from `examples/plugins/`)

**Usage:**

```bash
go run ./cmd/builder \
  -configurator github.com/fujin-io/fujin/public/plugins/configurator/file \
  -connector github.com/fujin-io/fujin/public/plugins/connector/kafka/franz \
  -connector github.com/fujin-io/fujin/public/plugins/connector/nats/core \
  -bind-middleware github.com/fujin-io/fujin/public/plugins/middleware/bind/auth_api_key \
  -connector-middleware github.com/fujin-io/fujin/public/plugins/middleware/connector/metrics \
  -tags "quic,tcp,grpc" \
  -output ./bin/fujin-minimal
```

**Flags:**
| Flag | Description |
|------|-------------|
| `-configurator` | Config loader (at least one required, typically `file`) |
| `-connector` | Broker connectors (repeat for multiple) |
| `-bind-middleware` | Bind/auth middleware (e.g. `auth_api_key`) |
| `-connector-middleware` | Connector middleware (e.g. `metrics`, `tracing`) |
| `-tags` | Go build tags: `quic`, `tcp`, `grpc` for transports; |
| `-output` | Output binary path (default: `fujin`) |
| `-cgo` | Enable CGO (if required by custom plugins) |

**Available default plugins:**
- Configurators: `public/plugins/configurator/file`
- Connectors: `kafka`, `nats/core`, `rabbitmq_amqp09`, `azure_amqp1`, `resp/pubsub`, `resp/streams`, `mqtt`, `nsq`
- Bind middlewares: `auth_api_key`
- Connector middlewares: `metrics`, `tracing`

Use full package paths, e.g. `github.com/fujin-io/fujin/public/plugins/connector/kafka/franz`.

## Contributing

Fujin is completely open-source, feel free to contribute! If you plan to contribute, please see if there's a `CONTRIBUTING.md` file for guidelines, or start by opening an issue to discuss your proposed changes.

## License

This project is licensed under the terms of the MIT License. See the [LICENSE](LICENSE) file for details.
