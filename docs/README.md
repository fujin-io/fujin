# Fujin documentation

Fujin is a Rust gateway between applications and message brokers. Clients connect through native protocol v1 or gRPC; both adapters share the same Session Core. The standard build includes Kafka and NATS JetStream broker connectors. TCP, QUIC, WebSocket, and Unix sockets are client transports, not broker connectors.

**How to read this:** start with your task, then follow links for implementation details and authoritative contracts. This directory is a task-oriented guide, not a second copy of the wire specifications.

## Start here

| I want to… | Read first | Go deeper when needed |
| --- | --- | --- |
| Run a local stack and send a message | [Getting started](getting-started.md) | [Configuration](configuration.md) |
| Choose a client protocol or SDK | [Client interfaces](interfaces.md) | [Native v1](../protocol.md), [gRPC protobuf](../crates/fujin-grpc-proto/proto/fujin.proto) |
| Configure Kafka or NATS JetStream, routes, and listeners | [Configuration](configuration.md) | [Full YAML example](../examples/assets/config/config.deployment.example.yaml) |
| Manage health, reload, or a binary upgrade | [Operations](operations.md) | [Docker and Helm](../deploy/helm/fujin/README.md) |
| Understand a request and the runtime lifecycle | [Architecture](architecture.md) | [Crate contracts](../crates/fujin/src/lib.rs) |
| Build a plugin or custom composition | [Extending Fujin](extending.md) | [Rust embedding example](../crates/fujin/examples/embed.rs) |
| Develop and verify changes | [Development](development.md) | [CONTRIBUTING](../CONTRIBUTING.md) |

## The system at a glance

```text
client → native (TCP / QUIC / WebSocket / Unix) ─┐
                                                  ├→ Session Core → connector generation → Kafka / NATS JetStream
client → protobuf gRPC ───────────────────────────┘
```

A configurator selects the initial configuration. `BIND` pins a session to one immutable connector generation; a configuration replacement affects later BIND operations only. Native and gRPC adapters delegate business semantics to the same Session Core.

## Authoritative references

- [Project README](../README.md) introduces the capabilities; [CONTRIBUTING](../CONTRIBUTING.md) defines the development workflow.
- [protocol.md](../protocol.md) specifies native v1 bytes; [fujin.proto](../crates/fujin-grpc-proto/proto/fujin.proto) defines the gRPC API.
- [Go SDK overview](../sdk/go/README.md) distinguishes network client and embedding; [C ABI header](../crates/fujin-ffi/include/fujin.h) defines the library contract for other languages.
- [Configuration example](../examples/assets/config/config.deployment.example.yaml), [local broker stack](../resources/README.md), [Helm chart](../deploy/helm/fujin/README.md), and [benchmark report](../bench_report.md) provide deeper reference material.

If a guide conflicts with executable code or an authoritative contract, correct the guide rather than introducing a parallel contract. The active implementation is the root Rust workspace; the final Go server is available at tag `v0.5.0` and branch `legacy/go-v0.5`.
