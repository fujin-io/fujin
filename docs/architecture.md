# Fujin architecture

> **When to read:** before changing the request path, application lifecycle, connectors, or protocol adapters. For wire formats, use [native protocol v1](../protocol.md) and the [protobuf schema](../crates/fujin-grpc-proto/proto/fujin.proto), not this overview.

## In one minute

Fujin separates the client interface, session semantics, and broker integration:

1. a native transport or the separate gRPC listener accepts a connection;
2. an adapter decodes a request and calls the shared `SessionCore`;
3. `SessionCore` validates session state and route capabilities;
4. the immutable generation pinned by BIND provides a `Reader` or `Writer` lease;
5. the connector completes an accepted operation through a callback sink, and the adapter encodes the response.

TCP, QUIC, WebSocket, and Unix sockets are **native transport** plugins. gRPC is not a transport plugin: it is a separate listener behind the `grpc` Cargo feature, but its requests use the same `SessionCore`.

## Workspace map

| Area | Responsibility | Main code |
|---|---|---|
| Composition | public facade, `ApplicationBuilder`, startup, and embedding | [`crates/fujin`](../crates/fujin/) |
| Production process | feature set and built-in plugin registration | [`apps/fujin`](../apps/fujin/) |
| Session semantics | BIND, route capabilities, produce/fetch/subscribe, settlement, transactions, cleanup | [`crates/fujin-core`](../crates/fujin-core/) |
| Native protocol | incremental codec and `NativeSession` adapter | [`crates/fujin-native`](../crates/fujin-native/) |
| Runtime | listeners, gRPC, health, reload, and graceful upgrade | [`crates/fujin-runtime`](../crates/fujin-runtime/) |
| Connectors | public traits, registry, immutable generations, and overrides | [`crates/fujin-connector`](../crates/fujin-connector/) |
| Configuration | bootstrap configuration, configurator registry, and snapshots | [`crates/fujin-configurator`](../crates/fujin-configurator/) |
| Middleware | BIND and connector middleware registries | [`crates/fujin-middleware`](../crates/fujin-middleware/) |
| Native transports | stream/listener contracts, TLS, and listener handoff | [`crates/fujin-transport`](../crates/fujin-transport/) |
| Errors | shared `CoreError`, status, and outcome model | [`crates/fujin-error`](../crates/fujin-error/) |
| gRPC types | protobuf and generated Rust bindings | [`crates/fujin-grpc-proto`](../crates/fujin-grpc-proto/) |
| Stable embedding ABI | C ABI over a statically composed application | [`crates/fujin-ffi`](../crates/fujin-ffi/) |
| Implementations | leaf configurator, connector, and transport crates | [`plugins`](../plugins/) |

## Bootstrap and startup

`ApplicationBuilder::build()` prepares the application without binding listener sockets:

1. explicitly registers `ConfiguratorPlugin`, `ConnectorPlugin`, `TransportRegistration`, and middleware registrations;
2. selects a configurator, explicitly or through `FUJIN_CONFIGURATOR`, and loads one complete `RuntimeConfig`;
3. compiles enabled native transports, the control-plane configuration, and the initial connector generation;
4. rejects duplicates, missing configured plugins, invalid settings, and contradictory route profiles;
5. returns an `Application` ready to start.

`Application::start()` binds listeners, waits for **every** configured listener to report readiness, and only then returns `RunningApplication` with actual endpoints. After readiness, it starts the configurator watcher when `watches_connectors()` is true. Shutdown cancels watchers and listeners, closes active sessions, and drains the catalog through bounded cleanup paths.

The source order is in [`ApplicationBuilder::build`](../crates/fujin/src/application.rs) and [`Application::start`](../crates/fujin/src/application.rs); listener orchestration is in [`server.rs`](../crates/fujin-runtime/src/server.rs).

## Request path

### Native

```text
TCP / QUIC / WebSocket / Unix plugin
  -> NativeSessionService
  -> incremental Decoder + NativeSession
  -> SessionCore
  -> Binding -> ConnectorRuntime -> Reader / Writer
  -> ReaderEventSink / CompletionSink
  -> native response encoder
```

A transport owns the listener, byte stream, TLS, and native session controls. It does not implement BIND or broker semantics. The authoritative frame layout and state machine are in [`protocol.md`](../protocol.md); the adapter is in [`crates/fujin-native/src/session.rs`](../crates/fujin-native/src/session.rs).

### gRPC

```text
Tonic bidirectional Stream
  -> GrpcSession
  -> SessionCore
  -> the same Binding and connector leases
  -> protobuf response stream
```

The service requires feature `fujin/grpc` (`fujin-runtime/grpc`) and is built from the [canonical protobuf schema](../crates/fujin-grpc-proto/proto/fujin.proto). Adapter parity is exercised in [`crates/fujin-runtime/tests/session_adapters.rs`](../crates/fujin-runtime/tests/session_adapters.rs).

### Shared semantics

One protocol adapter owns a `SessionCore` and invokes it sequentially. Concurrent connector callbacks enter only through `CompletionSink` and `ReaderEventSink`. Native and gRPC adapters therefore must not redefine BIND state, route capabilities, transaction rules, or settlement behavior; those changes belong in [`fujin-core`](../crates/fujin-core/).

BIND runs BIND middleware before atomically pinning the current generation. A successfully bound session continues to see that snapshot after a reload; a new session sees the newly published generation.

## Connector generation and lifecycle

The key invariants are defined in [`contract.rs`](../crates/fujin-connector/src/contract.rs) and [`generation.rs`](../crates/fujin-connector/src/generation.rs):

- `ConnectorDescriptor::compile` parses and validates immutable settings **without broker I/O**.
- A complete connector snapshot is compiled before publication. A compile or preflight failure leaves the current generation published.
- Reload publishes a new snapshot atomically. The previous generation drains and closes only after all of its `Binding` values are released.
- A BIND override is allowed only for a path in `overridable`. `ConnectorDescriptor::convert_override` converts its text value, and the result is compiled as a private derived generation without mutating the shared snapshot.
- `CompiledConnector::open_runtime` is lazy by default and runs on the first lease. `open_runtime_eagerly` and `exclusive_runtime_keys` are for the uncommon resource that needs preflight or cannot coexist across generations.
- A runtime belongs to a generation; readers and writers are session-scoped leases. `close(self: Arc<Self>)` needs a bounded, deterministic cleanup path.
- For `Writer`, `Ok(())` means accepted and requires exactly one later `Completion`; synchronous `Err` means not accepted.
- `Reader::subscribe` invokes readiness exactly once before the first delivery. An asynchronous subscription end arrives as `ReaderEvent::Terminal`.
- A route declares `Capabilities`, `AcceptanceGuarantee`, and settlement semantics before use. Adapters do not infer support from broker errors.
- Never hold a lock across broker or network I/O. Every spawned task, listener, reader, writer, and watcher needs bounded cancellation and cleanup.

The production implementation is [`plugins/connector/kafka`](../plugins/connector/kafka/). A compact test implementation of the descriptor/runtime/reader/writer seams is in [`crates/fujin-core/tests/support/mod.rs`](../crates/fujin-core/tests/support/mod.rs).

## Extension boundaries

Five statically linked families attach only through `ApplicationBuilder`: configurator, connector, transport, BIND middleware, and connector middleware. There is no runtime discovery or Rust dynamic-library loading. Exact traits, constructors, and composition workflows are collected in the [extension guide](extending.md).

## Platform and feature restrictions

- `grpc` is disabled by default in `fujin` and `fujin-runtime`; configuring a gRPC listener without it fails at startup. `fujin-app/full` enables it.
- `fujin-transport-unix` is compiled only under `cfg(unix)`; use `cfg = "unix"` in a custom manifest. Windows CI intentionally excludes that crate.
- Listener file-descriptor handoff and SIGHUP/SIGTERM-specific behavior are Unix-only. Other platforms use Ctrl-C, and Unix handoff is unavailable.
- `cdylib` and `staticlib` artifacts expose the C ABI; no Rust trait object crosses that ABI. A generated library disables graceful upgrade because its host owns process lifecycle.
