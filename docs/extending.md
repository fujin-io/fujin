# Extending Fujin

> **When to read:** when authoring a connector, configurator, native transport, or middleware crate, or when building a custom static composition. Read [the architecture guide](architecture.md) first for ownership and lifecycle boundaries.

## In one minute

A Fujin plugin is an ordinary Rust crate with a stable `plugin()` constructor. It returns one registration value from the public `fujin` facade and is explicitly added to `ApplicationBuilder`. Plugins are statically linked; Fujin does not scan directories, load Rust dynamic plugins, or discover them from environment variables.

There are five families:

| Family | Public implementation contract | Registration returned by `plugin()` | Builder method |
|---|---|---|---|
| Configurator | `fujin::configurator::Configurator` | `ConfiguratorPlugin` | `.configurator(...)` |
| Connector | `fujin::connector::ConnectorDescriptor` and runtime/lease traits | `ConnectorPlugin` | `.connector(...)` |
| Native transport | `fujin::transport::TransportPlugin` and `CompiledTransport` | `TransportRegistration` | `.transport(...)` |
| BIND middleware | `fujin::middleware::bind::BindMiddlewarePlugin` | `BindMiddlewareRegistration` | `.bind_middleware(...)` |
| Connector middleware | `fujin::middleware::connector::ConnectorMiddlewarePlugin` | `ConnectorMiddlewareRegistration` | `.connector_middleware(...)` |

These types are re-exported by [`crates/fujin/src/lib.rs`](../crates/fujin/src/lib.rs). Use those facade paths in third-party crates instead of reaching into internal runtime crates.

The standard application includes Kafka and NATS JetStream, file/env configurators, and four native transports; no built-in middleware implementation has been ported. Middleware contracts are available for custom crates.

## Author a connector

### Contract layers

A connector separates configuration from live broker resources:

1. `ConnectorDescriptor::compile(&serde_json::Value)` parses and validates settings without broker I/O and returns `Arc<dyn CompiledConnector>`.
2. `CompiledConnector::routes()` returns immutable `BTreeMap<String, RouteProfile>` declarations. `open_runtime()` creates generation-owned live resources, lazily unless `open_runtime_eagerly()` is true.
3. `ConnectorRuntime::open_reader` and `open_writer` create session-scoped leases for one route.
4. `Reader` and `Writer` accept operations synchronously and report asynchronous outcomes through `ReaderEventSink` and `CompletionSink`.

The exact public signatures and contract comments are in [`crates/fujin-connector/src/contract.rs`](../crates/fujin-connector/src/contract.rs). The Kafka implementation in [`plugins/connector/kafka/src/lib.rs`](../plugins/connector/kafka/src/lib.rs) is the production example. For a smaller implementation, follow the test connector in [`crates/fujin-core/tests/support/mod.rs`](../crates/fujin-core/tests/support/mod.rs).

The production Kafka connector exposes a complete registration constructor:

```rust
#[must_use]
pub fn plugin() -> fujin_connector::ConnectorPlugin {
    fujin_connector::ConnectorPlugin::new("kafka", KafkaDescriptor)
}
```

`KafkaDescriptor` is defined in the same [implementation](../plugins/connector/kafka/src/lib.rs). A new connector supplies its own fully implemented descriptor and runtime; the registration name becomes the configured `type`. Preserve these observable rules:

- every non-empty route has a valid `RouteProfile`;
- `Writer` returns `Ok(())` only after accepting responsibility for exactly one completion;
- `flush` is a snapshot barrier over operations accepted before it;
- a subscription calls `ReadyCallback` once with `Ok(())` after broker attachment and before delivery, or `Err(error)` if asynchronous attachment fails;
- each accepted fetch or settlement emits one matching completion event;
- `close` deterministically resolves pending work and releases owned tasks/resources;
- no lock is held across network or broker I/O;
- remote acknowledgement, reconnect, settlement, and transaction behavior is covered by a real broker test.

If BIND overrides are supported, list allowed paths in connector instance configuration and implement `ConnectorDescriptor::convert_override`. Fujin rejects paths not in `overridable` before compiling a private derived generation.

The runtime registration name (`"kafka"` above) is the configured `type`; the connector instance name is the map key under `connectors` and is selected by the client's `BIND`. For a valid instance with routes, see the [Kafka configuration example](configuration.md#kafka-instances-and-routes).

## Author another plugin family

### Configurator

Implement `Configurator::load` to return one complete `RuntimeConfig`. Implement `initial_connector_snapshot`, `watches_connectors`, and `watch_connectors` only for a source that supplies revisioned connector updates. Construct the registration with `ConfiguratorPlugin::new("acme", factory)` or `from_factory`. See [`plugins/configurator/file`](../plugins/configurator/file/) and [`plugins/configurator/env`](../plugins/configurator/env/).

A watcher submits complete snapshots through `fujin::configurator::ConnectorRuntime`; it must honor the provided `CancellationToken`. It must not mutate the catalog directly.

### Native transport

Implement `TransportPlugin::compile` as side-effect-free settings validation returning a `CompiledTransport`. `CompiledTransport::serve` binds and serves listeners through `TransportContext`, reports every listener with `signal_ready`, passes each byte stream to `serve_native_stream_with_config`, and stops on the context cancellation token. Return `TransportRegistration::new("acme", Plugin)`.

Built-in references are [`plugins/transport/tcp`](../plugins/transport/tcp/), [`quic`](../plugins/transport/quic/), [`websocket`](../plugins/transport/websocket/), and [`unix`](../plugins/transport/unix/). A transport carries native protocol bytes; gRPC is a separate runtime listener and is not implemented through `TransportPlugin`.

### Middleware

- `BindMiddlewarePlugin::process` receives inline settings and mutable BIND metadata. Rejecting it prevents generation acquisition. Register with `BindMiddlewareRegistration::new`.
- `ConnectorMiddlewarePlugin::compile` creates generation-scoped resources. Its `CompiledConnectorMiddleware` wraps session reader/writer leases and closes when the generation drains. Register with `ConnectorMiddlewareRegistration::new`.

Configured middleware runs in declaration order. An enabled but unregistered middleware is a configuration error. The authoritative contracts and chain implementation are in [`crates/fujin-middleware/src/lib.rs`](../crates/fujin-middleware/src/lib.rs).

## Compose directly with Rust

For embedding, add the plugin crates as Cargo dependencies and register each constructor explicitly:

```rust
use fujin::Application;

let application = Application::builder()
    .configurator(acme_configurator::plugin())
    .connector(acme_connector::plugin())
    .transport(fujin_transport_tcp::plugin())
    .build()
    .await?;

let running = application.start().await?;
println!("listeners: {:?}", running.endpoints());
running.shutdown().await?;
```

A complete runnable TCP embedding example is [`crates/fujin/examples/embed.rs`](../crates/fujin/examples/embed.rs). Passing `.config(RuntimeConfig)` is an alternative to registering/selecting a configurator for an embedded application.

For NATS JetStream, add `fujin-connector-nats` as a dependency and register `.connector(fujin_connector_nats::plugin())` instead of, or alongside, Kafka. This is a direct `ApplicationBuilder` composition; `settings` still use `type: nats` and require externally provisioned streams and explicit-ACK durable pull consumers ([configuration](configuration.md#nats-jetstream-instances-and-routes)).

## Compose with `cargo-fujin`

`cargo-fujin` generates a small Cargo project whose source consists of explicit `ApplicationBuilder` calls. The checked-in production example is [`deploy/docker/fujin.build.toml`](../deploy/docker/fujin.build.toml).

From the repository root, this creates a runnable local composition:

```bash
cargo install --path tools/cargo-fujin --locked
cargo fujin init --fujin-path ./crates/fujin
cargo fujin plugin add configurator \
  --name file --package fujin-configurator-file \
  --path ./plugins/configurator/file
cargo fujin plugin add connector \
  --name kafka --package fujin-connector-kafka \
  --path ./plugins/connector/kafka
# To include JetStream in this composition, also register:
cargo fujin plugin add connector \
  --name nats --package fujin-connector-nats \
  --path ./plugins/connector/nats
cargo fujin plugin add transport \
  --name tcp --package fujin-transport-tcp \
  --path ./plugins/transport/tcp
cargo fujin generate
cargo fujin build
```

The first build creates `.fujin/generated/Cargo.lock`. Use `cargo fujin build --locked` for later reproducible builds, or seed the generated project with `--lockfile PATH`. `--offline`, `--target`, `--profile`, `--output`, and `--clean-after` pass the corresponding build policy to the generated project. `cargo fujin clean` removes generated source and its build cache but preserves the installed output.

`fujin.build.toml` rules:

- `[application].fujin` and every plugin specify exactly one source: `version`, `git`, or `path`;
- Git dependencies may use at most one of `rev`, `tag`, or `branch`;
- `artifact` is `binary` (default), `cdylib`, or `staticlib`;
- `factory` defaults to `plugin` and may be a Rust path such as `factory::plugin`;
- `cfg` places the dependency and builder call behind a target expression;
- the manifest plugin `name` is a unique generated Cargo alias across all families; the value returned by the plugin constructor is its runtime registration name;
- generation requires at least one configurator and one connector. Starting the result also requires at least one native, gRPC, or health listener in runtime configuration.

For gRPC, enable the facade feature in the application dependency, for example:

```toml
[application]
fujin = { path = "./crates/fujin", features = ["grpc"] }
```

For the Unix transport, guard the plugin dependency and registration:

```toml
[[plugin]]
family = "transport"
name = "unix"
package = "fujin-transport-unix"
path = "./plugins/transport/unix"
cfg = "unix"
```

The Unix plugin itself is `#![cfg(unix)]`; an unconditional registration is not portable to Windows. gRPC is not a `[[plugin]]` entry. Native transport plugins do not enable it.

Generated `cdylib` and `staticlib` artifacts add `fujin-ffi` and export the versioned C API from [`crates/fujin-ffi/include/fujin.h`](../crates/fujin-ffi/include/fujin.h). They still contain statically linked Rust plugins; no Rust trait object crosses the ABI.

## Review checklist

Before proposing a plugin change:

- keep the implementation in one independent leaf crate;
- export a stable `plugin()` constructor and use an existing registration type;
- reject unknown/invalid settings before listeners bind;
- declare route capabilities and guarantees precisely;
- make cancellation and close paths bounded;
- add focused contract tests and broker-backed coverage where remote behavior matters;
- update the custom composition and user configuration examples that actually use it.

Development and verification commands are in [development.md](development.md). Configuration syntax belongs in [configuration.md](configuration.md), not in plugin source documentation.
