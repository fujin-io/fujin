# Developing Fujin

> **When to read:** when building the Rust workspace, choosing focused checks, changing a protocol contract, running broker-backed tests, preparing a release, or reproducing benchmarks. Contribution policy remains authoritative in [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## In one minute

Use the pinned Rust toolchain, iterate on the smallest affected package, and run broader checks only after focused work is green. Kafka semantics must be tested against Kafka. Native wire changes must update [`protocol.md`](../protocol.md); gRPC changes start from [`crates/fujin-grpc-proto/proto/fujin.proto`](../crates/fujin-grpc-proto/proto/fujin.proto). Releases are prepared by scripts and published only by the tagged release workflow.

```bash
make build
cargo test -p fujin-core
cargo test -p fujin-native
cargo test -p fujin-runtime --all-features
```

## Prerequisites

- Rust `1.97.1` with `rustfmt` and `clippy`, pinned by [`rust-toolchain.toml`](../rust-toolchain.toml).
- Make.
- Docker and Docker Compose for Kafka-backed tests and deployment validation.
- A C/C++ toolchain, CMake, and pkg-config for the `rdkafka`-based Kafka connector.
- Protocol Buffers tooling for protobuf/Go generation. The Rust build can use the vendored `protoc` fallback, but `make generate` also invokes the Go generator and therefore needs `protoc` plus `protoc-gen-go` on `PATH`.
- Go for SDK tests, generated client bindings, and compatibility checks.

The active code is the Rust workspace at the repository root. The old Go server ended at `v0.5.0`; do not make server changes on `legacy/go-v0.5`.

## Build and run

Build the production composition with its default `full` feature:

```bash
make build
```

This builds `fujin-app` in release mode and copies it to `bin/fujin`. To compile a smaller built-in composition:

```bash
cargo build --release -p fujin-app --no-default-features \
  --features configurator-file,connector-kafka,transport-tcp
```

`fujin-app` features are `configurator-file`, `configurator-env`, `connector-kafka`, `transport-tcp`, `transport-unix`, `transport-websocket`, `transport-quic`, and `grpc`; `full` enables all of them. The facade and runtime have no default gRPC feature, so library users must enable `fujin/grpc` explicitly.

Run the built-in development configuration with:

```bash
make run
```

`make run` sets `FUJIN_CONFIGURATOR=file` and `FUJIN_CONFIGURATOR_FILE_PATHS=./config.dev.yaml`. For custom static binaries and libraries, use the workflow in [extending.md](extending.md).

### Platform restrictions

- `fujin-transport-unix` is Unix-only (`#![cfg(unix)]`). Do not include it unconditionally in a Windows composition.
- Listener descriptor handoff, SIGHUP configuration/logging reload, and SIGTERM handling are Unix-specific. Non-Unix CLI builds use Ctrl-C shutdown.
- The CI platform check compiles TCP, QUIC, WebSocket, and the all-feature runtime on macOS and Windows, then checks Unix transport only when the runner is not Windows. Mirror it when investigating a platform-only problem:

```bash
cargo check -p fujin-runtime --all-features \
  -p fujin-transport-tcp \
  -p fujin-transport-quic \
  -p fujin-transport-websocket
cargo check -p fujin-transport-unix # Unix only
```

- Kafka compilation is heavier than pure Rust crates because the workspace statically builds librdkafka and vendored SSL/zlib features. Missing native compiler/CMake/pkg-config tools are environment failures, not connector failures.

## Focused verification

Choose checks by the contract changed:

```bash
# Session state, connector contracts, generations
cargo test -p fujin-core
cargo test -p fujin-connector

# Native codec and native adapter
cargo test -p fujin-native

# Listener lifecycle, reload, native/gRPC adapter parity
cargo test -p fujin-runtime --all-features

# One transport implementation
cargo test -p fujin-transport-tcp
cargo test -p fujin-transport-quic
cargo test -p fujin-transport-websocket
cargo test -p fujin-transport-unix # Unix only

# Composition generator
cargo test -p cargo-fujin
```

`.focused-spec/config.yaml` discovers focused scenarios in current OpenSpec specs and active changes;
`.focused-spec/runners/cargo-test.mjs` resolves `cargo-test::<crate-directory>::<fully-qualified-Rust-test-name>`
through Cargo's test listing and executes exactly that test. NATS broker-backed selectors require
`FUJIN_NATS_E2E=1` and a running JetStream broker; otherwise the runner rejects execution rather
than reporting a gated test as PASS. With the local [NATS stack](../resources/README.md#nats-jetstream-test-broker)
running, `FUJIN_NATS_E2E=1 node_modules/.bin/focused-spec run --scope current`
executes the exact broker-backed Rust tests from the synced main specification. Run
`node_modules/.bin/focused-spec validate --scope current --strict` to resolve selectors
without executing them; strict validation alone is not broker evidence.

Before merging a cross-cutting Rust change, the repository-wide gates are:

```bash
make fmt
make lint
make check
make test
```

These map to formatting, Clippy with warnings denied, `cargo check --workspace --all-features --all-targets`, and the equivalent full test run. Do not substitute a workspace run for a focused failure reproduction while iterating.

For SDK work:

```bash
make sdk-test
make sdk-compat
```

`sdk-test` runs both Go modules with the race detector. `sdk-compat` builds a current Rust server fixture and exercises native QUIC and gRPC through the Go client. The SDK module and tag policy is in [`sdk/go/README.md`](../sdk/go/README.md).

## Kafka-backed contract

Run the real broker contract with:

```bash
make e2e-kafka
```

The target starts Kafka from [`resources/docker-compose.kafka.yaml`](../resources/docker-compose.kafka.yaml), sets `FUJIN_KAFKA_E2E=1`, runs only [`plugins/connector/kafka/tests/kafka_e2e.rs`](../plugins/connector/kafka/tests/kafka_e2e.rs), and tears the stack down. The test covers produce, subscribe, cumulative settlement, and transactions through `SessionCore`.

Without `FUJIN_KAFKA_E2E`, the test returns early; a plain package test is therefore not evidence for broker behavior. Do not replace remote acknowledgement, reconnect, settlement, or transaction coverage with mocks.

## Change the native protocol

The authoritative byte-level specification is [`protocol.md`](../protocol.md). A native wire change normally touches:

1. request/response layout and state rules in `protocol.md`;
2. decoder, encoder, wire types, and adapter under [`crates/fujin-native`](../crates/fujin-native/);
3. fragmentation, malformed-frame, state transition, and allocation-sensitive tests near that code;
4. [`SessionCore`](../crates/fujin-core/) only when the transport-neutral semantic contract changes;
5. every compatible SDK, currently the Go native client under [`sdk/go/client`](../sdk/go/client/).

Use focused Rust checks first, then cross-adapter and SDK compatibility:

```bash
cargo test -p fujin-native
cargo test -p fujin-core
cargo test -p fujin-runtime --all-features
make sdk-test
make sdk-compat
```

Do not encode broker semantics in the native adapter. Native and gRPC must continue to delegate BIND, capabilities, transactions, settlement, and connector cleanup to the shared core; see [architecture.md](architecture.md).

## Change protobuf/gRPC

The single server and Go-client source schema is:

[`crates/fujin-grpc-proto/proto/fujin.proto`](../crates/fujin-grpc-proto/proto/fujin.proto)

After editing it:

```bash
make generate
cargo test -p fujin-grpc-proto -p fujin-runtime --all-features
make sdk-test
make sdk-compat
```

`make generate` builds `fujin-grpc-proto` and regenerates `sdk/go/client/grpc/v1/proto/fujin.pb.go`. Rust bindings are emitted into Cargo build output and are not committed; the generated Go binding is committed and CI verifies it has no diff. Preserve field numbers and the `fujin.v1` namespace unless the change deliberately introduces a versioned compatibility break.

If the operation semantics change, update both protocol adapters and `SessionCore` rather than making the protobuf adapter behave differently. The gRPC listener itself requires the `grpc` Cargo feature.

## Release workflow

All publishable Rust crates share one workspace version. Product tags are namespaced (`fujin/vX.Y.Z`), while Cargo, image, and Helm versions are unprefixed `X.Y.Z`; Go modules use their own namespaced tags.

On `develop`, prepare and validate an unprefixed semantic version:

```bash
./scripts/prepare_release.py 0.6.0
./scripts/validate_release.py 0.6.0
```

`prepare_release.py` updates the release-coupled workspace, lock, image, chart, deployment, and documentation versions. Review all resulting changes; do not hand-edit only `Cargo.toml`. `validate_release.py` checks version consistency, exact internal dependency requirements, publishability, and dependency publication order.

After that change is merged and verified on `main`:

1. create annotated tag `fujin/v0.6.0` on that exact commit;
2. manually dispatch [`.github/workflows/release.yml`](../.github/workflows/release.yml) with `tag=fujin/v0.6.0` and `version=0.6.0`;
3. use `bootstrap_crates` only for the first crates.io publication; normal releases use trusted publishing.

The workflow validates the tag and commit, runs Rust/SDK/Kafka/deployment gates, publishes crates in the dependency waves from [`scripts/release_crates.txt`](../scripts/release_crates.txt), verifies a version-only `cargo-fujin` composition, publishes the multi-platform GHCR image, creates Go module tags, packages the Helm chart, and creates the GitHub Release.

Never publish crates manually out of order or reuse a version once any artifact is public. A failed workflow may be rerun only for the unchanged tag. If source must change after publication starts, prepare a new version. The detailed release policy is in [`CONTRIBUTING.md`](../CONTRIBUTING.md#releases).

## Benchmarks

The retained report is [`bench_report.md`](../bench_report.md). Its source harnesses are under [`tools/bench`](../tools/bench/) and the report generator is [`scripts/generate_bench_report.sh`](../scripts/generate_bench_report.sh).

Run the native session benchmark alone:

```bash
make bench
```

Regenerate the full native TCP and gRPC report:

```bash
make bench-report
```

For a smaller local validation matrix:

```bash
FUJIN_BENCH_SMALL_OPERATIONS=1000 \
FUJIN_BENCH_LARGE_OPERATIONS=100 \
FUJIN_BENCH_PEAK_ITERATIONS=10000 \
  ./scripts/generate_bench_report.sh
```

The report uses a statically registered nop connector and localhost adapters, not Kafka. It measures protocol, Session Core, scheduling, encoding, callbacks, and transport overhead; it does **not** measure broker durability or acknowledgement latency. Timing and allocation runs are separate, and the generator replaces the report only after the full expected matrix succeeds.

Use the exact harness matching the question:

- [`session-bench.rs`](../tools/bench/src/bin/session-bench.rs): native TCP synchronous/pipelined produce;
- [`grpc-session-bench.rs`](../tools/bench/src/bin/grpc-session-bench.rs): gRPC equivalent;
- [`session-matrix-bench.rs`](../tools/bench/src/bin/session-matrix-bench.rs): native transport matrix;
- [`grpc-session-matrix-bench.rs`](../tools/bench/src/bin/grpc-session-matrix-bench.rs): gRPC matrix.

Record the source revision, dirty state, Rust version, OS/architecture, payload, concurrency, operation count, and allocator mode when sharing new numbers. Do not compare report rows as broker benchmarks or extrapolate to unmeasured transports.
