## 1. Plugin and route contracts

- [x] 1.1 Add the `fujin-connector-nats` leaf crate and `async-nats` workspace dependency; export `plugin()` registered as `nats`.
- [x] 1.2 Implement locally validated server, optional credential, and route settings plus immutable profiles; cover invalid routes, peer acceptance, and the unsupported header/transaction capabilities with exact tests.
- [x] 1.3 Register an optional `connector-nats` feature in `fujin-app`, include it in `full`, and test enabled/disabled builds and a direct `ApplicationBuilder` composition.

## 2. JetStream writer

- [x] 2.1 Open generation-owned NATS resources lazily; implement broker-acknowledged publish and error mapping with bounded admission, exactly-once completion, and explicit rejection of unsupported transactions.
- [x] 2.2 Implement snapshot flush and deterministic writer close, with broker-backed PubAck, rejection, barrier, disconnect, and pending-work tests.

## 3. JetStream reader

- [x] 3.1 Attach only to an existing explicit-ack durable pull consumer, rejecting missing/incompatible resources without modifying broker state.
- [x] 3.2 Implement bounded fetch and cancelable subscription pulls; verify zero/maximum counts, readiness-before-delivery, and terminal errors against NATS.
- [x] 3.3 Implement reader-scoped manual IDs, confirmed single ACK, requeue NACK, and auto-settle-before-delivery; verify redelivery, cross-reader/stale IDs, and close cancellation against NATS.

## 4. Integration and executable evidence

- [x] 4.1 Configure `.focused-spec/config.yaml` and a project-local Cargo test runner; verify exact selection, missing targets, passing, failing, and ignored outcomes against real Rust tests.
- [x] 4.2 Add a JetStream-enabled local NATS stack and independent broker-backed cases for the spec's exact `cargo-test` selectors.
- [x] 4.3 Replace every `planned:` selector with an executable target; run focused-spec strict validation and selected evidence, checking each changed outcome detects a reversible product regression where safe.
- [x] 4.4 Verify generation pinning and native/gRPC route profile/error parity on the real application path; keep Kafka and no-NATS compositions working.

## 5. Documentation and delivery

- [x] 5.1 Document NATS instance settings, external stream/consumer provisioning, credential handling, capability limits, and static composition in existing configuration/extension/deployment guides.
- [x] 5.2 Run focused tests and the relevant feature builds, execute NATS broker-backed smoke paths, validate the OpenSpec change and focused evidence, then sync/archive only after every outcome is implemented and verified.
