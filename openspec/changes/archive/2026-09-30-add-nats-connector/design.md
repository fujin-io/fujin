## Context

Fujin's `ConnectorDescriptor` compiles settings and immutable route profiles without broker I/O; each BIND pins a generation, and a runtime opens reader/writer leases. `Writer` methods accept work synchronously, complete it asynchronously exactly once, and implement flush as a snapshot barrier. `Reader` emits readiness before delivery, bounded fetch results, settlement results, and terminal events. Both native and gRPC sessions use the same Session Core.

NATS Core pub/sub has no persistent consumer or publish acknowledgment suited to this contract. JetStream supports acknowledged publication and explicit-ack durable pull consumers. Its textual headers cannot represent Fujin's arbitrary-byte header values losslessly; transactions are not available. JetStream stream storage policy is externally managed.

## Goals / Non-Goals

**Goals:** An optional, statically registered `nats` plugin that maps configured JetStream subjects and existing durable pull consumers to honest BIND profiles; real publish, fetch, subscription, ACK/NACK, reload, and bounded cleanup semantics through both adapters.

**Non-Goals:** Ephemeral NATS Core subscriptions, stream or consumer provisioning, transactions, Fujin header capability, broker-specific wire commands, or changes to Kafka behavior.

## Decisions

1. **One leaf plugin, existing contracts.** Create `plugins/connector/nats/` with `plugin() -> ConnectorPlugin` registered as `nats`. Add an optional `connector-nats` feature and include it in `fujin-app/full`; add an explicit `cargo-fujin` composition example. Use `async-nats` behind the plugin, not broker logic in Session Core. Alternative—special-case NATS in adapters—would diverge native and gRPC semantics.
2. **Explicit configuration and ownership.** `settings.common.servers` is a nonempty list of NATS server URLs; optionally accept an operator-specified credentials file for authenticated deployments. A route has `publish_subject` for producing, or both `stream` and `consumer` for reading, or both forms. Validate nonempty names, subject syntax, and complete route pairs at compile time; do not contact the broker during compile or alter server-side resources. Resolve pre-existing streams and explicit-ack durable pull consumers lazily at reader use; reject missing/incompatible resources rather than silently creating or reconfiguring them. Publish requires a subject already captured by a stream and reports an error if JetStream rejects it. Connections are owned by the generation and shared among its leases; reload preserves old generation until the last binding closes.
3. **Honest capability profile.** Producing routes advertise `PRODUCE` with `Peer` guarantee: complete only after a successful JetStream PubAck; actual disk/replication durability depends on stream policy. Reading routes advertise `FETCH`, `SUBSCRIBE`, `MANUAL_SETTLEMENT`, ACK `Single`, NACK `Requeue`. Neither `HEADERS` nor `TRANSACTIONS` is advertised; the unsupported writer transaction methods fail without accepting work. Native and gRPC adapters continue to reject unsupported operations using the common profile.
4. **Operation boundaries.** An accepted publish owns its payload and token and resolves exactly once; a failed send/timeout is reported through that token, not as a false success. Flush waits for all earlier accepted operations, without waiting for later ones. Close stops acceptance, resolves outstanding completions, and closes worker/connection resources within the existing lifecycle bounds. Bound per-lease pending operations and broker waits; do not spawn an unbounded task per message or hold locks across I/O.
5. **Pull-based reading.** A reader lease binds an existing explicit-ack durable pull consumer. `FETCH(maximum)` uses a finite broker wait and returns at most the requested count (including zero); subscription drives repeated bounded pulls and signals ready exactly once after consumer attachment, before the first delivery. Existing Session Core assumed readiness before `Reader::subscribe` returned; its async subscription now awaits a readiness callback carrying success or failure, preserving both adapter response ordering and failure cleanup without blocking an executor thread. Manual delivery IDs identify this reader's outstanding messages, not just a stream sequence: on ACK send a confirmed (`double_ack`) single-message acknowledgment, and on NACK send JetStream `Nak` for redelivery; report individual failures. In auto mode acknowledge each message before emitting it to Session Core (at-most-once relative to the Fujin client). Closing a reader cancels outstanding pulls and invalidates its IDs; unacknowledged messages remain eligible for broker redelivery. Surface an unexpected subscription end through `ReaderEvent::Terminal`.
6. **Verification and integration.** Add a local JetStream-enabled NATS stack and exact broker-backed tests for PubAck, fetch bounds, subscribe readiness, settlement/redelivery, broker errors/reconnect, flush/close, and generation pinning; cover native and gRPC adapter capability/error parity. The project-local `cargo-test` runner and focused-spec document layout are configured now and verified against an existing Rust test. NATS selectors remain `planned:` until their exact tests are implemented; a successful non-strict validation is not evidence that they ran. Update the user configuration, plugin guide, optional-feature builds, and composition examples only when functionality is present.

## Risks / Trade-offs

- **No lossless NATS header bridge** → Do not advertise HEADERS; payload remains unmodified. Do not silently coerce byte values to text.
- **JetStream server policy may use memory storage** → Advertise `peer_accept`, not `durable_accept`, despite a successful PubAck.
- **Publish/settlement outcome during disconnect may be unknown remotely** → Report the error or unknown outcome through existing `CoreError` semantics; do not retry non-idempotent operations blindly.
- **Consumer redelivery and multiple reader leases compete for a durable consumer** → Treat it as normal shared-consumer semantics; key IDs to the delivering reader and test redelivery under timeout and close.
- **Slow broker or stuck pull** → Enforce finite waits and bounded cancellation; broker-backed tests must verify shutdown resolves accepted work.

## Migration Plan

No existing connector config changes. Operators provision JetStream streams and explicit-ack durable pull consumers first, then add an instance with `type: nats` and point clients at its configured instance name. Roll back by removing that instance and disabling the optional feature; existing Kafka instances stay intact.

## Open Questions

None blocking the proposed contract. Confirm the chosen `async-nats` API and credentials-file/TLS handling during implementation against the dependency version selected by the workspace.
