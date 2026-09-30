## Why

Fujin currently ships only a Kafka broker connector. Applications using NATS JetStream cannot use the same native and gRPC session APIs without building and maintaining their own connector.

## What Changes

- Add a statically linked `nats` connector backed by JetStream, with locally validated connection and route settings and generation-scoped, lazy broker resources.
- Expose broker-acknowledged publish, bounded fetch, subscription, and single-message ACK/NACK for pre-existing JetStream streams and explicit-ack durable pull consumers through the existing Session Core.
- Advertise only capabilities the connector can uphold: no transactions and no Fujin headers until arbitrary-byte values can round-trip losslessly. A JetStream publish acknowledgment is advertised as `peer_accept`, not `durable_accept`, because stream storage policy is controlled externally.
- Make the connector available in the standard application and custom static compositions; document configuration and broker-backed verification without changing the native or gRPC wire formats.

## Capabilities

### New Capabilities

- `nats-jetstream-connector`: Validate and bind a NATS JetStream connector; publish, receive, settle, and clean up according to its route profile.

### Modified Capabilities

Existing Kafka behavior and shared Session Core operation semantics are unchanged; the connector readiness callback now accepts an asynchronous attachment result so broker failures can reject subscription before delivery.

## Impact

New leaf crate under `plugins/connector/nats/`, `async-nats` dependency, Cargo workspace and optional `fujin-app` feature/registration, asynchronous reader-readiness callback across connector implementations, local NATS test stack, connector integration tests, and configuration/extension/deployment documentation. NATS Core (ephemeral) pub/sub, JetStream stream/consumer provisioning, transactions, and binary-valued Fujin headers are outside this change.
