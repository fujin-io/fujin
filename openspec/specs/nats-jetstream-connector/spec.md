# NATS JetStream connector

## Purpose

Define the externally provisioned JetStream connector contract for Fujin sessions.

## Requirements

### Requirement: Compile and bind a JetStream connector
The `nats` connector SHALL validate its connection settings and nonempty route definitions without contacting a NATS server, SHALL expose immutable route profiles through BIND, and SHALL keep each bound session on its selected generation across configuration reloads.

#### Scenario: Invalid route rejected before startup
- **ID**: `nats.config.invalid-route`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::tests::invalid_route_rejected_without_broker`
- **WHEN** a configured NATS route has neither a publish subject nor a complete stream-and-consumer pair
- **THEN** connector compilation fails before opening any broker connection

#### Scenario: Advertised capabilities match JetStream support
- **ID**: `nats.bind.capabilities`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::tests::nats_route_profile_excludes_headers_and_transactions`
- **WHEN** a session binds a valid NATS instance with publish and read routes
- **THEN** BIND exposes produce with peer acceptance and read with fetch, subscribe, single ACK, and requeue NACK, without headers or transactions

#### Scenario: Existing binding survives connector replacement
- **ID**: `nats.bind.generation-pinning`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::nats_replacement_preserves_bound_generation`
- **WHEN** a complete connector snapshot replaces the NATS configuration while an existing session remains bound
- **THEN** that session continues to use its original route generation while later binds use the replacement

#### Scenario: Adapter parity and direct application composition
- **ID**: `nats.bind.adapter-parity`
- **EVIDENCE**: `cargo-test::crates/fujin-runtime::nats_native_and_grpc_bind_profiles_match`
- **EVIDENCE**: `cargo-test::crates/fujin::application::tests::nats_application_builder_serves_native_bind_without_broker`
- **WHEN** native and gRPC clients bind the same NATS connector catalog and an application built with the NATS plugin serves native BIND
- **THEN** the adapters expose the same route capabilities, consistently reject unsupported headers and transactions, and direct application registration serves the NATS profile

### Requirement: Acknowledge JetStream publications
The NATS connector SHALL accept produce operations only for configured publish routes, SHALL complete each accepted operation exactly once after a successful or failed broker result, and SHALL make flush a snapshot barrier over previously accepted operations.

#### Scenario: Publish succeeds after broker acknowledgment
- **ID**: `nats.publish.peer-ack`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::publish_waits_for_jetstream_puback`
- **WHEN** a client publishes a payload to a subject captured by a configured JetStream stream
- **THEN** the operation completes successfully only after JetStream acknowledges the publication

#### Scenario: Rejected publication is not reported as accepted
- **ID**: `nats.publish.rejected`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::publish_broker_rejection_reports_failure_once`
- **WHEN** JetStream rejects an accepted publication, including a subject with no matching stream
- **THEN** its operation completes once with a failure rather than a successful produce response

#### Scenario: Flush waits only for earlier publications
- **ID**: `nats.publish.flush-barrier`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::flush_is_snapshot_barrier`
- **WHEN** flush is accepted after one publish and before another on the same writer
- **THEN** flush completes after the first publish is resolved without waiting for the later publish

### Requirement: Read from an existing durable pull consumer
The NATS connector SHALL use only pre-existing explicit-ack durable JetStream pull consumers, SHALL report a missing/incompatible consumer before subscription readiness or via the first fetch result without creating broker resources, SHALL return bounded fetch results, and SHALL report an asynchronous terminal failure when a ready subscription ends unexpectedly.

#### Scenario: Missing or incompatible consumer rejects reading
- **ID**: `nats.reader.existing-consumer`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::missing_or_incompatible_consumer_is_rejected`
- **WHEN** a read route names a missing stream or a consumer without explicit ACK and pull delivery
- **THEN** subscription fails before readiness or the first fetch reports failure without creating or modifying broker resources

#### Scenario: Fetch returns a bounded batch
- **ID**: `nats.reader.bounded-fetch`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::fetch_returns_at_most_requested_count`
- **WHEN** a client fetches up to a positive maximum from a configured consumer
- **THEN** its completion contains no more than the requested messages and can contain zero after the configured wait

#### Scenario: Subscription becomes ready before delivery
- **ID**: `nats.reader.subscription-readiness`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::subscribe_ready_precedes_first_message`
- **WHEN** a client subscribes to an existing NATS pull consumer with queued messages
- **THEN** the subscription reports readiness exactly once before delivering any message

### Requirement: Settle delivered messages through JetStream
The NATS connector SHALL scope manual-settlement IDs to their reader lease, acknowledge individual messages only after confirmed broker settlement, requeue NACKed messages, and prevent clients from settling auto-settled deliveries.

#### Scenario: Manual ACK confirms one message
- **ID**: `nats.settlement.single-ack`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::ack_confirms_only_selected_message`
- **WHEN** a client ACKs one of two outstanding deliveries from its reader
- **THEN** the selected message is broker-acknowledged without settling the other delivery

#### Scenario: Manual NACK requeues a message
- **ID**: `nats.settlement.requeue`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::nack_requeues_delivery`
- **WHEN** a client NACKs an outstanding delivery
- **THEN** the broker makes that message eligible for redelivery instead of recording an ACK

#### Scenario: Auto-settle does not expose a settleable ID
- **ID**: `nats.settlement.auto-mode`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::auto_settle_acknowledges_before_delivery`
- **WHEN** a client receives a message in auto-settle mode
- **THEN** it receives no settleable message ID and the broker message was acknowledged before Fujin delivered it to the client

### Requirement: Cleanly stop NATS connector resources
The NATS connector SHALL stop accepting work on close, resolve every previously accepted writer operation once, cancel reader pulls, invalidate closed-reader message IDs, and release generation-owned resources after its last binding closes.

#### Scenario: Close resolves pending work
- **ID**: `nats.lifecycle.pending-close`
- **EVIDENCE**: `cargo-test::plugins/connector/nats::close_resolves_pending_work_and_releases_resources`
- **WHEN** a session closes while accepted publications or pulls remain pending
- **THEN** accepted operations resolve exactly once and no reader or connection task remains owned by the closed generation
