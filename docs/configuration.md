# Configuration

**Use this page to:** assemble one bootstrap document, choose how Fujin loads it, and configure Kafka or NATS JetStream and listeners. Follow [Getting started](getting-started.md) first if you have not run Fujin yet. This page maps the settings without duplicating the full reference.

## Minimal working document

```yaml
fujin:
  transports:
    - type: tcp
      settings:
        addr: 0.0.0.0:4850
grpc:
  enabled: false
health:
  enabled: true
  addr: 0.0.0.0:8080
connectors:
  primary:
    type: kafka
    settings:
      common:
        brokers: [localhost:9092]
        properties: {}
      routes:
        events:
          produce_topic: events
          consume_topics: [events]
          group: my-app
```

This example expects Kafka on `localhost:9092`, enables native TCP, and exposes HTTP health checks. A client sends `BIND("primary")`, then uses route `events`. The name `primary` selects a configured instance; `type: kafka` selects its plugin implementation.

## Load the document

`FUJIN_CONFIGURATOR` is required by the standard binary and selects one of its two built-in configurator plugins.

### File

```bash
FUJIN_CONFIGURATOR=file \
FUJIN_CONFIGURATOR_FILE_PATHS=./config.yaml \
  ./bin/fujin
```

`FUJIN_CONFIGURATOR_FILE_PATHS` is a comma-separated search list. Fujin loads the **first existing** path. A read or parse error for that file is terminal; it does not fall through to the next path. Without the variable, the search order is `./config.yaml`, `conf/config.yaml`, then `config/config.yaml`. Both JSON and YAML are accepted.

### Environment variable

```bash
FUJIN_CONFIGURATOR=env \
FUJIN_CONFIGURATOR_ENV_CONFIG='{"fujin":{"transports":[{"type":"tcp","settings":{"addr":"127.0.0.1:4850"}}]},"grpc":{"enabled":false},"connectors":{}}' \
  ./bin/fujin
```

`FUJIN_CONFIGURATOR_ENV_CONFIG` contains the complete JSON or YAML document; it is not a prefix for separate field variables. Secrets placed there have the same process-environment exposure as other environment variables, so choose a delivery method appropriate to your runtime.

The source behavior is defined by [`configurator/file`](../plugins/configurator/file/src/lib.rs) and [`configurator/env`](../plugins/configurator/env/src/lib.rs).

## Document hierarchy

| Section | Purpose |
| --- | --- |
| `fujin.transports[]` | Native listeners. Each entry has `type`, optional `enabled` (default `true`), and transport-specific `settings`. |
| `grpc` | A separate gRPC listener. It is not a `fujin.transports` entry. `enabled` defaults to `true`, so explicitly disable it when no gRPC address is supplied. |
| `health` | A separate HTTP `/healthz` and `/readyz` listener; disabled by default. |
| `connectors.<name>` | Client-visible connector instances. Each has `type`, `settings`, and optionally an `overridable` whitelist and middleware. |

Unknown fields in gRPC, TLS, native protocol, and built-in transport settings are rejected. See the root [Configuration section](../README.md#configuration) for the extended document shape. Working examples are [`config.dev.yaml`](../config.dev.yaml), the [container deployment configuration](../examples/assets/config/config.deployment.example.yaml), and the [local Kafka stack configuration](../resources/assets/config-kafka.yaml).

## Kafka instances and routes

The standard application includes `kafka` and `nats` broker connector types. Kafka `settings` have common and per-route levels:

```yaml
connectors:
  primary:                 # the client passes this name to BIND
    type: kafka
    settings:
      common:
        brokers: [kafka-1:9092, kafka-2:9092]
        properties:
          security.protocol: SASL_SSL
      routes:
        publish:
          produce_topic: events
          properties: {}
        consume:
          consume_topics: [events]
          group: event-workers
          properties:
            auto.offset.reset: earliest
        transactional:
          produce_topic: audit
          transactional_id: audit-writer
```

- `common.brokers` must contain non-empty addresses, and `routes` must not be empty.
- Each route needs a non-empty `produce_topic`, non-empty `consume_topics`, or both.
- A consuming route requires a non-empty `group`. `transactional_id` is valid only with `produce_topic`.
- `common.properties` and `routes.<name>.properties` are passed to librdkafka as string settings. Route settings are applied after common settings and therefore win for the same key.
- Compilation validates the document locally without broker I/O. Producers and consumers are opened lazily when a bound session first uses the route.

Fujin reports these capabilities to the client after `BIND`:

| Route fields | Reported capabilities |
| --- | --- |
| `produce_topic` | `PRODUCE` and headers; the reported produce acceptance guarantee is `durable_accept` |
| `consume_topics` + `group` | `SUBSCRIBE`, `FETCH`, headers, and manual settlement; ACK is cumulative and NACK is unsupported |
| `produce_topic` + `transactional_id` | Transactions in addition to produce |
| produce and consume fields together | The union of those capabilities |

Clients should inspect the returned route profile rather than infer capabilities from a route name. The [protocol capability contract](../protocol.md#route-capabilities-and-guarantees) and [Go SDK capability guide](../sdk/go/client/README.md#bind-and-route-capabilities) are authoritative. The Kafka schema and behavior live in [`plugins/connector/kafka`](../plugins/connector/kafka/src/lib.rs).

`overridable` lets a client modify only explicitly whitelisted settings paths during `BIND`; without a whitelist, overrides are rejected. See the protocol's [BIND section](../protocol.md#bind) for the wire contract. Keep the whitelist narrow: an overridden binding compiles a private derived generation from the immutable base configuration.

## NATS JetStream instances and routes

The `nats` connector uses JetStream, not Core NATS. Provision the stream and a **durable pull consumer with explicit ACK** outside Fujin; Fujin does not create or modify either. A publishing subject must match a provisioned stream for PubAck-backed success. For example:

```yaml
connectors:
  jetstream:             # BIND("jetstream")
    type: nats
    settings:
      common:
        servers: ["nats://nats:4222"]
        credentials_file: /run/secrets/fujin-nats.creds # optional
      routes:
        events:
          publish_subject: events.created
          stream: EVENTS
          consumer: event_workers
```

`common.servers` requires at least one `nats://` or `tls://` URL; `routes` cannot be empty. Each route needs `publish_subject`, both `stream` and `consumer`, or all three. The subject has no wildcards. The optional `credentials_file` is a path to a NATS `.creds` file read on first connection; mount it as a protected secret, do not include its contents in the document, and ensure the Fujin process can read it. TLS and broker authorization must be configured on the NATS side as appropriate. Compilation/BIND validates only local syntax and never contacts the broker: authentication, missing resources, and incompatible consumer settings surface on first broker use or subscription readiness.

Publish completes only after a JetStream PubAck and advertises `peer_accept`, not `durable_accept` (stream persistence and replication are external policy). Read routes advertise `FETCH`, `SUBSCRIBE`, single-message ACK, and requeue NACK. Auto-settle confirms the broker ACK before delivery. NATS routes do **not** support Fujin headers or transactions; native and gRPC clients must inspect the BIND profile. Reader IDs are scoped to their reader lease. Explicit-ACK durable pull consumers are shared broker resources: provision separate consumers for independent workloads and choose delivery/retention policies accordingly.

The full `fujin-app` feature set includes NATS. A smaller binary must enable `connector-nats`; a custom composition must register `fujin_connector_nats::plugin()` explicitly (see [Extending Fujin](extending.md#compose-directly-with-rust)). The local test broker is described in [resources](../resources/README.md#nats-jetstream-test-broker).

## Listeners

### Native transports

Each `fujin.transports[]` entry creates one listener of a registered type:

| `type` | Main settings | Constraints |
| --- | --- | --- |
| `tcp` | `addr`, `tls`, `tcp_keepalive`, `fujin` | TLS is optional; mutual TLS is supported. |
| `quic` | `addr`, `tls`, stream/window limits, idle timeout/keepalive, `fujin` | Requires `tls.enabled: true`. |
| `websocket` | `addr`, `path` (default `/fujin`), `allowed_origins`, `max_message_bytes`, `tls`, `fujin` | Accepts binary messages only; an empty Origin allowlist allows any Origin. |
| `unix` | `path`, `fujin` | Unix only. The path must be available at bind time; normal shutdown removes the socket file unless its listener was handed to a replacement process. |

The shared `tls` block provides PEM certificate and key paths. Client certificate enforcement requires both `require_and_verify_client_cert: true` and a non-empty `client_certs_dir`. The shared `fujin` block controls native-protocol PING/PONG, bounded output, write deadlines, and forced session termination.

Use the source definitions as the full field reference: [`tcp`](../plugins/transport/tcp/src/lib.rs), [`quic`](../plugins/transport/quic/src/lib.rs), [`websocket`](../plugins/transport/websocket/src/lib.rs), [`unix`](../plugins/transport/unix/src/lib.rs), and [shared native/TLS settings](../crates/fujin-transport/src/settings.rs). The native wire contract is [`protocol.md`](../protocol.md).

### gRPC and health

`grpc` is configured at the document root with `addr`, TLS, timeout, message/stream limits, and HTTP/2 keepalive, window, and connection-age settings. The RPC contract is the [protobuf schema](../crates/fujin-grpc-proto/proto/fujin.proto); [`GrpcConfig`](../crates/fujin-configurator/src/config.rs) is the complete settings definition.

`health.enabled: true` requires `health.addr`. This HTTP listener serves only liveness and readiness, not metrics or an application API. See [Health checks](operations.md#health-checks) for its operational meaning.

## Before starting

1. Every configured `type` must be statically linked and registered in the binary. An unregistered plugin is rejected before any listener binds.
2. Configure at least one listener: a native transport, gRPC, or health.
3. Ensure each Kafka address is reachable **from the Fujin process**. In Compose this is normally a service name, not `localhost`.
4. Use verifiable production TLS instead of the self-signed, verification-skipping setup from the local example.
5. After changing the document, decide whether connector-only reload is sufficient or bootstrap listeners require a restart; see [Connector reload](operations.md#connector-reload).
