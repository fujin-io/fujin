# Getting started

**Use this page to:** run Fujin with Kafka and send the first message through the supplied Go client. Start here; use [Configuration](configuration.md) to change the setup and [Operations](operations.md) to run it beyond the first test.

## Prerequisites

- Docker with the `docker compose` command;
- Go 1.25.1 or newer for the client examples;
- a local checkout of this repository.

Run the following commands from the repository root. The local stack contains three Kafka brokers, ZooKeeper, and Fujin with native QUIC/TCP listeners, gRPC, and HTTP health checks.

## 1. Start the stack

```bash
docker compose \
  -f resources/docker-compose.kafka.yaml \
  -f resources/docker-compose.fujin-kafka.yaml \
  up -d --build --wait
```

The equivalent shortcut is:

```bash
make up-kafka-fujin
```

The stack exposes:

| Purpose | Address |
| --- | --- |
| native QUIC | `localhost:4848/udp` |
| gRPC | `localhost:4849` |
| native TCP | `localhost:4850` |
| liveness/readiness | `localhost:8080` |

Check listener readiness:

```bash
curl --fail http://localhost:8080/readyz
```

An `ok` response means that every configured Fujin listener has bound its address. It does not test Kafka connectivity: Fujin creates a Kafka client lazily when a route is first used.

## 2. Start the producer

In a separate terminal, from the repository root:

```bash
cd sdk/go/client
go run ./examples/producer
```

The producer connects to `localhost:4848` over QUIC, sends `BIND` for the connector instance named `connector`, and writes to route `pub` once per second. The local example deliberately skips verification of its self-signed certificate; do not use that TLS setting in production.

The first message creates the Kafka topic `my_pub_topic` because this development configuration enables `allow.auto.create.topics`.

## 3. Start the consumer

Keep the producer running. In another terminal, run:

```bash
cd sdk/go/client
go run ./examples/consumer
```

The consumer binds to the same `connector` instance, fetches from route `sub`, and prints payloads and headers. Routes `pub` and `sub` use the same Kafka topic but expose different operations to the client.

Here, `connector` is the configured **instance name** passed to `BIND`; `kafka` is the statically linked plugin type. `pub` and `sub` are route names within the instance. See [`resources/assets/config-kafka.yaml`](../resources/assets/config-kafka.yaml) for the complete stack configuration and the [`producer`](../sdk/go/client/examples/producer/main.go) and [`consumer`](../sdk/go/client/examples/consumer/main.go) sources for the exact client calls.

## 4. Stop the stack

Stop the Go examples with Ctrl-C, then remove the containers and volumes:

```bash
docker compose \
  -f resources/docker-compose.kafka.yaml \
  -f resources/docker-compose.fujin-kafka.yaml \
  down -v --remove-orphans
```

The equivalent shortcut is:

```bash
make down-kafka-fujin
```

## Next steps

- [Configuration](configuration.md) — choose a configurator, listeners, and Kafka routes.
- [Client interfaces](interfaces.md) — choose native v1, gRPC, or the Go SDK.
- [Go client guide](../sdk/go/client/README.md) — TLS, capabilities, producing, consuming, subscriptions, and transactions.
- [Complete local Kafka stack guide](../resources/README.md) — Compose layout and Kafka-only commands.
- [Native v1 protocol](../protocol.md) and [gRPC protobuf schema](../crates/fujin-grpc-proto/proto/fujin.proto) — the authoritative protocol contracts.
