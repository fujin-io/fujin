# Client interfaces

**Use this page to choose how to connect.** For exact fields and API signatures, follow the authoritative references below. Both interfaces use the same Session Core semantics, but their wire formats differ.

## Choose an interface

| Need | Interface | Read next |
| --- | --- | --- |
| A native stream over TCP, QUIC, WebSocket, or a Unix socket | Fujin v1 | [Frame and operation specification](../protocol.md) |
| Protobuf/gRPC and standard gRPC infrastructure | `fujin.v1.FujinService` | [Protobuf source](../crates/fujin-grpc-proto/proto/fujin.proto) |
| A ready-to-use Go network client | Native QUIC or gRPC | [Go client guide](../sdk/go/client/README.md) |
| Control a Fujin server inside a Go process | Go embedding over the C ABI, **not** a network client | [Go embedding guide](../sdk/go/embed/README.md) |
| Embed Fujin in a Rust application | `fujin::Application` / `EmbeddedApplication` | [Extending Fujin](extending.md), [Rust example](../crates/fujin/examples/embed.rs) |

## Shared interaction model

1. A native session starts with `HELLO` to negotiate `fujin/1`, then `BIND`. The first operation on a gRPC stream is `BIND`; it does not use native `HELLO`.
2. `BIND` selects a configured **connector instance name** from `connectors` and returns its route profiles. That name is not the plugin type (`kafka`). A successful BIND establishes local validity, not broker availability.
3. The client selects a **route** for produce, fetch, or subscribe and checks its advertised capabilities. Headers, transactions, and manual settlement are not available on every route.
4. `PRODUCE`/`HPRODUCE`, `FETCH`/`HFETCH`, subscriptions, and `ACK`/`NACK` all use Session Core semantics. A transaction starts on one route and ends with commit or rollback. Close subscriptions and sessions when done.

Operation errors include status code, `outcome`, stable `reason`, message, and details. An `unknown` outcome proves neither success nor rollback; do not base a retry solely on error text. Produce guarantees and ACK/NACK behavior belong to the route profile. See [route capabilities in the protocol](../protocol.md#route-capabilities-and-guarantees) and [Go client error handling](../sdk/go/client/README.md#structured-operation-errors).

## Transport boundaries

- One TCP, WebSocket, or Unix socket connection carries one native session; on QUIC, one bidirectional stream carries one session. QUIC requires TLS and ALPN `fujin`.
- WebSocket accepts binary messages only. WebSocket message boundaries do **not** define Fujin frame boundaries; the native decoder consumes an arbitrarily fragmented byte stream.
- gRPC has its own protobuf wire format, not native frames. Configure its listener address and TLS separately from native listeners.

Implementing another SDK or transport? Use the [complete native v1 specification](../protocol.md) or [protobuf schema](../crates/fujin-grpc-proto/proto/fujin.proto), not examples as a substitute for the contract.
