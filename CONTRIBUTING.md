# Contributing to Fujin

Thank you for considering contributing to Fujin! This document provides guidelines and information for contributors.

## Development Setup

### Prerequisites

- **Go 1.24.2+** - Required for building
- **Make** - Cross-platform build automation (Windows/Linux/macOS)
- **Docker & Docker Compose** - For running message brokers locally
- **protoc** - Protocol Buffer compiler (optional, for regenerating proto files)

### Quick Setup

```bash
# 1. Clone the repository
git clone https://github.com/fujin-io/fujin.git
cd fujin

# 2. Build the project
make build

# 3. Run tests
make test

# 4. Start development environment (Kafka example)
make up-kafka
```

## Project Structure

```
fujin/
├── server/          # Server implementation (separate Go module)
│   ├── cmd/        # Main entry point
│   ├── internal/   # Internal server packages
│   └── public/     # Public API packages
├── api/            # gRPC API (separate Go module)
├── client/         # Client libraries
├── examples/       # Example applications
└── Makefile        # Cross-platform build commands
```

## Building

### All Platforms

The Makefile works on Windows, Linux, and macOS:

```bash
# Default build (all features)
make build

# Minimal build (Kafka only)
make build GO_BUILD_TAGS="kafka"

# With specific features
make build GO_BUILD_TAGS="kafka,grpc,observability"
```

### Manual Build

If you prefer not to use Make:

```bash
cd server
go build -tags="kafka,grpc,observability" -o ../bin/fujin ./cmd
```

## Build Tags

Fujin uses Go build tags for conditional compilation:

### Connector Tags
- `kafka` - Kafka support
- `nats_core` - NATS Core support
- `amqp091` - RabbitMQ (AMQP 0.9.1)
- `amqp10` - ActiveMQ, Azure Service Bus (AMQP 1.0)
- `resp_pubsub` - Redis PubSub
- `resp_streams` - Redis Streams
- `mqtt` - MQTT support
- `nsq` - NSQ support

### Feature Tags
- `observability` - Prometheus metrics & OpenTelemetry tracing
- `grpc` - gRPC server support

## Testing

```bash
# Run all tests
make test

# Run tests for specific package
cd server
go test -v -tags="kafka,grpc" ./internal/...

# Run benchmarks
make bench

# Custom benchmark
make bench BENCH_FUNC="BenchmarkMyFunction" BENCH_TIME="10s"
```

## Code Style

### Go Code

- Follow standard Go conventions
- Run `gofmt` before committing
- Use meaningful variable names
- Add comments for exported functions

## Adding New Connectors

To add support for a new message broker:

1. Create new directory: `server/public/connectors/impl/yourbroker/`
2. Implement `Reader` and `Writer` interfaces
3. Add build tag stub file: `init_yourbroker_stub.go`
4. Add actual implementation: `init_yourbroker.go` with build tag
5. Update `ALL_TAGS` in Makefile
6. Add tests
7. Update documentation

See existing connectors (e.g., `kafka/`, `nats/`) for reference.

## Generating Protocol Buffers

If you modify `.proto` files:

```bash
make generate
```

Or manually:
```bash
cd api
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       grpc/v1/fujin.proto
```

## Running Integration Tests

```bash
# Start broker (example: Kafka)
make up-kafka

# Run tests
cd server
go test -v -tags="kafka" ./test/...

# Clean up
make down-kafka
```

## Documentation

- Update relevant `README.md` files
- Add examples in `examples/` directory
- Update `QUICKSTART.md` for user-facing changes
- Comment complex code sections

## Pull Request Process

1. **Fork & Branch**
   - Fork the repository
   - Create feature branch: `git checkout -b feat/my-feature`

2. **Make Changes**
   - Write clean, tested code
   - Follow code style guidelines
   - Update documentation

3. **Test**
   - Run `make test`
   - Test on multiple platforms if possible
   - Verify builds with different tag combinations

4. **Commit**
   - Use conventional commit messages
   - Keep commits focused and atomic

5. **Submit PR**
   - Provide clear description
   - Reference related issues
   - Wait for review

## Cross-Platform Testing

If you're developing on Windows but want to ensure Linux/macOS compatibility:

- Use Docker for testing
- Avoid platform-specific APIs
- Test with different Go versions
- Use CI/CD results as reference

## Getting Help

- 💬 Open an issue for questions
- 📧 Contact maintainers
- 📖 Read existing documentation
- 🔍 Search closed issues

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

Thank you for contributing to Fujin! 🚀

