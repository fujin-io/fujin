# Makefile for Fujin

APP_NAME := fujin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

ALL_TAGS = grpc,fujin

GO_BUILD_TAGS ?= ${ALL_TAGS}

BENCH_TIME ?= 1000000x
BENCH_FUNC ?= Benchmark_Produce_1BPayload_RedisPubSub

# Detect OS
ifeq ($(OS),Windows_NT)
    DETECTED_OS := Windows
    BINARY_EXT := .exe
    RM := del /Q /F
    RMDIR := rmdir /S /Q
    MKDIR := mkdir
    PATHSEP := \\
	BOOTCONF := setx FUJIN_CONFIGURATOR "file" && setx FUJIN_CONFIGURATOR_FILE_PATHS "./config.dev.yaml"
else
    DETECTED_OS := $(shell uname -s)
    BINARY_EXT :=
    RM := rm -f
    RMDIR := rm -rf
    MKDIR := mkdir -p
    PATHSEP := /
	BOOTCONF := export FUJIN_CONFIGURATOR=file && export FUJIN_CONFIGURATOR_FILE_PATHS=./config.dev.yaml
endif

BIN_DIR := bin
BINARY := $(BIN_DIR)$(PATHSEP)$(APP_NAME)$(BINARY_EXT)

.PHONY: all
all: clean build run

.PHONY: build
build:
	@echo "==> Building ${APP_NAME} for ${DETECTED_OS} (Version: ${VERSION}, Tags: [${GO_BUILD_TAGS}])"
	@go build -tags=${GO_BUILD_TAGS} -ldflags "-s -w -X main.Version=${VERSION}" -o ./$(BINARY) ./cmd/...
	@echo "==> Binary created: $(BINARY)"

.PHONY: clean
clean:
	@echo "==> Cleaning"
ifeq ($(OS),Windows_NT)
	@if exist $(BIN_DIR) $(RMDIR) $(BIN_DIR) 2>nul
else
	@$(RMDIR) $(BIN_DIR)
endif

.PHONY: run
run:
	@echo "==> Running"
	@$(BOOTCONF) && $(BINARY)

.PHONY: generate
generate:
	@echo "==> Generating gRPC code"
	@cd public/proto && protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative grpc/v1/fujin.proto

.PHONY: test
test:
	@echo "==> Running tests"
	@cd server && go test -v -tags=${GO_BUILD_TAGS} ./...

.PHONY: help
help:
	@echo "Fujin Makefile ($(DETECTED_OS))"
	@echo ""
	@echo "Usage:"
	@echo "  make build [GO_BUILD_TAGS=\"tag1,tag2\"]  Build binary. Default GO_BUILD_TAGS=\"$(ALL_TAGS)\"."
	@echo "  make run [BOOTSTRAP=\"path/to/bootstrap.yaml\"]  Run binary with bootstrap config."
	@echo "  make clean                             Remove build artifacts."
	@echo "  make test                              Run all tests."
	@echo "  make bench                             Run benchmarks."
	@echo ""
	@echo "Config Loading Priority:"
	@echo "  1. Environment variable FUJIN_CONFIGURATOR (if set)"
	@echo "  2. BOOTSTRAP parameter or ./bootstrap.dev.yaml (default) or ./config.bootstrap.yaml or ./bootstrap.yaml (if exists)"
	@echo "  3. Default file paths: ./config.yaml, conf/config.yaml, config/config.yaml"
	@echo ""
	@echo "Variables:"
	@echo "  VERSION (default: git describe || dev) Version tag for builds."
	@echo "  GO_BUILD_TAGS (default: all features)  Comma-separated Go build tags."
	@echo "  BOOTSTRAP (optional)                   Path to bootstrap config file."
	@echo "  FUJIN_CONFIGURATOR (optional)         Config loader type (e.g., vault, file)."
	@echo "  FUJIN_BOOTSTRAP_CONFIG (optional)      Path to bootstrap config (env var)."
	@echo ""
	@echo "Platform: $(DETECTED_OS)"
	@echo "Binary: $(BINARY)"

# Broker management commands
.PHONY: up-kafka down-kafka up-nats down-nats up-rabbitmq down-rabbitmq up-artemis down-artemis up-emqx down-emqx up-nsq down-nsq

# Kafka
up-kafka:
	docker compose -f resources/docker-compose.fujin.kafka.yaml -f resources/docker-compose.kafka.yaml -f resources/docker-compose.observability.yaml up -d

down-kafka:
	docker compose -f resources/docker-compose.fujin.kafka.yaml -f resources/docker-compose.kafka.yaml -f resources/docker-compose.observability.yaml down

# NATS
up-nats_core:
	docker compose -f resources/docker-compose.fujin.nats_core.yaml -f resources/docker-compose.nats_core.yaml -f resources/docker-compose.observability.yaml up -d

down-nats_core:
	docker compose -f resources/docker-compose.fujin.nats_core.yaml -f resources/docker-compose.nats_core.yaml -f resources/docker-compose.observability.yaml down

# RabbitMQ
up-amqp091:
	docker compose -f resources/docker-compose.fujin.amqp091.yaml -f resources/docker-compose.rabbitmq.yaml -f resources/docker-compose.observability.yaml up -d

down-amqp091:
	docker compose -f resources/docker-compose.fujin.amqp091.yaml -f resources/docker-compose.rabbitmq.yaml -f resources/docker-compose.observability.yaml down

# ActiveMQ Artemis
up-amqp10:
	docker compose -f resources/docker-compose.fujin.amqp10.yaml -f resources/docker-compose.artemis.yaml -f resources/docker-compose.observability.yaml up -d

down-amqp10:
	docker compose -f resources/docker-compose.fujin.amqp10.yaml -f resources/docker-compose.artemis.yaml -f resources/docker-compose.observability.yaml down

# EMQX
up-mqtt:
	docker compose -f resources/docker-compose.fujin.mqtt.yaml -f resources/docker-compose.emqx.yaml -f resources/docker-compose.observability.yaml up -d

down-mqtt:
	docker compose -f resources/docker-compose.fujin.mqtt.yaml -f resources/docker-compose.emqx.yaml -f resources/docker-compose.observability.yaml down
# Redis (e.g. ValKey)
up-resp_pubsub:
	docker compose -f resources/docker-compose.fujin.resp_pubsub.yaml -f resources/docker-compose.valkey.yaml -f resources/docker-compose.observability.yaml up -d

down-resp_pubsub:
	docker compose -f resources/docker-compose.fujin.resp_pubsub.yaml -f resources/docker-compose.valkey.yaml -f resources/docker-compose.observability.yaml down

up-resp_streams:
	docker compose -f resources/docker-compose.fujin.resp_streams.yaml -f resources/docker-compose.valkey.yaml -f resources/docker-compose.observability.yaml up -d

down-resp_streams:
	docker compose -f resources/docker-compose.fujin.resp_streams.yaml -f resources/docker-compose.valkey.yaml -f resources/docker-compose.observability.yaml down


# NSQ
up-nsq:
	docker compose -f resources/docker-compose.fujin.nsq.yaml -f resources/docker-compose.nsq.yaml -f resources/docker-compose.observability.yaml up -d

down-nsq:
	docker compose -f resources/docker-compose.fujin.nsq.yaml -f resources/docker-compose.nsq.yaml -f resources/docker-compose.observability.yaml down

# Helper command to show all available broker commands
broker-help:
	@echo "Available broker commands:"
	@echo "  make up-kafka       - Start Kafka cluster"
	@echo "  make down-kafka     - Stop Kafka cluster"
	@echo "  make up-nats_core   - Start NATS server"
	@echo "  make down-nats_core - Stop NATS server"
	@echo "  make up-rabbitmq    - Start RabbitMQ server"
	@echo "  make down-rabbitmq  - Stop RabbitMQ server"
	@echo "  make up-artemis     - Start ArtemisMQ server"
	@echo "  make down-artemis   - Stop ArtemisMQ server"
	@echo "  make up-emqx        - Start EMQX server"
	@echo "  make down-emqx      - Stop EMQX server"
	@echo "  make up-redis       - Start Redis server (ValKey)"
	@echo "  make down-redis     - Stop Redis server (ValKey)"
	@echo "  make up-nsq         - Start NSQ cluster"
	@echo "  make down-nsq       - Stop NSQ cluster"

.PHONY: bench
bench:
	@cd server && go test -bench=${BENCH_FUNC} -benchtime=${BENCH_TIME} -tags=${GO_BUILD_TAGS} ./test