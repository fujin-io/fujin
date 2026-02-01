package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/fujin-io/fujin/public/service"

	// Import all connector plugins at once:
	// _ "github.com/fujin-io/fujin/public/plugins/connector/all"

	// Or import specific connector plugins individually:
	// _ "github.com/fujin-io/fujin/public/plugins/connector/kafka"

	// Note: For standard plugins, you need to build with the appropriate tags.
	// Available tags: [kafka nats_core amqp091 amqp10 resp_pubsub redis_streams mqtt nsq]
	// Example: go build -tags kafka ...

	// Import your custom connector plugin:
	_ "github.com/fujin-io/fujin/examples/plugins/connector/faker"
	_ "github.com/fujin-io/fujin/examples/plugins/middleware/connector/ratelimit"
	_ "github.com/fujin-io/fujin/public/plugins/configurator/file"
)

// This example demonstrates how to run a Fujin server with custom plugins.
// It accepts an optional config file path as an argument.
// Run from repo root: export FUJIN_CONFIGURATOR=file && export FUJIN_CONFIGURATOR_FILE_PATHS=./examples/plugins/config.yaml && go run -tags fujin examples/plugins/main.go
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	service.RunCLI(ctx)
}
