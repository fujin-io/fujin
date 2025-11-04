package main

import (
	"context"
	"os/signal"
	"syscall"

	_ "github.com/fujin-io/fujin/public/connectors/all"
	"github.com/fujin-io/fujin/public/service"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	service.RunCLI(ctx)
}
