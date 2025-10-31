package main

import (
	"context"
	"os/signal"
	"syscall"

	_ "github.com/ValerySidorin/fujin/server/public/connectors/all"
	"github.com/ValerySidorin/fujin/server/public/service"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	service.RunCLI(ctx)
}
