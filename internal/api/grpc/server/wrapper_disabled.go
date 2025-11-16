//go:build !grpc

package server

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/public/connectors"
	"github.com/fujin-io/fujin/public/server/config"
)

// GRPCServerWrapper wraps the gRPC server to implement the GRPCServer interface
// This is a stub implementation when gRPC is disabled
type GRPCServerWrapper struct {
	server *GRPCServer
}

// NewGRPCServerWrapper creates a new gRPC server wrapper (stub version)
func NewGRPCServerWrapper(conf config.GRPCServerConfig, baseConfig connectors.Config, l *slog.Logger) *GRPCServerWrapper {
	return &GRPCServerWrapper{
		server: NewGRPCServer(conf, baseConfig, l),
	}
}

// ListenAndServe starts the gRPC server (stub version)
func (w *GRPCServerWrapper) ListenAndServe(ctx context.Context) error {
	return w.server.ListenAndServe(ctx)
}

// Stop gracefully stops the gRPC server (stub version)
func (w *GRPCServerWrapper) Stop() {
	w.server.Stop()
}
