//go:build grpc

package server

import (
	"context"
	"log/slog"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

// GRPCServerWrapper wraps the gRPC server to implement the GRPCServer interface
type GRPCServerWrapper struct {
	server *GRPCServer
}

// NewGRPCServerWrapper creates a new gRPC server wrapper
func NewGRPCServerWrapper(conf serverconfig.GRPCServerConfig, baseConfig connectorconfig.ConnectorsConfig, l *slog.Logger) *GRPCServerWrapper {
	return &GRPCServerWrapper{
		server: NewGRPCServer(conf, baseConfig, l),
	}
}

// ListenAndServe starts the gRPC server
func (w *GRPCServerWrapper) ListenAndServe(ctx context.Context) error {
	return w.server.ListenAndServe(ctx)
}

// Stop gracefully stops the gRPC server
func (w *GRPCServerWrapper) Stop() {
	w.server.Stop()
}
