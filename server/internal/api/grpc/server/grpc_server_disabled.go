//go:build !grpc

package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/internal/connectors"
	"github.com/ValerySidorin/fujin/public/server/config"
)

var ErrGRPCNotCompiledIn = fmt.Errorf("grpc is not compiled in")

// GRPCServer stub implementation when gRPC is disabled
type GRPCServer struct {
	conf config.GRPCServerConfig
	l    *slog.Logger
}

// NewGRPCServer creates a stub gRPC server instance
func NewGRPCServer(conf config.GRPCServerConfig, cman *connectors.Manager, l *slog.Logger) *GRPCServer {
	return &GRPCServer{
		conf: conf,
		l:    l.With("server", "grpc"),
	}
}

// ListenAndServe returns an error indicating gRPC is not compiled in
func (s *GRPCServer) ListenAndServe(ctx context.Context) error {
	if s.conf.Enabled {
		s.l.Error("gRPC server is enabled but not compiled in - rebuild with 'grpc' build tag")
		return ErrGRPCNotCompiledIn
	}
	// If not enabled, just wait for context cancellation
	<-ctx.Done()
	return nil
}

// Stop does nothing in stub implementation
func (s *GRPCServer) Stop() {
	// no-op
}
