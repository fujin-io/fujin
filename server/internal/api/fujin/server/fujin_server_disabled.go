//go:build !fujin

package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ValerySidorin/fujin/internal/connectors"
	"github.com/ValerySidorin/fujin/public/server/config"
)

var ErrFujinNotCompiledIn = fmt.Errorf("fujin protocol is not compiled in")

// FujinServer stub implementation when fujin protocol is disabled
type FujinServer struct {
	conf config.FujinServerConfig
	l    *slog.Logger
}

// NewFujinServer creates a stub Fujin server instance
func NewFujinServer(conf config.FujinServerConfig, cman *connectors.Manager, l *slog.Logger) *FujinServer {
	return &FujinServer{
		conf: conf,
		l:    l.With("server", "fujin"),
	}
}

// ListenAndServe returns an error indicating fujin protocol is not compiled in
func (s *FujinServer) ListenAndServe(ctx context.Context) error {
	if s.conf.Enabled {
		s.l.Error("Fujin (QUIC) server is enabled but not compiled in - rebuild with 'fujin' build tag")
		return ErrFujinNotCompiledIn
	}
	// If not enabled, just wait for context cancellation
	<-ctx.Done()
	return nil
}

// Stop does nothing in stub implementation
func (s *FujinServer) Stop() {
	// no-op
}

// ReadyForConnections always returns false in stub implementation
func (s *FujinServer) ReadyForConnections(timeout time.Duration) bool {
	return false
}

// Done returns a nil channel in stub implementation
func (s *FujinServer) Done() <-chan struct{} {
	return nil
}
