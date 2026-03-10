//go:build !quic

package quic

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

var ErrQUICNotCompiledIn = fmt.Errorf("QUIC transport is not compiled in")

type FujinServer struct {
	conf serverconfig.QUICServerConfig
	l    *slog.Logger
}

func NewFujinServer(conf serverconfig.QUICServerConfig, baseConfig connectorconfig.ConnectorsConfig, l *slog.Logger) *FujinServer {
	return &FujinServer{
		conf: conf,
		l:    l.With("server", "fujin"),
	}
}

func (s *FujinServer) ListenAndServe(ctx context.Context) error {
	if s.conf.Enabled {
		s.l.Error("QUIC transport is enabled but not compiled in - rebuild with 'quic' build tag")
		return ErrQUICNotCompiledIn
	}
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
