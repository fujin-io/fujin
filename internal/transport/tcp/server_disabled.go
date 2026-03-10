//go:build !tcp

package tcp

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

var ErrTCPNotCompiledIn = fmt.Errorf("TCP transport is not compiled in")

type Server struct {
	conf serverconfig.TCPServerConfig
	l    *slog.Logger
}

func NewServer(conf serverconfig.TCPServerConfig, baseConfig connectorconfig.ConnectorsConfig, l *slog.Logger) *Server {
	return &Server{
		conf: conf,
		l:    l.With("server", "fujin-tcp"),
	}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	if s.conf.Enabled {
		s.l.Error("TCP transport is enabled but not compiled in - rebuild with 'tcp' build tag")
		return ErrTCPNotCompiledIn
	}
	<-ctx.Done()
	return nil
}

func (s *Server) ReadyForConnections(timeout time.Duration) bool {
	return false
}

func (s *Server) Done() <-chan struct{} {
	return nil
}
