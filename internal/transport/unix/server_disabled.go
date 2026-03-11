//go:build !unix

package unix

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

var ErrUnixNotCompiledIn = fmt.Errorf("Unix transport is not compiled in")

type Server struct {
	conf serverconfig.UnixServerConfig
	l    *slog.Logger
}

func NewServer(conf serverconfig.UnixServerConfig, baseConfig connectorconfig.ConnectorsConfig, l *slog.Logger) *Server {
	return &Server{
		conf: conf,
		l:    l.With("server", "fujin-unix"),
	}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	if s.conf.Enabled {
		s.l.Error("Unix transport is enabled but not compiled in - rebuild with 'unix' build tag")
		return ErrUnixNotCompiledIn
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
