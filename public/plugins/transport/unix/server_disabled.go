//go:build !unix

package unix

import (
	"context"
	"log/slog"
	"time"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

// Server is a stub on non-Unix platforms.
type Server struct{}

func NewServer(_ serverconfig.UnixServerConfig, _ connectorconfig.ConnectorsConfig, _ *slog.Logger) *Server {
	return nil
}

func (s *Server) ListenAndServe(_ context.Context) error    { return nil }
func (s *Server) ReadyForConnections(_ time.Duration) bool  { return true }
func (s *Server) Done() <-chan struct{}                      { ch := make(chan struct{}); close(ch); return ch }
