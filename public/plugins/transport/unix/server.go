//go:build unix

package unix

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	_ "github.com/fujin-io/fujin/internal/proto"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/handler"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/session"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

type Server struct {
	conf       serverconfig.UnixServerConfig
	baseConfig connectorconfig.ConnectorsConfig

	ready chan struct{}
	done  chan struct{}

	l *slog.Logger
}

func NewServer(conf serverconfig.UnixServerConfig, baseConfig connectorconfig.ConnectorsConfig, l *slog.Logger) *Server {
	return &Server{
		conf:       conf,
		baseConfig: baseConfig,
		ready:      make(chan struct{}),
		done:       make(chan struct{}),
		l:          l.With("server", "fujin_unix"),
	}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	path := s.conf.Path
	if path == "" {
		return fmt.Errorf("unix: path is required")
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("unix: remove stale socket: %w", err)
	}

	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("unix: listen: %w", err)
	}

	connWg := &sync.WaitGroup{}

	defer func() {
		ln.Close()
		os.Remove(path)

		timeout := time.After(30 * time.Second)
		doneCh := make(chan struct{})
		go func() {
			connWg.Wait()
			close(doneCh)
		}()

		select {
		case <-timeout:
			s.l.Error("closing unix listener after timeout")
		case <-doneCh:
			s.l.Info("closing unix listener after all connections done")
		}

		close(s.done)
		s.l.Info("fujin unix server stopped")
	}()

	close(s.ready)
	s.l.Info("fujin unix server started", "path", path)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.l.Error("accept unix conn", "err", err)
			continue
		}

		connWg.Add(1)
		go func() {
			defer func() {
				conn.Close()
				connWg.Done()
			}()

			handler.HandleStream(ctx, conn, session.StreamOptions{
				BaseConfig:            s.baseConfig,
				PingInterval:          s.conf.Fujin.PingInterval,
				PingTimeout:           s.conf.Fujin.PingTimeout,
				WriteDeadline:         s.conf.Fujin.WriteDeadline,
				ForceTerminateTimeout: s.conf.Fujin.ForceTerminateTimeout,
				AbortRead:             func() { conn.SetReadDeadline(time.Now()) },
				CloseRead:             func() { conn.SetReadDeadline(time.Now()) },
				Logger:                s.l,
			})
		}()
	}
}

func (s *Server) ReadyForConnections(timeout time.Duration) bool {
	select {
	case <-time.After(timeout):
		return false
	case <-s.ready:
		return true
	}
}

func (s *Server) Done() <-chan struct{} {
	return s.done
}
