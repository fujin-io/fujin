//go:build tcp

package tcp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	fujin "github.com/fujin-io/fujin/internal/protocol/fujin"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

type Server struct {
	conf       serverconfig.TCPServerConfig
	baseConfig connectorconfig.ConnectorsConfig

	ready chan struct{}
	done  chan struct{}

	l *slog.Logger
}

func NewServer(conf serverconfig.TCPServerConfig, baseConfig connectorconfig.ConnectorsConfig, l *slog.Logger) *Server {
	return &Server{
		conf:       conf,
		baseConfig: baseConfig,
		ready:      make(chan struct{}),
		done:       make(chan struct{}),
		l:          l.With("server", "fujin_tcp"),
	}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	var (
		ln  net.Listener
		err error
	)

	if s.conf.TLS != nil {
		ln, err = tls.Listen("tcp", s.conf.Addr, s.conf.TLS)
	} else {
		ln, err = net.Listen("tcp", s.conf.Addr)
	}
	if err != nil {
		return fmt.Errorf("listen tcp: %w", err)
	}

	connWg := &sync.WaitGroup{}

	defer func() {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.l.Error("close tcp listener", "err", err)
		}

		timeout := time.After(30 * time.Second)
		done := make(chan struct{})
		go func() {
			connWg.Wait()
			close(done)
		}()

		select {
		case <-timeout:
			s.l.Error("closing tcp listener after timeout")
		case <-done:
			s.l.Info("closing tcp listener after all connections done")
		}

		close(s.done)
		s.l.Info("fujin tcp server stopped")
	}()

	close(s.ready)
	s.l.Info("fujin tcp server started", "addr", ln.Addr())

	// Close listener when context is cancelled to unblock Accept
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
			s.l.Error("accept tcp conn", "err", err)
			continue
		}

		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(30 * time.Second)
			_ = tc.SetNoDelay(true)
		}

		connWg.Add(1)
		go func() {
			defer func() {
				conn.Close()
				connWg.Done()
			}()

			fujin.HandleStream(ctx, conn, fujin.StreamOptions{
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
