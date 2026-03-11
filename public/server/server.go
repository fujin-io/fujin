package server

import (
	"context"
	"log/slog"
	"time"

	grpc_server "github.com/fujin-io/fujin/internal/transport/grpc/v1/server"
	quicserver "github.com/fujin-io/fujin/internal/transport/quic"
	tcpserver "github.com/fujin-io/fujin/internal/transport/tcp"
	unixserver "github.com/fujin-io/fujin/internal/transport/unix"
	"github.com/fujin-io/fujin/public/server/config"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	conf config.Config

	quicServer TransportServer
	tcpServer  TransportServer
	unixServer TransportServer
	grpcServer GRPCServer

	l *slog.Logger
}

// TransportServer is the common interface for fujin-protocol transports (QUIC, TCP).
type TransportServer interface {
	ListenAndServe(ctx context.Context) error
	ReadyForConnections(timeout time.Duration) bool
	Done() <-chan struct{}
}

// GRPCServer interface for optional gRPC server
type GRPCServer interface {
	ListenAndServe(ctx context.Context) error
	Stop()
}

// NewServer creates a new server instance
func NewServer(conf config.Config, l *slog.Logger) (*Server, error) {
	conf.SetDefaults()

	s := &Server{
		conf: conf,
		l:    l,
	}

	if conf.QUIC.Enabled {
		s.quicServer = quicserver.NewFujinServer(s.conf.QUIC, s.conf.Connectors, s.l)
	}

	if conf.TCP.Enabled {
		s.tcpServer = tcpserver.NewServer(s.conf.TCP, s.conf.Connectors, s.l)
	}

	if conf.Unix.Enabled {
		s.unixServer = unixserver.NewServer(s.conf.Unix, s.conf.Connectors, s.l)
	}

	if conf.GRPC.Enabled {
		s.grpcServer = grpc_server.NewGRPCServerWrapper(s.conf.GRPC, s.conf.Connectors, s.l)
	}

	return s, nil
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	eg, eCtx := errgroup.WithContext(ctx)

	if s.quicServer != nil {
		eg.Go(func() error {
			return s.quicServer.ListenAndServe(eCtx)
		})
	}

	if s.tcpServer != nil {
		eg.Go(func() error {
			return s.tcpServer.ListenAndServe(eCtx)
		})
	}

	if s.unixServer != nil {
		eg.Go(func() error {
			return s.unixServer.ListenAndServe(eCtx)
		})
	}

	if s.grpcServer != nil {
		eg.Go(func() error {
			return s.grpcServer.ListenAndServe(eCtx)
		})
	}

	return eg.Wait()
}

func (s *Server) ReadyForConnections(timeout time.Duration) bool {
	ready := make(chan struct{})
	go func() {
		if s.quicServer != nil {
			if !s.quicServer.ReadyForConnections(timeout) {
				return
			}
		}
		if s.tcpServer != nil {
			if !s.tcpServer.ReadyForConnections(timeout) {
				return
			}
		}
		if s.unixServer != nil {
			if !s.unixServer.ReadyForConnections(timeout) {
				return
			}
		}
		close(ready)
	}()

	select {
	case <-time.After(timeout):
		return false
	case <-ready:
		return true
	}
}

func (s *Server) Done() <-chan struct{} {
	if s.quicServer != nil {
		return s.quicServer.Done()
	}
	if s.tcpServer != nil {
		return s.tcpServer.Done()
	}
	if s.unixServer != nil {
		return s.unixServer.Done()
	}
	done := make(chan struct{})
	close(done)
	return done
}
