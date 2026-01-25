package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/fujin-io/fujin/internal/api/fujin/v1/server"
	grpc_server "github.com/fujin-io/fujin/internal/api/grpc/v1/server"
	obs "github.com/fujin-io/fujin/internal/observability"
	"github.com/fujin-io/fujin/public/server/config"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	conf config.Config

	fujinServer FujinServer
	grpcServer  GRPCServer

	l *slog.Logger
}

// FujinServer interface for optional Fujin server
type FujinServer interface {
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

	if conf.Fujin.Enabled {
		s.fujinServer = server.NewFujinServer(s.conf.Fujin, s.conf.Connectors, s.l)
	}

	// Initialize gRPC server if enabled
	if conf.GRPC.Enabled {
		s.grpcServer = grpc_server.NewGRPCServerWrapper(s.conf.GRPC, s.conf.Connectors, s.l)
	}

	return s, nil
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	shutdown, _ := obs.Init(ctx, s.conf.Observability, s.l)
	if shutdown != nil {
		defer func() {
			stopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			_ = shutdown(stopCtx)
		}()
	}

	eg, eCtx := errgroup.WithContext(ctx)

	if s.fujinServer != nil {
		eg.Go(func() error {
			return s.fujinServer.ListenAndServe(eCtx)
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
		if s.fujinServer != nil {
			if !s.fujinServer.ReadyForConnections(timeout) {
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
	if s.fujinServer != nil {
		return s.fujinServer.Done()
	}
	// Return a closed channel if no fujin server
	done := make(chan struct{})
	close(done)
	return done
}
