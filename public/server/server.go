package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	grpc_server "github.com/fujin-io/fujin/internal/transport/grpc/v1/server"
	"github.com/fujin-io/fujin/public/plugins/transport"
	"github.com/fujin-io/fujin/public/server/config"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	conf config.Config

	transportServers []transport.TransportServer
	grpcServer       GRPCServer

	l *slog.Logger
}

// TransportServer is the common interface for fujin-protocol transports.
// Use transport.TransportServer from the transport plugin package.
type TransportServer = transport.TransportServer

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

	for _, e := range conf.Transports {
		srv, err := transport.NewServer(e, conf.Connectors, l)
		if err != nil {
			return nil, err
		}
		if srv != nil {
			s.transportServers = append(s.transportServers, srv)
		}
	}

	if conf.GRPC.Enabled {
		s.grpcServer = grpc_server.NewGRPCServerWrapper(s.conf.GRPC, s.conf.Connectors, s.l)
	}

	return s, nil
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	eg, eCtx := errgroup.WithContext(ctx)

	fmt.Println(s == nil)
	for _, ts := range s.transportServers {
		ts := ts
		eg.Go(func() error {
			return ts.ListenAndServe(eCtx)
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
		fmt.Println(s == nil)
		fmt.Println(s.transportServers)
		for _, ts := range s.transportServers {
			if !ts.ReadyForConnections(timeout) {
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
	for _, ts := range s.transportServers {
		return ts.Done()
	}
	done := make(chan struct{})
	close(done)
	return done
}
