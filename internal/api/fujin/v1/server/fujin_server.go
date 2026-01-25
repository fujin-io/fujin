//go:build fujin

package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/fujin-io/fujin/internal/api/fujin/pool"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/metrics"
)

var (
	NextProtos = []string{v1.Version}
)

type FujinServer struct {
	conf       serverconfig.FujinServerConfig
	baseConfig connectorconfig.ConnectorsConfig

	ready chan struct{}
	done  chan struct{}

	l *slog.Logger
}

func NewFujinServer(conf serverconfig.FujinServerConfig, baseConfig connectorconfig.ConnectorsConfig, l *slog.Logger) *FujinServer {
	return &FujinServer{
		conf:       conf,
		baseConfig: baseConfig,
		ready:      make(chan struct{}),
		done:       make(chan struct{}),
		l:          l.With("server", "fujin"),
	}
}

func (s *FujinServer) ListenAndServe(ctx context.Context) error {
	addr, err := net.ResolveUDPAddr("udp", s.conf.Addr)
	if err != nil {
		return fmt.Errorf("resolve udp addr: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listen udp: %w", err)
	}
	tr := &quic.Transport{
		Conn: conn,
	}

	if s.conf.ObservabilityEnabled {
		tr.Tracer = metrics.NewTracer()
		s.conf.QUIC.Tracer = metrics.DefaultConnectionTracer
	}

	s.conf.TLS = s.conf.TLS.Clone()

	if s.conf.TLS == nil {
		s.conf.TLS = &tls.Config{}
	}

	s.conf.TLS.NextProtos = NextProtos

	if len(s.conf.TLS.Certificates) == 0 ||
		s.conf.TLS.ClientCAs == nil {
		s.l.Warn("tls not configured, this is not recommended for production environment")
	}

	ln, err := tr.Listen(s.conf.TLS, s.conf.QUIC)
	if err != nil {
		return fmt.Errorf("listen quic: %w", err)
	}

	connWg := &sync.WaitGroup{}

	defer func() {
		if err := ln.Close(); err != nil {
			s.l.Error("close quic listener", "err", err)
		}

		timeout := time.After(30 * time.Second)
		done := make(chan struct{})

		go func() {
			connWg.Wait()
			close(done)
		}()

		select {
		case <-timeout:
			s.l.Error("closing quic listener after timeout")
		case <-done:
			s.l.Info("closing quic listener after all connections done")
		}

		if err := tr.Close(); err != nil {
			s.l.Error("close quic transport", "err", err)
		}
		if err := conn.Close(); err != nil {
			s.l.Error("close udp listener", "err", err)
		}

		close(s.done)
		s.l.Info("fujin server stopped")
	}()

	close(s.ready)
	s.l.Info("fujin server started", "addr", ln.Addr())

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn, err := ln.Accept(ctx)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					s.l.Error(fmt.Errorf("accept conn: %w", err).Error())
				}
				continue
			}

			negotiated := conn.ConnectionState().TLS.NegotiatedProtocol
			if negotiated == "" {
				_ = conn.CloseWithError(v1.ConnErr, "unsupported protocol: none")
				continue
			}
			switch negotiated {
			case v1.Version:
				// ok – current version
			default:
				s.l.Warn("rejecting connection: unsupported ALPN", "alpn", negotiated)
				_ = conn.CloseWithError(v1.ConnErr, "unsupported protocol: "+negotiated)
				continue
			}

			ctx, cancel := context.WithCancel(ctx)
			connWg.Add(1)
			go func() {
				retryCount := 0
				t := time.NewTicker(s.conf.PingInterval)
				defer func() {
					t.Stop()
					cancel()
					connWg.Done()
				}()

				pingBuf := pool.Get(1)
				defer pool.Put(pingBuf)
				pingBuf = pingBuf[:1]

				for {
					select {
					case <-ctx.Done():
						return
					case <-t.C:
						str, err := conn.OpenStreamSync(ctx)
						if err != nil {
							retryCount++
							s.l.Error("open ping stream error", "err", err, "retry", retryCount)
							if retryCount >= s.conf.PingMaxRetries {
								if err := conn.CloseWithError(v1.PingErr, "open stream failed after retries: "+err.Error()); err != nil {
									s.l.Error("close with error", "err", err)
								}
								return
							}
							continue
						}
						retryCount = 0

						pingBuf[0] = byte(v1.OP_CODE_PING)
						if _, err := str.Write(pingBuf); err != nil {
							retryCount++
							s.l.Error("write ping error", "err", err, "retry", retryCount)
							_ = str.Close()
							if retryCount >= s.conf.PingMaxRetries {
								if err := conn.CloseWithError(v1.PingErr, "write failed after retries: "+err.Error()); err != nil {
									s.l.Error("close with error", "err", err)
								}
								return
							}
							continue
						}

						if err := str.Close(); err != nil {
							s.l.Error("close ping stream error", "err", err)
						}

						err = str.SetDeadline(time.Now().Add(s.conf.PingTimeout))
						if err != nil {
							s.l.Error("set read deadline error", "err", err)
						}

						_, err = io.ReadAll(str)
						if err != nil {
							retryCount++
							s.l.Error("read ping response error", "err", err, "retry", retryCount)
							if retryCount >= s.conf.PingMaxRetries {
								if err := conn.CloseWithError(v1.PingErr, "read failed after retries: "+err.Error()); err != nil {
									s.l.Error("close with error", "err", err)
								}
								return
							}
							continue
						}
						retryCount = 0
					}
				}
			}()

			go func() {
				for {
					str, err := conn.AcceptStream(ctx)
					if err != nil {
						if err != ctx.Err() {
							if err := conn.CloseWithError(v1.ConnErr, "accept stream: "+err.Error()); err != nil {
								s.l.Error("close with error", "err", err)
							}
						}
						return
					}

					connWg.Add(1)
					go func() {
						out := NewOutbound(str, s.conf.WriteDeadline, s.l)
						h := newHandler(ctx,
							s.conf.PingInterval, s.conf.PingTimeout, s.conf.PingStream,
							s.baseConfig, out, str, s.l)
						in := newInbound(str, s.conf.ForceTerminateTimeout, h, s.l)
						go in.readLoop(ctx)
						out.WriteLoop()
						str.Close()
						connWg.Done()
					}()
				}
			}()
		}
	}
}

func (s *FujinServer) ReadyForConnections(timeout time.Duration) bool {
	select {
	case <-time.After(timeout):
		return false
	case <-s.ready:
		return true
	}
}

func (s *FujinServer) Done() <-chan struct{} {
	return s.done
}
