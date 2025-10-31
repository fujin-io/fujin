package fujin

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	v1 "github.com/ValerySidorin/fujin/api/fujin/v1"
	"github.com/ValerySidorin/fujin/api/fujin/v1/proto"
	"github.com/quic-go/quic-go"
)

var (
	ReadBufferSize = 512

	DISCONNECT_REQ = []byte{byte(proto.OP_CODE_DISCONNECT)}
)

type Conn struct {
	qconn *quic.Conn

	timeout time.Duration
	wdl     time.Duration
	closed  atomic.Bool

	l *slog.Logger
}

func Dial(ctx context.Context, addr string, tlsConf *tls.Config, quicConf *quic.Config, opts ...Option) (*Conn, error) {
	if tlsConf != nil {
		tlsConf = tlsConf.Clone()
		tlsConf.NextProtos = []string{v1.Version}
	}
	conn, err := quic.DialAddr(ctx, addr, tlsConf, quicConf)
	if err != nil {
		return nil, fmt.Errorf("quic: dial addr: %w", err)
	}

	c := &Conn{
		qconn:   conn,
		timeout: 10 * time.Second,
		wdl:     5 * time.Second,
		l:       slog.Default(),
	}

	for _, opt := range opts {
		opt(c)
	}

	go func() {
		var pingBuf [1]byte

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if c.closed.Load() {
					return
				}
				str, err := conn.AcceptStream(ctx)
				if err != nil {
					c.l.Error("ping: accept stream", "err", err)
					continue
				}

				if err := handlePing(str, pingBuf[:]); err != nil {
					c.l.Error("ping: handle ping", "err", err)
					continue
				}
			}
		}
	}()

	return c, nil
}

func (c *Conn) Close() error {
	// disconnect and close all streams
	c.closed.Store(true)
	if err := c.qconn.CloseWithError(0x0, ""); err != nil {
		return fmt.Errorf("quic: close: %w", err)
	}
	return nil
}

func handlePing(str *quic.Stream, buf []byte) error {
	defer str.Close()

	_, err := str.Read(buf[:])
	if err == io.EOF {
		buf[0] = byte(proto.RESP_CODE_PONG)
		if _, err := str.Write(buf[:]); err != nil {
			return fmt.Errorf("ping: write pong: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("ping: read: %w", err)
	}

	return nil
}
