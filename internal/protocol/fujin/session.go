package fujin

import (
	"context"
	"log/slog"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector/config"
)

// StreamOptions configures a protocol session for a single stream.
// Transports (QUIC, TCP, etc.) provide these options when handling a connection.
type StreamOptions struct {
	BaseConfig            config.ConnectorsConfig
	PingInterval          time.Duration
	PingTimeout           time.Duration
	PingStream            bool
	WriteDeadline         time.Duration
	ForceTerminateTimeout time.Duration
	AbortRead             func()
	CloseRead             func()
	Logger                *slog.Logger
}

// HandleStream runs the fujin binary protocol over the given stream.
// It blocks until the write loop finishes (i.e., the session ends).
// The caller is responsible for closing the stream after HandleStream returns.
func HandleStream(ctx context.Context, str Stream, opts StreamOptions) {
	out := NewOutbound(str, opts.WriteDeadline, opts.Logger)
	h := newHandler(ctx,
		opts.PingInterval, opts.PingTimeout, opts.PingStream,
		opts.BaseConfig, out, str, opts.Logger,
	)
	in := newInbound(str, opts.ForceTerminateTimeout, h, opts.Logger,
		opts.AbortRead, opts.CloseRead,
	)
	go in.readLoop(ctx)
	out.WriteLoop()
}
