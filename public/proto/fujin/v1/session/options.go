package session

import (
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
