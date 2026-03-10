package fujin

import (
	"io"
	"time"
)

// Stream is a transport-agnostic bidirectional byte stream.
// Both *quic.Stream and net.Conn satisfy this interface.
type Stream interface {
	io.ReadWriteCloser
	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}
