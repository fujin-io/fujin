package core

import (
	"context"
	"fmt"
	"log/slog"
	"unsafe"

	"github.com/fujin-io/fujin/public/connectors/cerr"
	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/nats-io/nats.go"
)

// Reader implements connector.ReadCloser for NATS Core
type Reader struct {
	conf       ConnectorConfig
	autoCommit bool
	nc         *nats.Conn
	l          *slog.Logger
}

// NewReader creates a new NATS Core reader
func NewReader(conf ConnectorConfig, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	nc, err := nats.Connect(conf.URL)
	if err != nil {
		return nil, fmt.Errorf("nats: connect: %w", err)
	}

	return &Reader{
		conf:       conf,
		autoCommit: autoCommit,
		nc:         nc,
		l:          l.With("reader_type", "nats_core"),
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	sub, err := r.nc.Subscribe(r.conf.Subject, func(msg *nats.Msg) {
		h(msg.Data, msg.Subject)
	})
	if err != nil {
		return fmt.Errorf("nats: subscribe: %w", err)
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			r.l.Error("unsubscribe", "err", err)
		}
	}()

	<-ctx.Done()
	return nil
}

func (r *Reader) HSubscribe(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	sub, err := r.nc.Subscribe(r.conf.Subject, func(msg *nats.Msg) {
		var hs [][]byte
		for k, headers := range msg.Header {
			hs = append(hs, unsafe.Slice((*byte)(unsafe.StringData(k)), len(k)))
			var vals [][]byte
			for _, hdr := range headers {
				vals = append(vals, unsafe.Slice((*byte)(unsafe.StringData(hdr)), len(hdr)))
			}
			hs = append(hs, joinBytes(vals, ','))
		}
		h(msg.Data, msg.Subject, hs)
	})
	if err != nil {
		return fmt.Errorf("nats: subscribe: %w", err)
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			r.l.Error("unsubscribe", "err", err)
		}
	}()

	<-ctx.Done()
	return nil
}

func (r *Reader) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	fetchHandler(0, cerr.ErrNotSupported)
}

func (r *Reader) HFetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	fetchHandler(0, cerr.ErrNotSupported)
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	// NATS Core doesn't support acknowledgments (at-most-once delivery)
	ackHandler(cerr.ErrNotSupported)
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	// NATS Core doesn't support acknowledgments (at-most-once delivery)
	nackHandler(cerr.ErrNotSupported)
}

func (r *Reader) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return buf
}

func (r *Reader) MsgIDStaticArgsLen() int {
	return 0
}

func (r *Reader) IsAutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() error {
	r.nc.Close()
	return nil
}

// joinBytes joins byte slices with a separator
func joinBytes(elems [][]byte, sep byte) []byte {
	switch len(elems) {
	case 0:
		return nil
	case 1:
		out := make([]byte, len(elems[0]))
		copy(out, elems[0])
		return out
	}

	totalLen := len(elems) - 1
	for _, e := range elems {
		totalLen += len(e)
	}

	out := make([]byte, totalLen)

	pos := copy(out, elems[0])
	for _, e := range elems[1:] {
		pos += copy(out[pos:], []byte{sep})
		pos += copy(out[pos:], e)
	}

	return out
}

