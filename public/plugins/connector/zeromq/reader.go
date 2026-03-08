package zeromq

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	zmq "github.com/go-zeromq/zmq4"
)

// Reader implements connector.ReadCloser for ZeroMQ PUB/SUB.
// It expects messages in the form: [topic][payload] (two frames).
type Reader struct {
	conf       ConnectorConfig
	socket     zmq.Socket
	autoCommit bool
	l          *slog.Logger
}

// NewReader creates a new ZeroMQ reader.
func NewReader(conf ConnectorConfig, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	if err := conf.ValidateReader(); err != nil {
		return nil, err
	}

	sub := zmq.NewSub(context.Background())
	if err := sub.Dial(conf.Endpoint); err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf("zeromq: dial %q: %w", conf.Endpoint, err)
	}

	// Subscribe to topics (or all topics if none specified).
	if len(conf.Topics) == 0 {
		if err := sub.SetOption(zmq.OptionSubscribe, ""); err != nil {
			_ = sub.Close()
			return nil, fmt.Errorf("zeromq: subscribe to all topics: %w", err)
		}
	} else {
		for _, t := range conf.Topics {
			if err := sub.SetOption(zmq.OptionSubscribe, t); err != nil {
				_ = sub.Close()
				return nil, fmt.Errorf("zeromq: subscribe to topic %q: %w", t, err)
			}
		}
	}

	return &Reader{
		conf:       conf,
		socket:     sub,
		autoCommit: autoCommit,
		l:          l.With("reader_type", "zeromq"),
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := r.socket.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("zeromq: recv: %w", err)
		}
		if len(msg.Frames) < 2 {
			continue
		}
		topic := string(msg.Frames[0])
		payload := msg.Frames[1]
		h(payload, topic)
	}
}

func (r *Reader) SubscribeWithHeaders(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := r.socket.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("zeromq: recv: %w", err)
		}
		if len(msg.Frames) < 2 {
			continue
		}
		topic := string(msg.Frames[0])
		payload := msg.Frames[1]
		h(payload, topic, nil)
	}
}

func (r *Reader) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	// ZeroMQ PUB/SUB doesn't support pull-style fetching.
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) FetchWithHeaders(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	// ZeroMQ PUB/SUB doesn't support pull-style fetching.
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	// ZeroMQ PUB/SUB has at-most-once semantics, no acknowledgments.
	ackHandler(util.ErrNotSupported)
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	// ZeroMQ PUB/SUB has at-most-once semantics, no acknowledgments.
	nackHandler(util.ErrNotSupported)
}

func (r *Reader) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	// ZeroMQ PUB/SUB does not expose message IDs.
	return buf
}

func (r *Reader) MsgIDArgsLen() int {
	return 0
}

func (r *Reader) AutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() error {
	return r.socket.Close()
}

