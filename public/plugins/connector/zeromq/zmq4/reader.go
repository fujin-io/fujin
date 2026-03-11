package zmq4

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	"github.com/go-zeromq/zmq4"
)

// Reader implements connector.ReadCloser for ZeroMQ SUB
type Reader struct {
	conf       ConnectorConfig
	autoCommit bool
	sub        zmq4.Socket
	l          *slog.Logger
}

// NewReader creates a new ZMQ reader that subscribes via SUB socket
func NewReader(ctx context.Context, conf ConnectorConfig, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	ep, bind, err := conf.SubscribeEndpoint()
	if err != nil {
		return nil, err
	}

	sub := zmq4.NewSub(ctx)
	if bind {
		if err := sub.Listen(ep); err != nil {
			return nil, fmt.Errorf("zmq4: sub listen %q: %w", ep, err)
		}
	} else {
		if err := sub.Dial(ep); err != nil {
			sub.Close()
			return nil, fmt.Errorf("zmq4: sub dial %q: %w", ep, err)
		}
	}

	// Set subscription filters. Empty = subscribe to all (use empty string in ZMQ).
	topics := conf.ConsumeTopics
	if len(topics) == 0 {
		topics = []string{""} // receive all
	}
	for _, t := range topics {
		if err := sub.SetOption(zmq4.OptionSubscribe, t); err != nil {
			sub.Close()
			return nil, fmt.Errorf("zmq4: subscribe %q: %w", t, err)
		}
	}

	return &Reader{
		conf:       conf,
		autoCommit: autoCommit,
		sub:        sub,
		l:          l.With("connector", "zmq4"),
	}, nil
}

type recvResult struct {
	msg zmq4.Msg
	err error
}

// Subscribe runs the recv loop and delivers messages to h. First frame = topic, rest = body.
func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	defer r.sub.Close()
	ch := make(chan recvResult, 1)
	go func() {
		for {
			msg, err := r.sub.Recv()
			select {
			case ch <- recvResult{msg: msg, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-ch:
			if res.err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("zmq4: recv: %w", res.err)
			}
			msg := res.msg
			if len(msg.Frames) < 1 {
				continue
			}
			if len(msg.Frames) == 1 {
				h(msg.Frames[0], "")
				continue
			}
			topic := string(msg.Frames[0])
			body := msg.Frames[len(msg.Frames)-1]
			h(body, topic)
		}
	}
}

// SubscribeWithHeaders passes additional frames as headers (kv pairs)
func (r *Reader) SubscribeWithHeaders(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	defer r.sub.Close()
	ch := make(chan recvResult, 1)
	go func() {
		for {
			msg, err := r.sub.Recv()
			select {
			case ch <- recvResult{msg: msg, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-ch:
			if res.err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("zmq4: recv: %w", res.err)
			}
			msg := res.msg
			if len(msg.Frames) < 1 {
				continue
			}
			topic := ""
			if len(msg.Frames) >= 1 {
				topic = string(msg.Frames[0])
			}
			body := msg.Frames[len(msg.Frames)-1]
			var hs [][]byte
			if len(msg.Frames) > 2 {
				hs = msg.Frames[1 : len(msg.Frames)-1]
			}
			h(body, topic, hs)
		}
	}
}

func (r *Reader) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) FetchWithHeaders(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(util.ErrNotSupported)
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(util.ErrNotSupported)
}

func (r *Reader) MsgIDArgsLen() int { return 0 }

func (r *Reader) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return buf
}

func (r *Reader) AutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() error {
	return r.sub.Close()
}
