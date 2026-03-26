package test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("gen", newGenConnector); err != nil {
		panic(fmt.Sprintf("register gen connector: %v", err))
	}
}

// GenConfig configures the gen connector.
type GenConfig struct {
	MsgSize int `yaml:"msg_size"`
}

// genConnector generates messages as fast as possible for benchmarking subscribe throughput.
type genConnector struct {
	msgSize int
}

func newGenConnector(config any, l *slog.Logger) (connector.Connector, error) {
	msgSize := 32 // default
	if m, ok := config.(GenConfig); ok {
		msgSize = m.MsgSize
	}
	if msgSize <= 0 {
		msgSize = 1
	}
	return &genConnector{msgSize: msgSize}, nil
}

func (g *genConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	return &genReader{msg: sizedBytes(g.msgSize)}, nil
}

func (g *genConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	return newWriter(), nil
}

func (g *genConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return func(settingPath, value string) (any, error) { return nil, nil }
}

// genReader generates messages in a tight loop until context is cancelled.
type genReader struct {
	msg []byte
}

func (r *genReader) Subscribe(ctx context.Context, h func([]byte, string, ...any)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			h(r.msg, "sub")
		}
	}
}

func (r *genReader) SubscribeWithHeaders(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			h(r.msg, "sub", nil)
		}
	}
}

func (r *genReader) Fetch(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, topic string, args ...any)) {
	fetchHandler(0, nil)
}

func (r *genReader) FetchWithHeaders(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, topic string, hs [][]byte, args ...any)) {
	fetchHandler(0, nil)
}

func (r *genReader) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	ackHandler(nil)
	for _, id := range msgIDs {
		ackMsgHandler(id, nil)
	}
}

func (r *genReader) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	nackHandler(nil)
	for _, id := range msgIDs {
		nackMsgHandler(id, nil)
	}
}

func (r *genReader) EncodeMsgID(buf []byte, _ string, args ...any) []byte {
	return buf
}

func (r *genReader) MsgIDArgsLen() int {
	return 0
}

func (r *genReader) AutoCommit() bool {
	return true
}

func (r *genReader) Close() error {
	return nil
}

var _ connector.Connector = (*genConnector)(nil)
var _ connector.ReadCloser = (*genReader)(nil)
