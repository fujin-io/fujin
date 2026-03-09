package test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

func init() {
	if err := connector.Register("nop", newNopConnector); err != nil {
		panic(fmt.Sprintf("register nop connector: %v", err))
	}
}

// nopConnector implements connector.Connector interface for no op
type nopConnector struct{}

// newNopConnector creates a new nop connector instance
func newNopConnector(config any, l *slog.Logger) (connector.Connector, error) {
	return &nopConnector{}, nil
}

// NewReader creates a reader from configuration
func (n *nopConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	return newReader(), nil
}

// NewWriter creates a writer from configuration
func (n *nopConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	return newWriter(), nil
}

// GetConfigValueConverter returns the config value converter for no op
func (n *nopConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return func(settingPath, value string) (any, error) { return nil, nil }
}

// reader implements connector.ReadCloser for no op
type reader struct{}

// newReader creates a new nop reader
func newReader() connector.ReadCloser {
	return &reader{}
}

func (r *reader) Subscribe(ctx context.Context, h func([]byte, string, ...any)) error {
	<-ctx.Done()
	return nil
}

func (r *reader) SubscribeWithHeaders(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	<-ctx.Done()
	return nil
}

func (r *reader) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	fetchHandler(0, nil)
}

func (r *reader) FetchWithHeaders(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	fetchHandler(0, nil)
}

func (r *reader) Ack(
	ctx context.Context,
	msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(nil)
	for _, id := range msgIDs {
		ackMsgHandler(id, nil)
	}
}

func (r *reader) Nack(
	ctx context.Context,
	msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(nil)
	for _, id := range msgIDs {
		nackMsgHandler(id, nil)
	}
}

func (r *reader) EncodeMsgID(buf []byte, _ string, args ...any) []byte {
	return buf
}

func (r *reader) MsgIDArgsLen() int {
	return 0
}

func (r *reader) AutoCommit() bool {
	return true
}

func (r *reader) Close() error {
	return nil
}

// writer implements connector.WriteCloser for no op
type writer struct{}

// newWriter creates a new nop writer
func newWriter() connector.WriteCloser {
	return &writer{}
}

func (w *writer) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	callback(nil)
}

func (w *writer) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	callback(nil)
}

func (w *writer) Flush(ctx context.Context) error {
	return nil
}

func (w *writer) BeginTx(ctx context.Context) error {
	return util.ErrNotSupported
}

func (w *writer) CommitTx(ctx context.Context) error {
	return util.ErrNotSupported
}

func (w *writer) RollbackTx(ctx context.Context) error {
	return util.ErrNotSupported
}

func (w *writer) Close() error {
	return nil
}
