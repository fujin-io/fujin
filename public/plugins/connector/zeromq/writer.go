package zeromq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	zmq "github.com/go-zeromq/zmq4"
)

// Writer implements connector.WriteCloser for ZeroMQ PUB/SUB.
// It sends messages as two frames: [topic][payload].
type Writer struct {
	conf   ConnectorConfig
	socket zmq.Socket
	l      *slog.Logger

	mu sync.Mutex
}

// NewWriter creates a new ZeroMQ writer.
func NewWriter(conf ConnectorConfig, l *slog.Logger) (connector.WriteCloser, error) {
	if err := conf.ValidateWriter(); err != nil {
		return nil, err
	}

	pub := zmq.NewPub(context.Background())
	if err := pub.Dial(conf.Endpoint); err != nil {
		_ = pub.Close()
		return nil, fmt.Errorf("zeromq: dial %q: %w", conf.Endpoint, err)
	}

	return &Writer{
		conf:   conf,
		socket: pub,
		l:      l.With("writer_type", "zeromq"),
	}, nil
}

func (w *Writer) Produce(_ context.Context, msg []byte, callback func(err error)) {
	w.mu.Lock()
	defer w.mu.Unlock()

	zmqMsg := zmq.NewMsgFrom(
		[]byte(w.conf.Topic),
		msg,
	)

	if err := w.socket.SendMulti(zmqMsg); err != nil {
		if callback != nil {
			callback(err)
		}
		return
	}
	if callback != nil {
		callback(nil)
	}
}

func (w *Writer) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	// ZeroMQ PUB/SUB doesn't have native headers; ignore headers.
	w.Produce(ctx, msg, callback)
}

func (w *Writer) Flush(_ context.Context) error {
	// PUB sockets are synchronous from this code's perspective; nothing to flush.
	return nil
}

func (w *Writer) BeginTx(_ context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) CommitTx(_ context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) RollbackTx(_ context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.socket.Close()
}

