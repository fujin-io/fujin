package zmq4

import (
	"context"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	"github.com/go-zeromq/zmq4"
)

// Writer implements connector.WriteCloser for ZeroMQ PUB
type Writer struct {
	conf   ConnectorConfig
	topic  string // ZMQ topic frame (client name or ProduceTopic)
	pub    zmq4.Socket
	pubEp  string
	pubBind bool
	mu     sync.Mutex
	l      *slog.Logger
}

// NewWriter creates a new ZMQ writer that publishes to the shared PUB socket.
// clientName is the config client key (used as ZMQ topic when ProduceTopic is empty).
func NewWriter(ctx context.Context, conf ConnectorConfig, clientName string, l *slog.Logger) (connector.WriteCloser, error) {
	ep, bind, err := conf.PublishEndpoint()
	if err != nil {
		return nil, err
	}

	pub, err := getOrCreatePUB(ctx, ep, bind)
	if err != nil {
		return nil, err
	}

	topic := conf.Topic()
	if topic == "" {
		topic = clientName
	}
	if topic == "" {
		releasePUB(ep, bind)
		return nil, util.ValidationErr("produce_topic or client name required for zmq4 writer")
	}

	return &Writer{
		conf:    conf,
		topic:   topic,
		pub:     pub,
		pubEp:   ep,
		pubBind: bind,
		l:       l.With("connector", "zmq4", "topic", topic),
	}, nil
}

// Produce sends [topic, message] as ZMQ multipart
func (w *Writer) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	w.mu.Lock()
	defer w.mu.Unlock()

	m := zmq4.NewMsgFrom([]byte(w.topic), msg)
	if err := w.pub.SendMulti(m); err != nil {
		callback(err)
		return
	}
	callback(nil)
}

// HProduce sends [topic, headers..., message]. ZMQ has no native headers; we prepend key-value frames
func (w *Writer) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	if len(headers) == 0 {
		w.Produce(ctx, msg, callback)
		return
	}
	// Multipart: topic, [k,v,k,v,...], body
	frames := [][]byte{[]byte(w.topic)}
	frames = append(frames, headers...)
	frames = append(frames, msg)
	m := zmq4.NewMsgFrom(frames...)
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.pub.SendMulti(m); err != nil {
		callback(err)
		return
	}
	callback(nil)
}

// Flush is a no-op for ZMQ (no buffering at this layer)
func (w *Writer) Flush(ctx context.Context) error {
	return nil
}

// BeginTx not supported
func (w *Writer) BeginTx(ctx context.Context) error {
	return util.ErrNotSupported
}

// CommitTx not supported
func (w *Writer) CommitTx(ctx context.Context) error {
	return util.ErrNotSupported
}

// RollbackTx not supported
func (w *Writer) RollbackTx(ctx context.Context) error {
	return util.ErrNotSupported
}

// Close releases the PUB reference (does not necessarily close the socket)
func (w *Writer) Close() error {
	releasePUB(w.pubEp, w.pubBind)
	return nil
}
