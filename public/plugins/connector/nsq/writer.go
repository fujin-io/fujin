package nsq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	"github.com/nsqio/go-nsq"
	"github.com/panjf2000/ants/v2"
)

// Writer implements connector.WriteCloser for NSQ
type Writer struct {
	conf     ConnectorConfig
	producer *nsq.Producer
	pool     *ants.Pool
	l        *slog.Logger
	wg       sync.WaitGroup
	chPool   sync.Pool
}

// NewWriter creates a new NSQ writer
func NewWriter(conf ConnectorConfig, l *slog.Logger) (connector.WriteCloser, error) {
	cfg := nsq.NewConfig()
	prod, err := nsq.NewProducer(conf.Address, cfg)
	if err != nil {
		return nil, fmt.Errorf("new producer: %w", err)
	}

	pool, err := ants.NewPool(conf.Pool.Size, ants.WithPreAlloc(conf.Pool.PreAlloc))
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}

	return &Writer{
		conf:     conf,
		producer: prod,
		pool:     pool,
		l:        l.With("writer_type", "nsq"),
		chPool: sync.Pool{
			New: func() any {
				return make(chan *nsq.ProducerTransaction, 1)
			},
		},
	}, nil
}

func (w *Writer) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	ch := w.chPool.Get().(chan *nsq.ProducerTransaction)

	err := w.producer.PublishAsync(w.conf.Topic, msg, ch)
	if err != nil {
		callback(err)
		w.chPool.Put(ch)
		return
	}

	w.wg.Add(1)
	err = w.pool.Submit(func() {
		defer w.wg.Done()
		defer w.chPool.Put(ch)

		select {
		case tx := <-ch:
			callback(tx.Error)
		case <-ctx.Done():
			callback(ctx.Err())
		}
	})
	if err != nil {
		callback(err)
		w.chPool.Put(ch)
		w.wg.Done()
	}
}

func (w *Writer) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	// NSQ doesn't support headers, fall back to regular produce
	w.Produce(ctx, msg, callback)
}

func (w *Writer) Flush(ctx context.Context) error {
	w.wg.Wait()
	return nil
}

func (w *Writer) BeginTx(ctx context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) CommitTx(ctx context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) RollbackTx(ctx context.Context) error {
	return util.ErrNotSupported
}

func (w *Writer) Close() error {
	w.wg.Wait()
	w.producer.Stop()
	if w.pool != nil {
		if w.conf.Pool.ReleaseTimeout != 0 {
			if err := w.pool.ReleaseTimeout(w.conf.Pool.ReleaseTimeout); err != nil {
				return fmt.Errorf("release pool: %w", err)
			}
		} else {
			w.pool.Release()
		}
	}
	return nil
}
