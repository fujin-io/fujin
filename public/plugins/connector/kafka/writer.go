package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (c *Connector) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	c.wg.Add(1)
	c.cl.Produce(ctx, &kgo.Record{
		Topic: c.conf.ProduceTopic,
		Value: msg,
	}, func(r *kgo.Record, err error) {
		callback(err)
		c.wg.Done()
	})
}

func (c *Connector) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	c.wg.Add(1)
	rec := &kgo.Record{
		Topic: c.conf.ProduceTopic,
		Value: msg,
	}
	if len(headers) > 0 {
		var kh []kgo.RecordHeader
		for i := 0; i < len(headers); i += 2 {
			key := headers[i]
			var val []byte
			if i+1 < len(headers) {
				val = headers[i+1]
			}
			kh = append(kh, kgo.RecordHeader{Key: string(key), Value: val})
		}
		rec.Headers = kh
	}
	c.cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
		callback(err)
		c.wg.Done()
	})
}

func (c *Connector) Flush(ctx context.Context) error {
	c.wg.Wait()
	return nil
}

func (c *Connector) BeginTx(ctx context.Context) error {
	return c.cl.BeginTransaction()
}

func (c *Connector) CommitTx(ctx context.Context) error {
	if err := c.cl.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: flush: %w", err)
	}

	switch err := c.cl.EndTransaction(ctx, kgo.TryCommit); err {
	case nil:
	case kerr.OperationNotAttempted:
		return c.RollbackTx(ctx)
	default:
		return fmt.Errorf("kafka: commit transaction: %w", err)
	}

	return nil
}

func (c *Connector) RollbackTx(ctx context.Context) error {
	if err := c.cl.AbortBufferedRecords(ctx); err != nil {
		return fmt.Errorf("kafka: abort buffered records: %w", err)
	}
	if err := c.cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		return fmt.Errorf("kafka: rollback transaction: %w", err)
	}

	return nil
}
