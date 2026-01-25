package kafka

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (c *Connector) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	pingCtx, cancel := context.WithTimeout(ctx, c.conf.PingTimeout)
	defer cancel()

	if err := c.cl.Ping(pingCtx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			fmt.Println("before fetch")
			fetches := c.cl.PollRecords(ctx, c.conf.MaxPollRecords)
			if ctx.Err() != nil {
				return nil
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				return fmt.Errorf("kafka: poll fetches: %v", fmt.Sprint(errs))
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				c.handler(iter.Next(), h)
			}
		}
	}
}

func (c *Connector) HSubscribe(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	pingCtx, cancel := context.WithTimeout(ctx, c.conf.PingTimeout)
	defer cancel()

	if err := c.cl.Ping(pingCtx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			fmt.Println("before fetch")
			fetches := c.cl.PollRecords(ctx, c.conf.MaxPollRecords)
			if ctx.Err() != nil {
				return nil
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				return fmt.Errorf("kafka: poll fetches: %v", fmt.Sprint(errs))
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				c.headeredHandler(iter.Next(), h)
			}
		}
	}
}

func (c *Connector) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	if c.fetching.Load() {
		fetchHandler(0, nil)
		return
	}

	c.fetching.Store(true)
	defer c.fetching.Store(false)

	fetches := c.cl.PollRecords(ctx, int(n))
	if ctx.Err() != nil {
		fetchHandler(0, nil)
		return
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		fetchHandler(0, fmt.Errorf("kafka: poll fetches: %v", fmt.Sprint(errs)))
		return
	}

	fetchHandler(uint32(fetches.NumRecords()), nil)

	iter := fetches.RecordIter()
	var rec *kgo.Record
	for !iter.Done() {
		rec = iter.Next()
		c.handler(rec, msgHandler)
	}

	// We need to commit messages manually for some reason, even if auto commit is enabled
	if c.autoCommit {
		if err := c.cl.CommitRecords(ctx, rec); err != nil {
			c.l.Error("commit record", "err", err)
		}
	}
}

func (c *Connector) HFetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	if c.fetching.Load() {
		fetchHandler(0, nil)
		return
	}

	c.fetching.Store(true)
	defer c.fetching.Store(false)

	fetches := c.cl.PollRecords(ctx, int(n))
	if ctx.Err() != nil {
		fetchHandler(0, nil)
		return
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		fetchHandler(0, fmt.Errorf("kafka: poll fetches: %v", fmt.Sprint(errs)))
		return
	}

	fetchHandler(uint32(fetches.NumRecords()), nil)

	iter := fetches.RecordIter()
	var rec *kgo.Record
	for !iter.Done() {
		rec = iter.Next()
		c.headeredHandler(rec, msgHandler)
	}

	if c.autoCommit {
		if err := c.cl.CommitRecords(ctx, rec); err != nil {
			c.l.Error("commit record", "err", err)
		}
	}
}

func (c *Connector) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	offsets := make(map[string]map[int32]kgo.EpochOffset)
	msgIDMapping := make(map[string]map[int32][][]byte)

	for _, id := range msgIDs {
		partition := int32(binary.BigEndian.Uint16(id[:2]))
		epoch := int32(binary.BigEndian.Uint16(id[2:4]))
		offset := int64(binary.BigEndian.Uint32(id[4:8]))
		topic := string(id[8:])

		if msgIDMapping[topic] == nil {
			msgIDMapping[topic] = make(map[int32][][]byte)
		}

		msgIDMapping[topic][partition] = append(msgIDMapping[topic][partition], id)

		toffsets := offsets[topic]
		if toffsets == nil {
			toffsets = make(map[int32]kgo.EpochOffset)
			offsets[topic] = toffsets
		}

		if at, exists := toffsets[partition]; exists {
			if at.Epoch > epoch || at.Epoch == epoch && at.Offset > offset {
				continue
			}
		}
		toffsets[partition] = kgo.EpochOffset{
			Epoch:  epoch,
			Offset: offset + 1,
		}
	}

	c.cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		ackHandler(err)
		if err != nil {
			return
		}

		for _, topic := range resp.Topics {
			for _, partition := range topic.Partitions {
				err := kerr.ErrorForCode(partition.ErrorCode)
				for _, msgID := range msgIDMapping[topic.Topic][partition.Partition] {
					ackMsgHandler(msgID, err)
				}
			}
		}
	})
}

func (c *Connector) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(nil)
	for _, msgID := range msgIDs {
		nackMsgHandler(msgID, nil)
	}
}

func (c *Connector) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(args[0].(int32)))
	buf = binary.BigEndian.AppendUint16(buf, uint16(args[1].(int32)))
	buf = binary.BigEndian.AppendUint32(buf, uint32(args[2].(int64)))
	return append(buf, topic...)
}

func (c *Connector) MsgIDStaticArgsLen() int {
	return 8
}

func (c *Connector) IsAutoCommit() bool {
	return c.autoCommit
}
