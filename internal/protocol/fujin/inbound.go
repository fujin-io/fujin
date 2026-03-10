package fujin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/fujin-io/fujin/internal/protocol/fujin/pool"
	"github.com/fujin-io/fujin/internal/common/assert"
)

var (
	ErrNilHandler = errors.New("handler is nil")
)

const (
	readBufferSize = 512
)

type inbound struct {
	str       Stream
	h         *handler
	ftt       time.Duration // force terminate timeout
	abortRead func()        // transport-specific: abort read with error (QUIC: CancelRead(ConnErr))
	closeRead func()        // transport-specific: close read cleanly (QUIC: CancelRead(NoErr))
	l         *slog.Logger
}

func newInbound(str Stream, ftt time.Duration, h *handler, l *slog.Logger, abortRead, closeRead func()) *inbound {
	assert.NotNil(h)
	assert.NotNil(l)

	return &inbound{
		str:       str,
		h:         h,
		ftt:       ftt,
		abortRead: abortRead,
		closeRead: closeRead,
		l:         l,
	}
}

func (i *inbound) readLoop(ctx context.Context) {
	stopCh := make(chan struct{})
	buf := pool.Get(readBufferSize)

	defer func() {
		pool.Put(buf)
		i.h.close()
		i.close()
		close(stopCh)
		i.h.out.BroadcastCond()
	}()

	var (
		n   int
		err error
	)

	go func() {
		select {
		case <-ctx.Done():
			i.waitAndDisconnect()
		case <-stopCh:
		}
	}()

	for {
		n, err = i.str.Read(buf[:readBufferSize])
		if n == 0 && err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			i.abortRead()
			i.l.Error("read stream", "err", err)
			break
		}

		err := i.h.handle(buf[:n])
		if err != nil {
			if !errors.Is(err, ErrClose) {
				i.l.Error("handle buf", "err", err)
				i.abortRead()
			}
			break
		}
		buf = buf[:0]

		if i.h.stopRead {
			i.closeRead()
			break
		}
	}
}

func (i *inbound) waitAndDisconnect() {
	i.h.enqueueStop()
	time.Sleep(i.ftt)
	i.h.disconnect()
	i.close()
}

func (i *inbound) close() {
	i.closeRead()
	i.h.wg.Wait()
	i.h.out.Close()
	<-i.h.closed
	i.h.flushBufs()
}
