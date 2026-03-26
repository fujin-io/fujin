package proto

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStream implements session.Stream for testing.
type mockStream struct {
	mu         sync.Mutex
	buf        bytes.Buffer
	writeErr   error
	writeDelay time.Duration
	writeCount int
	closed     bool
}

func (m *mockStream) Read(p []byte) (int, error)         { return 0, io.EOF }
func (m *mockStream) Close() error                       { m.closed = true; return nil }
func (m *mockStream) SetDeadline(t time.Time) error      { return nil }
func (m *mockStream) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockStream) SetWriteDeadline(t time.Time) error { return nil }

func (m *mockStream) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.writeDelay > 0 {
		m.mu.Unlock()
		time.Sleep(m.writeDelay)
		m.mu.Lock()
	}
	if m.writeErr != nil {
		m.writeCount++
		return 0, m.writeErr
	}
	m.writeCount++
	return m.buf.Write(p)
}

func (m *mockStream) written() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buf.Bytes()
}

func newTestOutbound(str *mockStream) *Outbound {
	l := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return NewOutbound(str, 5*time.Second, l)
}

func TestNewOutbound(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)

	assert.NotNil(t, o.c, "cond should be initialized")
	assert.False(t, o.IsClosed())
	assert.Equal(t, 5*time.Second, o.wdl)
	assert.Equal(t, int64(0), o.pb)
	assert.Nil(t, o.v)
	assert.Nil(t, o.wv)
}

func TestEnqueueProto_SingleMessage(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	data := []byte("hello world")
	o.EnqueueProto(data)

	time.Sleep(50 * time.Millisecond)
	o.Close()
	<-done

	assert.Equal(t, data, str.written())
}

func TestEnqueueProto_MultipleMessages(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	msgs := [][]byte{
		[]byte("msg1"),
		[]byte("msg2"),
		[]byte("msg3"),
	}
	for _, m := range msgs {
		o.EnqueueProto(m)
	}

	time.Sleep(50 * time.Millisecond)
	o.Close()
	<-done

	expected := bytes.Join(msgs, nil)
	assert.Equal(t, expected, str.written())
}

func TestEnqueueProtoMulti(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	msgs := [][]byte{
		[]byte("aaa"),
		[]byte("bbb"),
		[]byte("ccc"),
	}
	o.EnqueueProtoMulti(msgs...)

	time.Sleep(50 * time.Millisecond)
	o.Close()
	<-done

	expected := bytes.Join(msgs, nil)
	assert.Equal(t, expected, str.written())
}

func TestEnqueueProto_IgnoredAfterClose(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	o.Close()
	<-done

	o.EnqueueProto([]byte("should not appear"))
	o.EnqueueProtoMulti([]byte("also not"))

	assert.Empty(t, str.written())
}

func TestEnqueueProtoMulti_IgnoredAfterClose(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	o.Close()
	<-done

	o.EnqueueProtoMulti([]byte("a"), []byte("b"))
	assert.Empty(t, str.written())
}

func TestWriteLoop_ExitsOnClose(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	o.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WriteLoop did not exit after Close")
	}
}

func TestWriteLoop_FlushesBeforeClose(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	o.EnqueueProto([]byte("pre-close data"))

	go func() {
		o.WriteLoop()
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	o.Close()
	<-done

	assert.Equal(t, []byte("pre-close data"), str.written())
}

func TestWriteLoop_CleanupOnClose(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)

	o.Lock()
	o.QueueOutboundNoLock([]byte("buffered data"))
	o.Unlock()

	done := make(chan struct{})
	go func() {
		o.WriteLoop()
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	o.Close()
	<-done

	o.Lock()
	assert.Nil(t, o.v)
	assert.Nil(t, o.wv)
	o.Unlock()
}

func TestWriteLoop_DrainsPendingOnClose(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	const numMsgs = 1000
	msg := []byte("hello!")
	for i := 0; i < numMsgs; i++ {
		o.EnqueueProto(msg)
	}

	// Close immediately after enqueue burst — pending buffers must be flushed, not dropped
	o.Close()
	<-done

	expected := numMsgs * len(msg)
	assert.Equal(t, expected, len(str.written()),
		"all pending bytes must be written to stream on close, not dropped")
}

func TestQueueOutbound_CopiesData(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)

	original := []byte("original data")
	o.queueOutbound(original)

	copy(original, "XXXXXXXXXXXXXX"[:len(original)])

	o.Lock()
	var queued []byte
	for _, buf := range o.v {
		queued = append(queued, buf...)
	}
	o.Unlock()

	assert.Equal(t, []byte("original data"), queued)
}

func TestQueueOutboundNoLock_CopiesData(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)

	original := []byte("test copy")
	o.Lock()
	o.QueueOutboundNoLock(original)
	o.Unlock()

	copy(original, "XXXXXXXXX"[:len(original)])

	o.Lock()
	var queued []byte
	for _, buf := range o.v {
		queued = append(queued, buf...)
	}
	o.Unlock()

	assert.Equal(t, []byte("test copy"), queued)
}

func TestQueueOutboundByteNoLock(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)

	o.Lock()
	o.QueueOutboundByteNoLock(0x42)
	o.Unlock()

	o.Lock()
	require.Len(t, o.v, 1)
	assert.Equal(t, []byte{0x42}, []byte(o.v[0]))
	assert.Equal(t, int64(1), o.pb)
	o.Unlock()
}

func TestQueueOutbound_PendingBytesTracking(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)

	o.queueOutbound([]byte("hello"))
	o.Lock()
	assert.Equal(t, int64(5), o.pb)
	o.Unlock()

	o.queueOutbound([]byte(" world"))
	o.Lock()
	assert.Equal(t, int64(11), o.pb)
	o.Unlock()
}

func TestQueueOutbound_CoalesceIntoLastBuffer(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)

	o.queueOutbound([]byte("aaaaaaaaaa"))

	o.Lock()
	bufCount1 := len(o.v)
	o.Unlock()

	o.queueOutbound([]byte("bb"))

	o.Lock()
	bufCount2 := len(o.v)
	o.Unlock()

	assert.Equal(t, bufCount1, bufCount2, "small messages should coalesce into same buffer")

	o.Lock()
	var total []byte
	for _, buf := range o.v {
		total = append(total, buf...)
	}
	o.Unlock()

	assert.Equal(t, []byte("aaaaaaaaaabb"), total)
}

func TestWriteBuffers_NilStream(t *testing.T) {
	l := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	o := NewOutbound(nil, 5*time.Second, l)

	o.wv = append(o.wv, []byte("data"))
	n := o.writeBuffers()
	assert.Equal(t, int64(0), n)
}

func TestWriteBuffers_WriteError(t *testing.T) {
	str := &mockStream{writeErr: errors.New("connection reset")}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	o.EnqueueProto([]byte("will fail"))
	time.Sleep(50 * time.Millisecond)

	o.Close()
	<-done

	o.Lock()
	assert.Nil(t, o.v)
	assert.Nil(t, o.wv)
	o.Unlock()
}

func TestClose_Idempotent(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)

	o.Close()
	assert.True(t, o.IsClosed())

	o.Close()
	assert.True(t, o.IsClosed())
}

func TestBroadcastCond(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)

	o.BroadcastCond()
}

func TestConcurrent_EnqueueAndWriteLoop(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	const goroutines = 10
	const msgsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	var totalBytes atomic.Int64

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < msgsPerGoroutine; i++ {
				msg := []byte("x")
				o.EnqueueProto(msg)
				totalBytes.Add(int64(len(msg)))
			}
		}(g)
	}

	wg.Wait()

	require.Eventually(t, func() bool {
		o.Lock()
		pb := o.pb
		o.Unlock()
		return pb == 0
	}, 2*time.Second, 10*time.Millisecond)

	o.Close()
	<-done

	written := len(str.written())
	total := int(totalBytes.Load())
	assert.Equal(t, total, written,
		"all bytes should be written to stream")
}

func TestConcurrent_EnqueueMultiAndWriteLoop(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			batch := [][]byte{
				[]byte("aa"),
				[]byte("bb"),
				[]byte("cc"),
			}
			o.EnqueueProtoMulti(batch...)
		}()
	}

	wg.Wait()

	require.Eventually(t, func() bool {
		o.Lock()
		pb := o.pb
		o.Unlock()
		return pb == 0
	}, 2*time.Second, 10*time.Millisecond)

	o.Close()
	<-done

	assert.Equal(t, 60, len(str.written()))
}

func TestConcurrent_CloseWhileEnqueue(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			o.EnqueueProto([]byte("data"))
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		o.Close()
	}()

	wg.Wait()
	<-done

	o.Lock()
	assert.Nil(t, o.v)
	assert.Nil(t, o.wv)
	o.Unlock()
}

func TestLargePayload(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	payload := make([]byte, 128*1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	o.EnqueueProto(payload)
	time.Sleep(100 * time.Millisecond)
	o.Close()
	<-done

	assert.Equal(t, payload, str.written())
}

func TestSignalFlush_WithoutWriteLoop(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)

	o.SignalFlush()
}

func TestPendingBytes_DecreasedAfterFlush(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	o.EnqueueProto([]byte("12345"))

	time.Sleep(50 * time.Millisecond)

	o.Lock()
	pb := o.pb
	o.Unlock()

	assert.Equal(t, int64(0), pb, "pending bytes should be 0 after flush")

	o.Close()
	<-done
}

func TestEnqueueProtoMulti_NoLockConflict(t *testing.T) {
	str := &mockStream{}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.EnqueueProtoMulti([]byte("x"), []byte("y"))
		}()
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		o.Lock()
		pb := o.pb
		o.Unlock()
		return pb == 0
	}, 2*time.Second, 10*time.Millisecond)

	o.Close()
	<-done

	assert.Equal(t, 100, len(str.written()))
}

func TestWriteLoop_NoIOUnderLock(t *testing.T) {
	// OLD VERSION: lock is held during I/O in flushOutbound.
	// This test detects it: enqueue should not block on slow I/O.
	str := &mockStream{writeDelay: 10 * time.Millisecond}
	o := newTestOutbound(str)
	done := make(chan struct{})

	go func() {
		o.WriteLoop()
		close(done)
	}()

	o.EnqueueProto([]byte("trigger slow write"))
	time.Sleep(2 * time.Millisecond) // let WriteLoop start I/O

	start := time.Now()
	for i := 0; i < 10; i++ {
		o.EnqueueProto([]byte("fast"))
	}
	enqueueTime := time.Since(start)

	// If lock were held during I/O, 10 enqueues would take ~100ms.
	// Without lock during I/O, they complete near-instantly.
	assert.Less(t, enqueueTime, 50*time.Millisecond,
		"enqueue should not block on slow I/O")

	time.Sleep(200 * time.Millisecond)
	o.Close()
	<-done
}
