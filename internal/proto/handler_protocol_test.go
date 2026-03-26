package proto

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	pool2 "github.com/fujin-io/fujin/internal/common/pool"
	"github.com/fujin-io/fujin/internal/connectors"
	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/connector/config"
	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Mock connector writer
// ---------------------------------------------------------------------------

type mockConnectorWriter struct {
	mu       sync.Mutex
	produced []mockProduced
	flushed  int
	closed   bool

	txBegan      bool
	txCommitted  bool
	txRolledBack bool

	produceErr  error
	flushErr    error
	beginTxErr  error
	commitTxErr error
}

type mockProduced struct {
	msg     []byte
	headers [][]byte
}

func (w *mockConnectorWriter) Produce(_ context.Context, msg []byte, cb func(err error)) {
	w.mu.Lock()
	cp := make([]byte, len(msg))
	copy(cp, msg)
	w.produced = append(w.produced, mockProduced{msg: cp})
	err := w.produceErr
	w.mu.Unlock()
	if cb != nil {
		cb(err)
	}
}

func (w *mockConnectorWriter) HProduce(_ context.Context, msg []byte, headers [][]byte, cb func(err error)) {
	w.mu.Lock()
	cp := make([]byte, len(msg))
	copy(cp, msg)
	hsCopy := make([][]byte, len(headers))
	for i, h := range headers {
		hsCopy[i] = append([]byte(nil), h...)
	}
	w.produced = append(w.produced, mockProduced{msg: cp, headers: hsCopy})
	err := w.produceErr
	w.mu.Unlock()
	if cb != nil {
		cb(err)
	}
}

func (w *mockConnectorWriter) Flush(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushed++
	return w.flushErr
}

func (w *mockConnectorWriter) BeginTx(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.txBegan = true
	return w.beginTxErr
}

func (w *mockConnectorWriter) CommitTx(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.txCommitted = true
	return w.commitTxErr
}

func (w *mockConnectorWriter) RollbackTx(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.txRolledBack = true
	return nil
}

func (w *mockConnectorWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *mockConnectorWriter) getProduced() []mockProduced {
	w.mu.Lock()
	defer w.mu.Unlock()
	r := make([]mockProduced, len(w.produced))
	copy(r, w.produced)
	return r
}

// ---------------------------------------------------------------------------
// Mock connector reader
// ---------------------------------------------------------------------------

type mockConnectorReader struct {
	autoCommit   bool
	msgIDArgsLen int
}

func (r *mockConnectorReader) Subscribe(_ context.Context, _ func([]byte, string, ...any)) error {
	return nil
}
func (r *mockConnectorReader) SubscribeWithHeaders(_ context.Context, _ func([]byte, string, [][]byte, ...any)) error {
	return nil
}
func (r *mockConnectorReader) Fetch(_ context.Context, _ uint32, frh func(uint32, error), _ func([]byte, string, ...any)) {
	frh(0, nil)
}
func (r *mockConnectorReader) FetchWithHeaders(_ context.Context, _ uint32, frh func(uint32, error), _ func([]byte, string, [][]byte, ...any)) {
	frh(0, nil)
}
func (r *mockConnectorReader) Ack(_ context.Context, _ [][]byte, ah func(error), _ func([]byte, error)) {
	ah(nil)
}
func (r *mockConnectorReader) Nack(_ context.Context, _ [][]byte, nh func(error), _ func([]byte, error)) {
	nh(nil)
}
func (r *mockConnectorReader) MsgIDArgsLen() int                                   { return r.msgIDArgsLen }
func (r *mockConnectorReader) EncodeMsgID(buf []byte, _ string, _ ...any) []byte   { return buf }
func (r *mockConnectorReader) AutoCommit() bool                                    { return r.autoCommit }
func (r *mockConnectorReader) Close() error                                        { return nil }

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

type testHarness struct {
	h   *Handler
	str *mockStream
	out *Outbound
}

func newProtocolTestHarness() *testHarness {
	str := &mockStream{}
	l := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	out := NewOutbound(str, 5*time.Second, l)

	h := &Handler{
		ctx:          context.Background(),
		ps:           &parseState{},
		sessionState: STREAM_STATE_BIND,
		pingInterval: 2 * time.Second,
		pingTimeout:  5 * time.Second,
		closed:       make(chan struct{}),
		disconnect:   func() {},
		l:            l,
		out:          out,
		str:          str,
		subIDPool:    pool2.NewBytePool(),
		subscribers:  make(map[byte]connector.ReadCloser),
		unsubFuncs:   make(map[byte]func()),
		fetchReaders: make(map[string]map[bool]byte),
		fetchMsgHandlers: make(map[string]map[bool]func(message []byte, topic string, args ...any)),
		fetchMsgWithHeadersHandlers: make(map[string]map[bool]func(message []byte, topic string, hs [][]byte, args ...any)),
	}

	return &testHarness{h: h, str: str, out: out}
}

// setConnected sets handler state to connected (as if BIND completed)
func (th *testHarness) setConnected(connectorName string) {
	th.h.connected = true
	th.h.sessionState = STREAM_STATE_CONNECTED
	th.h.nonTxSessionWriters = make(map[string]connector.WriteCloser)
	th.h.cman = connectors.NewManagerV2(config.ConnectorConfig{}, connectorName, th.h.l)
}

// setConnectedWithWriter sets handler state to connected with a pre-populated writer
func (th *testHarness) setConnectedWithWriter(topic string, w connector.WriteCloser) {
	th.setConnected("test")
	th.h.nonTxSessionWriters[topic] = w
}

// startWriteLoop starts the outbound write loop and returns a done channel
func (th *testHarness) startWriteLoop() chan struct{} {
	done := make(chan struct{})
	go func() {
		th.out.WriteLoop()
		close(done)
	}()
	return done
}

// readResponse reads all bytes written to mock stream (with short timeout for async callbacks)
func (th *testHarness) readResponse(wait time.Duration) []byte {
	time.Sleep(wait)
	return th.str.written()
}

func (th *testHarness) close(done chan struct{}) {
	th.out.Close()
	if done != nil {
		<-done
	}
}

// feed feeds data through handler.handle()
func (th *testHarness) feed(data []byte) error {
	return th.h.handle(data)
}

// ---------------------------------------------------------------------------
// Frame builder helpers
// ---------------------------------------------------------------------------

// buildBindFrame constructs: BIND opcode + connectorNameLen(u32) + connectorName + metaCount(u16) + [meta k/v] + overridesCount(u16) + [overrides k/v]
func buildBindFrame(connectorName string, meta map[string]string, overrides map[string]string) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_BIND))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(connectorName)))
	buf = append(buf, connectorName...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(meta)))
	for k, v := range meta {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(overrides)))
	for k, v := range overrides {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

// buildProduceFrame constructs: PRODUCE opcode + correlationID(4b) + topicLen(u32) + topic + msgSize(u32) + msg
func buildProduceFrame(cID [4]byte, topic string, msg []byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_PRODUCE))
	buf = append(buf, cID[:]...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msg)))
	buf = append(buf, msg...)
	return buf
}

// buildHProduceFrame constructs: HPRODUCE opcode + correlationID(4b) + topicLen(u32) + topic + headerCount(u16) + [headerStrLen(u32) + headerStr]... + msgSize(u32) + msg
func buildHProduceFrame(cID [4]byte, topic string, headers []string, msg []byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_HPRODUCE))
	buf = append(buf, cID[:]...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(headers)))
	for _, h := range headers {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(h)))
		buf = append(buf, h...)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msg)))
	buf = append(buf, msg...)
	return buf
}

// buildSubscribeFrame constructs: SUBSCRIBE opcode + correlationID(4b) + autoCommit(1b) + topicLen(u32) + topic
func buildSubscribeFrame(cID [4]byte, autoCommit bool, topic string) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_SUBSCRIBE))
	buf = append(buf, cID[:]...)
	if autoCommit {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)
	return buf
}

// buildFetchFrame constructs: FETCH opcode + correlationID(4b) + autoCommit(1b) + topicLen(u32) + topic + n(u32)
func buildFetchFrame(cID [4]byte, autoCommit bool, topic string, n uint32) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_FETCH))
	buf = append(buf, cID[:]...)
	if autoCommit {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)
	buf = binary.BigEndian.AppendUint32(buf, n)
	return buf
}

// buildTxBeginFrame constructs: TX_BEGIN opcode + correlationID(4b)
func buildTxBeginFrame(cID [4]byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_TX_BEGIN))
	buf = append(buf, cID[:]...)
	return buf
}

// buildTxCommitFrame constructs: TX_COMMIT opcode + correlationID(4b)
func buildTxCommitFrame(cID [4]byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_TX_COMMIT))
	buf = append(buf, cID[:]...)
	return buf
}

// buildTxRollbackFrame constructs: TX_ROLLBACK opcode + correlationID(4b)
func buildTxRollbackFrame(cID [4]byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_TX_ROLLBACK))
	buf = append(buf, cID[:]...)
	return buf
}

// buildAckFrame constructs: ACK opcode + correlationID(4b) + subID(1b) + msgIDsLen(u32) + [msgIDTopicLen(u32) + topic + msgIDLen(u32) + msgID]...
func buildAckFrame(cID [4]byte, subID byte, msgIDs []struct{ topic, id string }) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_ACK))
	buf = append(buf, cID[:]...)
	buf = append(buf, subID)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msgIDs)))
	for _, m := range msgIDs {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.topic)))
		buf = append(buf, m.topic...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.id)))
		buf = append(buf, m.id...)
	}
	return buf
}

// buildNackFrame constructs: NACK opcode + correlationID(4b) + subID(1b) + msgIDsLen(u32) + [msgIDTopicLen(u32) + topic + msgIDLen(u32) + msgID]...
func buildNackFrame(cID [4]byte, subID byte, msgIDs []struct{ topic, id string }) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_NACK))
	buf = append(buf, cID[:]...)
	buf = append(buf, subID)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msgIDs)))
	for _, m := range msgIDs {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.topic)))
		buf = append(buf, m.topic...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.id)))
		buf = append(buf, m.id...)
	}
	return buf
}

func buildDisconnectFrame() []byte {
	return []byte{byte(v1.OP_CODE_DISCONNECT)}
}

func buildPongFrame() []byte {
	return []byte{byte(v1.RESP_CODE_PONG)}
}

func buildUnsubscribeFrame(cID [4]byte, subID byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_UNSUBSCRIBE))
	buf = append(buf, cID[:]...)
	buf = append(buf, subID)
	return buf
}

// Ensure frame builders are referenced (used in various tests)
var (
	_ = buildSubscribeFrame
	_ = buildFetchFrame
	_ = buildAckFrame
	_ = buildNackFrame
)

// ---------------------------------------------------------------------------
// State Machine — Opcode Dispatch Tests
// ---------------------------------------------------------------------------

func TestHandle_BindState_AcceptsPong(t *testing.T) {
	th := newProtocolTestHarness()
	err := th.feed(buildPongFrame())
	assert.NoError(t, err)
	assert.Equal(t, OP_START, th.h.ps.state, "parser state should return to OP_START")
}

func TestHandle_BindState_AcceptsBind(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	frame := buildBindFrame("test-connector", nil, nil)
	err := th.feed(frame)
	assert.NoError(t, err)
	assert.Equal(t, OP_START, th.h.ps.state)
	assert.True(t, th.h.connected, "should be connected after BIND")
	assert.Equal(t, STREAM_STATE_CONNECTED, th.h.sessionState)
}

func TestHandle_BindState_RejectsProduceOpcode(t *testing.T) {
	th := newProtocolTestHarness()
	frame := buildProduceFrame([4]byte{0, 0, 0, 1}, "topic", []byte("msg"))
	err := th.feed(frame)
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_BindState_RejectsFetchOpcode(t *testing.T) {
	th := newProtocolTestHarness()
	err := th.feed([]byte{byte(v1.OP_CODE_FETCH)})
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_BindState_RejectsDisconnect(t *testing.T) {
	th := newProtocolTestHarness()
	err := th.feed(buildDisconnectFrame())
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_BindState_RejectsInvalidOpcode(t *testing.T) {
	th := newProtocolTestHarness()
	err := th.feed([]byte{0xFF})
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_BindState_AcceptsSubscribe(t *testing.T) {
	// In BIND state, SUBSCRIBE is allowed (sets state to OP_SUBSCRIBE)
	th := newProtocolTestHarness()
	// Just feed the opcode byte to check dispatch
	err := th.feed([]byte{byte(v1.OP_CODE_SUBSCRIBE)})
	assert.NoError(t, err)
	// Handler accepted subscribe and is now parsing its args
	assert.Equal(t, OP_SUBSCRIBE, th.h.ps.state)
}

func TestHandle_ConnectedState_AcceptsDisconnect(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed(buildDisconnectFrame())
	assert.ErrorIs(t, err, ErrClose)
}

func TestHandle_ConnectedState_AcceptsPong(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed(buildPongFrame())
	assert.NoError(t, err)
}

func TestHandle_ConnectedState_RejectsInvalidOpcode(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed([]byte{0xFF})
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_ConnectedState_RejectsWithoutConnected(t *testing.T) {
	// State is CONNECTED but connected flag is false (edge case)
	th := newProtocolTestHarness()
	th.h.sessionState = STREAM_STATE_CONNECTED
	th.h.connected = false
	err := th.feed([]byte{byte(v1.OP_CODE_PRODUCE)})
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_ConnectedState_AcceptsAllWriterCmds(t *testing.T) {
	ops := []struct {
		name    string
		opcode  byte
		expState int
	}{
		{"PRODUCE", byte(v1.OP_CODE_PRODUCE), OP_PRODUCE},
		{"HPRODUCE", byte(v1.OP_CODE_HPRODUCE), OP_PRODUCE_H},
		{"TX_BEGIN", byte(v1.OP_CODE_TX_BEGIN), OP_BEGIN_TX},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			th := newProtocolTestHarness()
			th.setConnected("test")
			err := th.feed([]byte{op.opcode})
			assert.NoError(t, err)
			assert.Equal(t, op.expState, th.h.ps.state)
		})
	}
}

func TestHandle_ConnectedState_AcceptsAllReaderCmds(t *testing.T) {
	ops := []struct {
		name     string
		opcode   byte
		expState int
	}{
		{"FETCH", byte(v1.OP_CODE_FETCH), OP_FETCH},
		{"SUBSCRIBE", byte(v1.OP_CODE_SUBSCRIBE), OP_SUBSCRIBE},
		{"UNSUBSCRIBE", byte(v1.OP_CODE_UNSUBSCRIBE), OP_UNSUBSCRIBE},
		{"ACK", byte(v1.OP_CODE_ACK), OP_ACK},
		{"NACK", byte(v1.OP_CODE_NACK), OP_NACK},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			th := newProtocolTestHarness()
			th.setConnected("test")
			err := th.feed([]byte{op.opcode})
			assert.NoError(t, err)
			assert.Equal(t, op.expState, th.h.ps.state)
		})
	}
}

func TestHandle_ConnectedState_HFetchSetsHeaderedFlag(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed([]byte{byte(v1.OP_CODE_HFETCH)})
	assert.NoError(t, err)
	assert.True(t, th.h.ps.fa.headered)
	assert.Equal(t, OP_FETCH, th.h.ps.state)
}

func TestHandle_ConnectedState_HSubscribeSetsHeaderedFlag(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed([]byte{byte(v1.OP_CODE_HSUBSCRIBE)})
	assert.NoError(t, err)
	assert.True(t, th.h.ps.sa.headered)
	assert.Equal(t, OP_SUBSCRIBE, th.h.ps.state)
}

func TestHandle_ConnectedState_TxCommitOutsideTx_DispatchToFail(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed([]byte{byte(v1.OP_CODE_TX_COMMIT)})
	assert.NoError(t, err)
	assert.Equal(t, OP_COMMIT_TX_FAIL, th.h.ps.state)
}

func TestHandle_ConnectedState_TxRollbackOutsideTx_DispatchToFail(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed([]byte{byte(v1.OP_CODE_TX_ROLLBACK)})
	assert.NoError(t, err)
	assert.Equal(t, OP_ROLLBACK_TX_FAIL, th.h.ps.state)
}

func TestHandle_InTxState_AcceptsDisconnect(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	err := th.feed(buildDisconnectFrame())
	assert.ErrorIs(t, err, ErrClose)
}

func TestHandle_InTxState_AcceptsPong(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	err := th.feed(buildPongFrame())
	assert.NoError(t, err)
}

func TestHandle_InTxState_ProduceDispatchesToTx(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	err := th.feed([]byte{byte(v1.OP_CODE_PRODUCE)})
	assert.NoError(t, err)
	assert.Equal(t, OP_PRODUCE_TX, th.h.ps.state)
}

func TestHandle_InTxState_TxBeginDispatchesToFail(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	err := th.feed([]byte{byte(v1.OP_CODE_TX_BEGIN)})
	assert.NoError(t, err)
	assert.Equal(t, OP_BEGIN_TX_FAIL, th.h.ps.state)
}

func TestHandle_InTxState_TxCommitDispatchesToCommit(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	err := th.feed([]byte{byte(v1.OP_CODE_TX_COMMIT)})
	assert.NoError(t, err)
	assert.Equal(t, OP_COMMIT_TX, th.h.ps.state)
}

func TestHandle_InTxState_TxRollbackDispatchesToRollback(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	err := th.feed([]byte{byte(v1.OP_CODE_TX_ROLLBACK)})
	assert.NoError(t, err)
	assert.Equal(t, OP_ROLLBACK_TX, th.h.ps.state)
}

func TestHandle_InTxState_RejectsInvalidOpcode(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	err := th.feed([]byte{0xFF})
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_InTxState_RejectsWithoutConnected(t *testing.T) {
	th := newProtocolTestHarness()
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	th.h.connected = false
	err := th.feed([]byte{byte(v1.OP_CODE_TX_COMMIT)})
	assert.ErrorIs(t, err, ErrParseProto)
}

// ---------------------------------------------------------------------------
// BIND Parsing Tests
// ---------------------------------------------------------------------------

func TestHandle_Bind_ParsesConnectorName(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	frame := buildBindFrame("my-connector", nil, nil)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
	assert.True(t, th.h.connected)
	assert.Equal(t, STREAM_STATE_CONNECTED, th.h.sessionState)
}

func TestHandle_Bind_WithMeta(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	meta := map[string]string{
		"api-key": "secret123",
		"user":    "test-user",
	}
	frame := buildBindFrame("test-conn", meta, nil)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.True(t, th.h.connected)
	assert.Equal(t, OP_START, th.h.ps.state)
}

func TestHandle_Bind_WithOverrides_ParsesCorrectly(t *testing.T) {
	// Note: ApplyOverrides may fail without a real connector factory registered,
	// but the parsing itself should succeed and the handler should respond with an error
	// response rather than crashing.
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	overrides := map[string]string{
		"writer.pub.transactional_id": "my-tx-id",
	}
	frame := buildBindFrame("test-conn", nil, overrides)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
}

func TestHandle_Bind_WithMetaAndOverrides(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	meta := map[string]string{"key1": "val1"}
	overrides := map[string]string{"setting1": "value1"}
	frame := buildBindFrame("conn1", meta, overrides)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
}

func TestHandle_Bind_AlreadyConnected(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	// First BIND
	frame := buildBindFrame("test-conn", nil, nil)
	err := th.feed(frame)
	require.NoError(t, err)
	assert.True(t, th.h.connected)

	// Second BIND should fail (already connected, so we're in CONNECTED state)
	// In CONNECTED state, BIND is not a valid opcode
	err = th.feed([]byte{byte(v1.OP_CODE_BIND)})
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_Bind_EmptyMeta(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	frame := buildBindFrame("test-conn", map[string]string{}, map[string]string{})
	err := th.feed(frame)
	require.NoError(t, err)
	assert.True(t, th.h.connected)
}

func TestHandle_Bind_Response(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	frame := buildBindFrame("test-conn", nil, nil)
	err := th.feed(frame)
	require.NoError(t, err)

	resp := th.readResponse(50 * time.Millisecond)
	require.GreaterOrEqual(t, len(resp), 2, "should have at least 2 bytes in response")
	assert.Equal(t, byte(v1.RESP_CODE_BIND), resp[0], "first byte should be BIND response code")
	assert.Equal(t, byte(v1.ERR_CODE_NO), resp[1], "second byte should be ERR_CODE_NO")
}

// ---------------------------------------------------------------------------
// PRODUCE Parsing Tests
// ---------------------------------------------------------------------------

func TestHandle_Produce_FullFrame(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("test-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	msg := []byte("hello world")
	frame := buildProduceFrame(cID, "test-topic", msg)
	err := th.feed(frame)
	require.NoError(t, err)

	// Wait for async produce callback
	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, msg, produced[0].msg)
}

func TestHandle_Produce_LargePayload(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("big-topic", w)

	cID := [4]byte{0, 0, 0, 2}
	msg := make([]byte, 64*1024) // 64KB
	for i := range msg {
		msg[i] = byte(i % 256)
	}

	frame := buildProduceFrame(cID, "big-topic", msg)
	err := th.feed(frame)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, msg, produced[0].msg)
}

func TestHandle_Produce_MultipleMessages(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("topic1", w)

	msgs := [][]byte{[]byte("msg1"), []byte("msg2"), []byte("msg3")}
	var combined []byte
	for i, msg := range msgs {
		cID := [4]byte{0, 0, 0, byte(i + 1)}
		combined = append(combined, buildProduceFrame(cID, "topic1", msg)...)
	}

	err := th.feed(combined)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 3)
	for i, p := range produced {
		assert.Equal(t, msgs[i], p.msg)
	}
}

func TestHandle_Produce_Response(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("resp-topic", w)

	cID := [4]byte{0, 0, 0, 42}
	frame := buildProduceFrame(cID, "resp-topic", []byte("data"))
	err := th.feed(frame)
	require.NoError(t, err)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	// Response: RESP_CODE_PRODUCE(1b) + correlationID(4b) + ERR_CODE_NO(1b)
	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_PRODUCE), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.ERR_CODE_NO), resp[5])
}

func TestHandle_Produce_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("byte-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	frame := buildProduceFrame(cID, "byte-topic", []byte("byte-by-byte"))

	// Feed one byte at a time
	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("byte-by-byte"), produced[0].msg)
}

func TestHandle_Produce_ChunkedInput(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("chunk-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	frame := buildProduceFrame(cID, "chunk-topic", []byte("chunked-data-payload"))

	// Feed in random chunks
	chunks := []int{3, 7, 2, 5, 1, 4, 100} // last one will be clamped
	offset := 0
	for _, size := range chunks {
		end := offset + size
		if end > len(frame) {
			end = len(frame)
		}
		if offset >= len(frame) {
			break
		}
		err := th.feed(frame[offset:end])
		require.NoError(t, err)
		offset = end
	}

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("chunked-data-payload"), produced[0].msg)
}

func TestHandle_Produce_StateResetAfterComplete(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("topic", w)

	cID := [4]byte{0, 0, 0, 1}
	frame := buildProduceFrame(cID, "topic", []byte("msg"))
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state, "state should be reset to OP_START after complete PRODUCE")
	assert.Nil(t, th.h.ps.ca.cID, "correlation ID should be nil after complete PRODUCE")

	th.close(done)
}

// ---------------------------------------------------------------------------
// HPRODUCE Parsing Tests
// ---------------------------------------------------------------------------

func TestHandle_HProduce_WithHeaders(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("h-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	headers := []string{"key1", "val1", "key2", "val2"} // 4 strings = 2 key-value pairs
	msg := []byte("headered-msg")
	frame := buildHProduceFrame(cID, "h-topic", headers, msg)
	err := th.feed(frame)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, msg, produced[0].msg)
	require.Len(t, produced[0].headers, 4)
	assert.Equal(t, "key1", string(produced[0].headers[0]))
	assert.Equal(t, "val1", string(produced[0].headers[1]))
	assert.Equal(t, "key2", string(produced[0].headers[2]))
	assert.Equal(t, "val2", string(produced[0].headers[3]))
}

func TestHandle_HProduce_ZeroHeaders(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("h-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	frame := buildHProduceFrame(cID, "h-topic", nil, []byte("no-headers"))
	err := th.feed(frame)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("no-headers"), produced[0].msg)
	assert.Empty(t, produced[0].headers)
}

func TestHandle_HProduce_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("h-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	headers := []string{"h1", "v1"}
	frame := buildHProduceFrame(cID, "h-topic", headers, []byte("hdr-msg"))

	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("hdr-msg"), produced[0].msg)
}

func TestHandle_HProduce_Response(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("h-topic", w)

	cID := [4]byte{0, 0, 0, 7}
	frame := buildHProduceFrame(cID, "h-topic", nil, []byte("x"))
	err := th.feed(frame)
	require.NoError(t, err)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_HPRODUCE), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.ERR_CODE_NO), resp[5])
}

// ---------------------------------------------------------------------------
// BIND Parsing — Fragmented Input
// ---------------------------------------------------------------------------

func TestHandle_Bind_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	frame := buildBindFrame("my-conn", map[string]string{"k": "v"}, nil)

	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	assert.True(t, th.h.connected)
	assert.Equal(t, OP_START, th.h.ps.state)
}

// ---------------------------------------------------------------------------
// TX Operations Tests
// ---------------------------------------------------------------------------

func TestHandle_TxBegin_FromConnected(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	frame := buildTxBeginFrame(cID)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
	assert.Equal(t, STREAM_STATE_CONNECTED_IN_TX, th.h.sessionState)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_BEGIN), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.ERR_CODE_NO), resp[5])
}

func TestHandle_TxBegin_WhenAlreadyInTx(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX

	cID := [4]byte{0, 0, 0, 2}
	frame := buildTxBeginFrame(cID)
	err := th.feed(frame)
	require.NoError(t, err)

	// Should return to OP_START
	assert.Equal(t, OP_START, th.h.ps.state)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	// Should get an error response
	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_BEGIN), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.ERR_CODE_YES), resp[5])
}

func TestHandle_TxCommit_InTxState(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	// Set up a mock tx writer
	w := &mockConnectorWriter{}
	th.h.currentTxWriter = w
	th.h.currentTxWriterTopic = "tx-topic"

	cID := [4]byte{0, 0, 0, 3}
	frame := buildTxCommitFrame(cID)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
	assert.Equal(t, STREAM_STATE_CONNECTED, th.h.sessionState)

	w.mu.Lock()
	assert.True(t, w.txCommitted)
	w.mu.Unlock()

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_COMMIT), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.ERR_CODE_NO), resp[5])
}

func TestHandle_TxCommit_OutsideTx(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 4}
	frame := buildTxCommitFrame(cID)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	// Should get error response (invalid tx state)
	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_COMMIT), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.ERR_CODE_YES), resp[5])
}

func TestHandle_TxRollback_InTxState(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	w := &mockConnectorWriter{}
	th.h.currentTxWriter = w
	th.h.currentTxWriterTopic = "tx-topic"

	cID := [4]byte{0, 0, 0, 5}
	frame := buildTxRollbackFrame(cID)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
	assert.Equal(t, STREAM_STATE_CONNECTED, th.h.sessionState)

	w.mu.Lock()
	assert.True(t, w.txRolledBack)
	w.mu.Unlock()

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_ROLLBACK), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.ERR_CODE_NO), resp[5])
}

func TestHandle_TxRollback_OutsideTx(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 6}
	frame := buildTxRollbackFrame(cID)
	err := th.feed(frame)
	require.NoError(t, err)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_ROLLBACK), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.ERR_CODE_YES), resp[5])
}

// ---------------------------------------------------------------------------
// FETCH Parsing Tests (parsing only — up to the fetch() call)
// ---------------------------------------------------------------------------

func TestHandle_Fetch_ParsesArgs(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")

	// Build a fetch frame manually up to the point where fetch() would be called
	cID := [4]byte{0, 0, 0, 1}

	// Feed opcode
	err := th.feed([]byte{byte(v1.OP_CODE_FETCH)})
	require.NoError(t, err)
	assert.Equal(t, OP_FETCH, th.h.ps.state)

	// Feed correlation ID
	err = th.feed(cID[:])
	require.NoError(t, err)
	assert.Equal(t, OP_FETCH_AUTO_COMMIT_ARG, th.h.ps.state)

	// Feed autoCommit = true
	err = th.feed([]byte{1})
	require.NoError(t, err)
	assert.True(t, th.h.ps.fa.autoCommit)
	assert.Equal(t, OP_FETCH_TOPIC_ARG, th.h.ps.state)

	// Feed topic length (5 = "topic")
	topicLen := make([]byte, 4)
	binary.BigEndian.PutUint32(topicLen, 5)
	err = th.feed(topicLen)
	require.NoError(t, err)
	assert.Equal(t, uint32(5), th.h.ps.fa.topicLen)
	assert.Equal(t, OP_FETCH_TOPIC_PAYLOAD, th.h.ps.state)

	// Feed topic
	err = th.feed([]byte("topic"))
	require.NoError(t, err)
	assert.Equal(t, "topic", th.h.ps.fa.topic)
	assert.Equal(t, OP_FETCH_N_ARG, th.h.ps.state)
}

func TestHandle_Fetch_InvalidAutoCommit(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	// Opcode + cID + invalid autoCommit byte
	frame := append([]byte{byte(v1.OP_CODE_FETCH)}, cID[:]...)
	frame = append(frame, 2) // invalid: not 0 or 1
	err := th.feed(frame)
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// SUBSCRIBE Parsing Tests (parsing only)
// ---------------------------------------------------------------------------

func TestHandle_Subscribe_ParsesArgs(t *testing.T) {
	th := newProtocolTestHarness()

	cID := [4]byte{0, 0, 0, 1}

	// Feed opcode
	err := th.feed([]byte{byte(v1.OP_CODE_SUBSCRIBE)})
	require.NoError(t, err)
	assert.Equal(t, OP_SUBSCRIBE, th.h.ps.state)

	// Feed correlation ID
	err = th.feed(cID[:])
	require.NoError(t, err)
	assert.Equal(t, OP_SUBSCRIBE_AUTO_COMMIT_ARG, th.h.ps.state)

	// Feed autoCommit = false
	err = th.feed([]byte{0})
	require.NoError(t, err)
	assert.False(t, th.h.ps.sa.autoCommit)
	assert.Equal(t, OP_SUBSCRIBE_TOPIC_ARG, th.h.ps.state)

	// Feed topic length (10 = "test-topic")
	topicLen := make([]byte, 4)
	binary.BigEndian.PutUint32(topicLen, 10)
	err = th.feed(topicLen)
	require.NoError(t, err)
	assert.Equal(t, uint32(10), th.h.ps.sa.topicLen)
	assert.Equal(t, OP_SUBSCRIBE_TOPIC_PAYLOAD, th.h.ps.state)
}

func TestHandle_Subscribe_InvalidAutoCommit(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	cID := [4]byte{0, 0, 0, 1}
	frame := append([]byte{byte(v1.OP_CODE_SUBSCRIBE)}, cID[:]...)
	frame = append(frame, 5) // invalid
	err := th.feed(frame)
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// ACK Parsing Tests (parsing only — up to ack handler call)
// ---------------------------------------------------------------------------

func TestHandle_Ack_ZeroMsgIDs(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	// Build ACK frame with zero msgIDs (triggers enqueueAckSuccess)
	var frame []byte
	frame = append(frame, byte(v1.OP_CODE_ACK))
	frame = append(frame, cID[:]...)
	frame = append(frame, 42) // subID = 42
	frame = binary.BigEndian.AppendUint32(frame, 0) // 0 msgIDs

	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state, "should reset to OP_START after zero-msgID ACK")

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	// Should get ACK success: RESP_CODE_ACK(1) + cID(4) + ERR_CODE_NO(1) + count(4) = 10 bytes
	require.GreaterOrEqual(t, len(resp), 10)
	assert.Equal(t, byte(v1.RESP_CODE_ACK), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.ERR_CODE_NO), resp[5])
	// Verify count = 0 in response
	respCount := binary.BigEndian.Uint32(resp[6:10])
	assert.Equal(t, uint32(0), respCount)
}

// ---------------------------------------------------------------------------
// PONG Tests
// ---------------------------------------------------------------------------

func TestHandle_Pong_AllStates(t *testing.T) {
	states := []struct {
		name  string
		state sessionState
	}{
		{"BIND", STREAM_STATE_BIND},
		{"CONNECTED", STREAM_STATE_CONNECTED},
		{"IN_TX", STREAM_STATE_CONNECTED_IN_TX},
	}

	for _, st := range states {
		t.Run(st.name, func(t *testing.T) {
			th := newProtocolTestHarness()
			th.h.sessionState = st.state
			if st.state != STREAM_STATE_BIND {
				th.h.connected = true
			}
			err := th.feed(buildPongFrame())
			assert.NoError(t, err)
			assert.Equal(t, OP_START, th.h.ps.state)
		})
	}
}

// ---------------------------------------------------------------------------
// DISCONNECT Tests
// ---------------------------------------------------------------------------

func TestHandle_Disconnect_FromConnected(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed(buildDisconnectFrame())
	assert.ErrorIs(t, err, ErrClose)
}

func TestHandle_Disconnect_FromInTx(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	err := th.feed(buildDisconnectFrame())
	assert.ErrorIs(t, err, ErrClose)
}

// ---------------------------------------------------------------------------
// Fragmented Input Tests
// ---------------------------------------------------------------------------

func TestHandle_ProduceThenDisconnect_InOneBuffer(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("topic", w)

	cID := [4]byte{0, 0, 0, 1}
	produceFrame := buildProduceFrame(cID, "topic", []byte("msg"))
	disconnectFrame := buildDisconnectFrame()

	combined := append(produceFrame, disconnectFrame...)
	err := th.feed(combined)
	assert.ErrorIs(t, err, ErrClose)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	// Produce should have been processed before disconnect
	produced := w.getProduced()
	assert.Len(t, produced, 1)
	assert.Equal(t, []byte("msg"), produced[0].msg)
}

func TestHandle_MultipleProducesInOneBuffer(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("topic", w)

	var combined []byte
	for i := 0; i < 10; i++ {
		cID := [4]byte{0, 0, 0, byte(i + 1)}
		combined = append(combined, buildProduceFrame(cID, "topic", []byte("data"))...)
	}

	err := th.feed(combined)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	assert.Len(t, produced, 10)
}

func TestHandle_BindThenProduce_Sequential(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()

	// First: BIND
	bindFrame := buildBindFrame("test-conn", nil, nil)
	err := th.feed(bindFrame)
	require.NoError(t, err)
	require.True(t, th.h.connected)

	// Inject a writer into the now-connected handler
	w := &mockConnectorWriter{}
	th.h.nonTxSessionWriters["my-topic"] = w

	// Second: PRODUCE
	cID := [4]byte{0, 0, 0, 1}
	produceFrame := buildProduceFrame(cID, "my-topic", []byte("after-bind"))
	err = th.feed(produceFrame)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("after-bind"), produced[0].msg)
}

func TestHandle_BindThenProduce_InOneBuffer(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()

	bindFrame := buildBindFrame("test-conn", nil, nil)

	// We can't really do PRODUCE in the same buffer as BIND because
	// handleBind creates the cman but doesn't register writer factories.
	// However, we can test that parsing continues correctly.
	err := th.feed(bindFrame)
	require.NoError(t, err)
	assert.True(t, th.h.connected)
	assert.Equal(t, OP_START, th.h.ps.state)

	th.close(done)
}

// ---------------------------------------------------------------------------
// TX_BEGIN → PRODUCE → TX_COMMIT flow
// ---------------------------------------------------------------------------

func TestHandle_TxBegin_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	frame := buildTxBeginFrame(cID)

	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	assert.Equal(t, STREAM_STATE_CONNECTED_IN_TX, th.h.sessionState)
	th.close(done)
}

func TestHandle_TxCommit_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	w := &mockConnectorWriter{}
	th.h.currentTxWriter = w
	th.h.currentTxWriterTopic = "tx-topic"

	cID := [4]byte{0, 0, 0, 1}
	frame := buildTxCommitFrame(cID)

	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	assert.Equal(t, STREAM_STATE_CONNECTED, th.h.sessionState)
	w.mu.Lock()
	assert.True(t, w.txCommitted)
	w.mu.Unlock()
	th.close(done)
}

func TestHandle_TxRollback_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	th.h.sessionState = STREAM_STATE_CONNECTED_IN_TX
	w := &mockConnectorWriter{}
	th.h.currentTxWriter = w
	th.h.currentTxWriterTopic = "tx-topic"

	cID := [4]byte{0, 0, 0, 1}
	frame := buildTxRollbackFrame(cID)

	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	assert.Equal(t, STREAM_STATE_CONNECTED, th.h.sessionState)
	w.mu.Lock()
	assert.True(t, w.txRolledBack)
	w.mu.Unlock()
	th.close(done)
}

// ---------------------------------------------------------------------------
// UNSUBSCRIBE Parsing Tests
// ---------------------------------------------------------------------------

func TestHandle_Unsubscribe_ParsesCorrelationAndSubID(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	frame := buildUnsubscribeFrame(cID, 5)
	err := th.feed(frame)
	require.NoError(t, err)
	assert.Equal(t, OP_START, th.h.ps.state)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	// Response: RESP_CODE_UNSUBSCRIBE(1) + cID(4) + ERR_CODE_NO(1) = 6 bytes
	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_UNSUBSCRIBE), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.ERR_CODE_NO), resp[5])
}

// ---------------------------------------------------------------------------
// Empty input
// ---------------------------------------------------------------------------

func TestHandle_EmptyInput(t *testing.T) {
	th := newProtocolTestHarness()
	err := th.feed([]byte{})
	assert.NoError(t, err)
	assert.Equal(t, OP_START, th.h.ps.state)
}

// ---------------------------------------------------------------------------
// Produce with empty payload (msg size = 0) should error
// ---------------------------------------------------------------------------

func TestHandle_Produce_EmptyPayload(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	frame := buildProduceFrame(cID, "topic", []byte{})
	err := th.feed(frame)
	// Empty msg size (0) should trigger ErrWriteMsgSizeArgEmpty
	assert.ErrorIs(t, err, ErrWriteMsgSizeArgEmpty)

	th.close(done)
}

// ---------------------------------------------------------------------------
// Produce with empty topic should error
// ---------------------------------------------------------------------------

func TestHandle_Produce_EmptyTopic(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	// Build frame with topic len = 0 which will fail validation
	var frame []byte
	frame = append(frame, byte(v1.OP_CODE_PRODUCE))
	frame = append(frame, cID[:]...)
	frame = binary.BigEndian.AppendUint32(frame, 0) // topic len = 0
	err := th.feed(frame)
	assert.ErrorIs(t, err, ErrWriteTopicLenArgEmpty)

	th.close(done)
}

// ---------------------------------------------------------------------------
// Multiple sequential commands across handle() calls
// ---------------------------------------------------------------------------

func TestHandle_SequentialProduces_AcrossCalls(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("topic", w)

	for i := 0; i < 5; i++ {
		cID := [4]byte{0, 0, 0, byte(i + 1)}
		frame := buildProduceFrame(cID, "topic", []byte("seq-msg"))
		err := th.feed(frame)
		require.NoError(t, err)
		assert.Equal(t, OP_START, th.h.ps.state)
	}

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	assert.Len(t, produced, 5)
	for _, p := range produced {
		assert.Equal(t, []byte("seq-msg"), p.msg)
	}
}

// ---------------------------------------------------------------------------
// BIND response verification with stream output
// ---------------------------------------------------------------------------

func TestHandle_Bind_MultipleResponseBytes(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()

	frame := buildBindFrame("conn", nil, nil)
	err := th.feed(frame)
	require.NoError(t, err)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	// BIND response is exactly 2 bytes: RESP_CODE_BIND + ERR_CODE_NO
	require.GreaterOrEqual(t, len(resp), 2)

	// Verify first two bytes
	assert.Equal(t, byte(v1.RESP_CODE_BIND), resp[0])
	assert.Equal(t, byte(v1.ERR_CODE_NO), resp[1])
}

// ---------------------------------------------------------------------------
// Verify BIND sets up connector manager
// ---------------------------------------------------------------------------

func TestHandle_Bind_SetsUpConnectorManager(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	assert.Nil(t, th.h.cman, "cman should be nil before BIND")
	assert.Nil(t, th.h.nonTxSessionWriters, "writers should be nil before BIND")

	frame := buildBindFrame("my-connector", nil, nil)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.NotNil(t, th.h.cman, "cman should be set after BIND")
	assert.NotNil(t, th.h.nonTxSessionWriters, "writers map should be initialized after BIND")
}

// ---------------------------------------------------------------------------
// Verify handler starts with clean state
// ---------------------------------------------------------------------------

func TestHandle_InitialState(t *testing.T) {
	th := newProtocolTestHarness()
	h := th.h

	assert.Equal(t, OP_START, h.ps.state)
	assert.Equal(t, STREAM_STATE_BIND, h.sessionState)
	assert.False(t, h.connected)
	assert.Nil(t, h.cman)
	assert.Nil(t, h.nonTxSessionWriters)
	assert.NotNil(t, h.ps)
	assert.NotNil(t, h.out)
}

// ---------------------------------------------------------------------------
// PRODUCE with multiple topics
// ---------------------------------------------------------------------------

func TestHandle_Produce_MultipleDifferentTopics(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w1 := &mockConnectorWriter{}
	w2 := &mockConnectorWriter{}
	th.setConnected("test")
	th.h.nonTxSessionWriters["topic-a"] = w1
	th.h.nonTxSessionWriters["topic-b"] = w2

	cID1 := [4]byte{0, 0, 0, 1}
	cID2 := [4]byte{0, 0, 0, 2}
	frame1 := buildProduceFrame(cID1, "topic-a", []byte("for-a"))
	frame2 := buildProduceFrame(cID2, "topic-b", []byte("for-b"))

	err := th.feed(append(frame1, frame2...))
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced1 := w1.getProduced()
	produced2 := w2.getProduced()
	require.Len(t, produced1, 1)
	require.Len(t, produced2, 1)
	assert.Equal(t, []byte("for-a"), produced1[0].msg)
	assert.Equal(t, []byte("for-b"), produced2[0].msg)
}

// ---------------------------------------------------------------------------
// HProduce with many headers
// ---------------------------------------------------------------------------

func TestHandle_HProduce_MultipleHeaders(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("h-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	// 6 strings = 3 key-value header pairs
	headers := []string{"k1", "v1", "k2", "v2", "k3", "v3"}
	frame := buildHProduceFrame(cID, "h-topic", headers, []byte("msg"))
	err := th.feed(frame)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	require.Len(t, produced[0].headers, 6)
	for i, h := range headers {
		assert.Equal(t, h, string(produced[0].headers[i]))
	}
}

// ---------------------------------------------------------------------------
// Verify stream reads work with the test harness
// ---------------------------------------------------------------------------

func TestHarness_StreamCapture(t *testing.T) {
	// Basic sanity check that our test harness captures outbound data
	str := &mockStream{}
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	out := NewOutbound(str, 5*time.Second, l)
	done := make(chan struct{})
	go func() {
		out.WriteLoop()
		close(done)
	}()

	out.EnqueueProto([]byte("hello"))
	time.Sleep(50 * time.Millisecond)

	data := str.written()
	assert.Equal(t, []byte("hello"), data)

	out.Close()
	<-done
}

// ---------------------------------------------------------------------------
// Large batch: many PRODUCEs in one buffer
// ---------------------------------------------------------------------------

func TestHandle_Produce_LargeBatch(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("batch", w)

	const count = 100
	var combined []byte
	for i := 0; i < count; i++ {
		cID := [4]byte{0, 0, byte(i >> 8), byte(i & 0xFF)}
		combined = append(combined, buildProduceFrame(cID, "batch", []byte("batch-data"))...)
	}

	err := th.feed(combined)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	assert.Len(t, produced, count)
}

// ---------------------------------------------------------------------------
// Correlation ID preservation
// ---------------------------------------------------------------------------

func TestHandle_Produce_CorrelationIDPreservedInResponse(t *testing.T) {
	cIDs := [][4]byte{
		{0x00, 0x00, 0x00, 0x01},
		{0xDE, 0xAD, 0xBE, 0xEF},
		{0xFF, 0xFF, 0xFF, 0xFF},
	}

	for _, cID := range cIDs {
		t.Run(string(bytes.Join([][]byte{cID[:]}, nil)), func(t *testing.T) {
			th := newProtocolTestHarness()
			done := th.startWriteLoop()
			w := &mockConnectorWriter{}
			th.setConnectedWithWriter("topic", w)

			frame := buildProduceFrame(cID, "topic", []byte("x"))
			err := th.feed(frame)
			require.NoError(t, err)

			resp := th.readResponse(50 * time.Millisecond)
			th.close(done)

			require.GreaterOrEqual(t, len(resp), 6)
			assert.Equal(t, cID[:], resp[1:5], "correlation ID should be preserved in response")
		})
	}
}
