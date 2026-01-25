package connector

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/fujin-io/fujin/public/plugins/connector/config"
)

// mockReadCloser is a test reader implementation
type mockReadCloser struct {
	Reader
	closeCalled bool
}

func (m *mockReadCloser) Close() error {
	m.closeCalled = true
	return nil
}

// mockWriteCloser is a test writer implementation
type mockWriteCloser struct {
	Writer
	closeCalled bool
}

func (m *mockWriteCloser) Close() error {
	m.closeCalled = true
	return nil
}

// mockReader implements reader.Reader interface
type mockReader struct{}

func (m *mockReader) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	return nil
}

func (m *mockReader) HSubscribe(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	return nil
}

func (m *mockReader) Fetch(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, args ...any)) {
}

func (m *mockReader) HFetch(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, hs [][]byte, args ...any)) {
}

func (m *mockReader) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
}

func (m *mockReader) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
}

func (m *mockReader) MsgIDStaticArgsLen() int {
	return 0
}

func (m *mockReader) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return buf
}

func (m *mockReader) IsAutoCommit() bool {
	return false
}

// mockWriter implements writer.Writer interface
type mockWriter struct{}

func (m *mockWriter) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	if callback != nil {
		callback(nil)
	}
}

func (m *mockWriter) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	if callback != nil {
		callback(nil)
	}
}

func (m *mockWriter) Flush(ctx context.Context) error {
	return nil
}

func (m *mockWriter) BeginTx(ctx context.Context) error {
	return nil
}

func (m *mockWriter) CommitTx(ctx context.Context) error {
	return nil
}

func (m *mockWriter) RollbackTx(ctx context.Context) error {
	return nil
}

func TestRegisterConfigValueConverter(t *testing.T) {
	// Register a test connector with converter
	_ = Register("test_protocol_converter", func(config any, l *slog.Logger) (Connector, error) {
		return &mockConnector{
			converter: func(settingPath string, value string) (any, error) {
				return value, nil
			},
		}, nil
	})

	// Verify converter is accessible
	converter := GetConfigValueConverter("test_protocol_converter")
	if converter == nil {
		t.Fatal("GetConfigValueConverter() should return converter from connector")
	}

	// Test the converter
	result, err := converter("test.path", "test_value")
	if err != nil {
		t.Fatalf("converter() error = %v", err)
	}
	if result != "test_value" {
		t.Fatalf("converter() = %v, want test_value", result)
	}
}

func TestGetConfigValueConverter(t *testing.T) {
	// Get non-existent converter
	converter := GetConfigValueConverter("nonexistent")
	if converter != nil {
		t.Fatal("GetConfigValueConverter() should return nil for non-existent protocol")
	}

	// Register and get
	_ = Register("test_get", func(config any, l *slog.Logger) (Connector, error) {
		return &mockConnector{
			converter: func(settingPath string, value string) (any, error) {
				return value, nil
			},
		}, nil
	})

	converter = GetConfigValueConverter("test_get")
	if converter == nil {
		t.Fatal("GetConfigValueConverter() should find registered converter")
	}
}

func TestNewWriter(t *testing.T) {
	l := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Register a test writer factory
	_ = Register("test_new_writer", func(config any, l *slog.Logger) (Connector, error) {
		return &mockConnector{}, nil
	})

	// Test NewWriter with registered protocol
	conf := config.ConnectorConfig{
		Protocol: "test_new_writer",
		Settings: "test_settings",
	}

	w, err := NewWriter(conf, "test_name", l)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	if w == nil {
		t.Fatal("NewWriter() should return a writer")
	}

	// Test Close
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Test NewWriter with unregistered protocol
	conf.Protocol = "nonexistent"
	_, err = NewWriter(conf, "test_name", l)
	if err == nil {
		t.Fatal("NewWriter() should return error for unregistered protocol")
	}
}

func TestNewReader(t *testing.T) {
	l := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Register a test reader factory
	_ = Register("test_new_reader", func(config any, l *slog.Logger) (Connector, error) {
		return &mockConnector{}, nil
	})

	// Test NewReader with registered protocol
	conf := config.ConnectorConfig{
		Protocol: "test_new_reader",
		Settings: "test_settings",
	}

	r, err := NewReader(conf, "test_name", false, l)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	if r == nil {
		t.Fatal("NewReader() should return a reader")
	}

	// Test Close
	if err := r.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Test NewReader with autoCommit
	r, err = NewReader(conf, "test_name", true, l)
	if err != nil {
		t.Fatalf("NewReader() with autoCommit error = %v", err)
	}
	if r == nil {
		t.Fatal("NewReader() should return a reader")
	}

	// Test NewReader with unregistered protocol
	conf.Protocol = "nonexistent"
	_, err = NewReader(conf, "test_name", false, l)
	if err == nil {
		t.Fatal("NewReader() should return error for unregistered protocol")
	}
}

func TestNewReader_WithError(t *testing.T) {
	l := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Test with non-existent protocol
	conf := config.ConnectorConfig{
		Protocol: "nonexistent_reader",
		Settings: "test_config",
	}
	_, err := NewReader(conf, "test_name", false, l)
	if err == nil {
		t.Fatal("NewReader() should have failed for non-existent protocol")
	}

	// Test with connector that returns error from factory
	_ = Register("error_factory_reader", func(config any, l *slog.Logger) (Connector, error) {
		return nil, errors.New("factory error")
	})

	conf.Protocol = "error_factory_reader"
	_, err = NewReader(conf, "test_name", false, l)
	if err == nil {
		t.Fatal("NewReader() should return error when factory fails")
	}
}

func TestNewWriter_WithError(t *testing.T) {
	l := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Test with non-existent protocol
	conf := config.ConnectorConfig{
		Protocol: "nonexistent_writer",
		Settings: "test_config",
	}
	_, err := NewWriter(conf, "test_name", l)
	if err == nil {
		t.Fatal("NewWriter() should have failed for non-existent protocol")
	}

	// Test with connector that returns error from factory
	_ = Register("error_factory", func(config any, l *slog.Logger) (Connector, error) {
		return nil, errors.New("factory error")
	})

	conf.Protocol = "error_factory"
	_, err = NewWriter(conf, "test_name", l)
	if err == nil {
		t.Fatal("NewWriter() should return error when factory fails")
	}
}

func TestConnectorConfig(t *testing.T) {
	// Test config.ConnectorConfig structure
	conf := config.ConnectorConfig{
		Protocol:    "test",
		Overridable: []string{"path1", "path2"},
		Settings:    map[string]any{"key": "value"},
	}

	if conf.Protocol != "test" {
		t.Errorf("Protocol = %v, want test", conf.Protocol)
	}
	if len(conf.Overridable) != 2 {
		t.Errorf("Overridable length = %v, want 2", len(conf.Overridable))
	}
	if conf.Settings == nil {
		t.Fatal("Settings should not be nil")
	}
}

func TestConnectorsConfig(t *testing.T) {
	// Test config.ConnectorsConfig map
	configs := config.ConnectorsConfig{
		"connector1": config.ConnectorConfig{
			Protocol: "protocol1",
			Settings: "settings1",
		},
		"connector2": config.ConnectorConfig{
			Protocol: "protocol2",
			Settings: "settings2",
		},
	}

	if len(configs) != 2 {
		t.Errorf("config.ConnectorsConfig length = %v, want 2", len(configs))
	}

	if configs["connector1"].Protocol != "protocol1" {
		t.Errorf("connector1.Protocol = %v, want protocol1", configs["connector1"].Protocol)
	}
}

func TestList(t *testing.T) {
	// Register some connectors to ensure List() has something to return
	_ = Register("list_test_connector", func(config any, l *slog.Logger) (Connector, error) {
		return &mockConnector{}, nil
	})

	protocols := List()
	if len(protocols) == 0 {
		t.Fatal("List() should return registered protocols")
	}

	// Check that our registered protocol is in the list
	found := false
	for _, protocol := range protocols {
		if protocol == "list_test_connector" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("List() should contain 'list_test_connector' protocol")
	}
}

// mockConnector is a test connector implementation
type mockConnector struct {
	readerCalls int
	writerCalls int
	converter   ConfigValueConverterFunc
}

func (m *mockConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (ReadCloser, error) {
	m.readerCalls++
	return &mockReadCloser{Reader: &mockReader{}}, nil
}

func (m *mockConnector) NewWriter(config any, name string, l *slog.Logger) (WriteCloser, error) {
	m.writerCalls++
	return &mockWriteCloser{Writer: &mockWriter{}}, nil
}

func (m *mockConnector) GetConfigValueConverter() ConfigValueConverterFunc {
	return m.converter
}

func TestRegister(t *testing.T) {
	// Register a test connector
	err := Register("test_connector", func(config any, l *slog.Logger) (Connector, error) {
		return &mockConnector{}, nil
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// Try to register again - should fail
	err = Register("test_connector", func(config any, l *slog.Logger) (Connector, error) {
		return &mockConnector{}, nil
	})
	if err == nil {
		t.Fatal("Register() should have failed for duplicate protocol")
	}
}

func TestGet(t *testing.T) {
	// Register a connector for this test
	_ = Register("get_test_connector", func(config any, l *slog.Logger) (Connector, error) {
		return &mockConnector{}, nil
	})

	// Get existing connector factory
	factory, ok := Get("get_test_connector")
	if !ok {
		t.Fatal("Get() should find registered factory")
	}
	if factory == nil {
		t.Fatal("Get() returned nil factory")
	}

	// Get non-existent connector factory
	_, ok = Get("nonexistent_connector")
	if ok {
		t.Fatal("Get() should not find non-existent factory")
	}
}

func TestNewReaderWithConnector(t *testing.T) {
	l := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Register a connector using the new interface
	_ = Register("test_connector_reader", func(config any, l *slog.Logger) (Connector, error) {
		return &mockConnector{}, nil
	})

	// Test creating a reader using the connector interface
	conf := config.ConnectorConfig{
		Protocol: "test_connector_reader",
		Settings: "test_config",
	}

	r, err := NewReader(conf, "test_name", false, l)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	if r == nil {
		t.Fatal("NewReader() should return a reader")
	}
}

func TestNewWriterWithConnector(t *testing.T) {
	l := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Register a connector using the new interface
	_ = Register("test_connector_writer", func(config any, l *slog.Logger) (Connector, error) {
		return &mockConnector{}, nil
	})

	// Test creating a writer using the connector interface
	conf := config.ConnectorConfig{
		Protocol: "test_connector_writer",
		Settings: "test_config",
	}

	w, err := NewWriter(conf, "test_name", l)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	if w == nil {
		t.Fatal("NewWriter() should return a writer")
	}
}

func TestConfigValueConverter_ErrorHandling(t *testing.T) {
	// Register a connector with a converter that returns an error
	_ = Register("error_converter", func(config any, l *slog.Logger) (Connector, error) {
		return &mockConnector{
			converter: func(settingPath string, value string) (any, error) {
				return nil, errors.New("conversion error")
			},
		}, nil
	})

	converter := GetConfigValueConverter("error_converter")
	if converter == nil {
		t.Fatal("GetConfigValueConverter() should return registered converter")
	}

	_, err := converter("test.path", "test_value")
	if err == nil {
		t.Fatal("converter() should return error when conversion fails")
	}
}
