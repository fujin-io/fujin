package decorator

import (
	"log/slog"
	"testing"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

// mockDecorator is a test decorator
type mockDecorator struct {
	writerCalls int
	readerCalls int
}

func (m *mockDecorator) WrapWriter(w connector.Writer, connectorName string) connector.Writer {
	m.writerCalls++
	return w
}

func (m *mockDecorator) WrapReader(r connector.Reader, connectorName string) connector.Reader {
	m.readerCalls++
	return r
}

func TestRegister(t *testing.T) {
	// Register a test decorator
	err := Register("test_decorator", func(config any, l *slog.Logger) (Decorator, error) {
		return &mockDecorator{}, nil
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// Try to register again - should fail
	err = Register("test_decorator", func(config any, l *slog.Logger) (Decorator, error) {
		return &mockDecorator{}, nil
	})
	if err == nil {
		t.Fatal("Register() should have failed for duplicate name")
	}
}

func TestGet(t *testing.T) {
	// Register a decorator for this test
	_ = Register("get_test", func(config any, l *slog.Logger) (Decorator, error) {
		return &mockDecorator{}, nil
	})

	// Get existing decorator
	factory, ok := Get("get_test")
	if !ok {
		t.Fatal("Get() should find registered decorator")
	}
	if factory == nil {
		t.Fatal("Get() returned nil factory")
	}

	// Get non-existent decorator
	_, ok = Get("nonexistent")
	if ok {
		t.Fatal("Get() should not find non-existent decorator")
	}
}

func TestList(t *testing.T) {
	// Register a decorator to ensure List() has something to return
	_ = Register("list_test", func(config any, l *slog.Logger) (Decorator, error) {
		return &mockDecorator{}, nil
	})

	names := List()
	if len(names) == 0 {
		t.Fatal("List() should return registered decorators")
	}

	// Check that our registered decorator is in the list
	found := false
	for _, name := range names {
		if name == "list_test" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("List() should contain 'list_test' decorator")
	}
}

