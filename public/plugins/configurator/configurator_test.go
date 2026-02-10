package configurator

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
)

// mockConfigurator is a test configurator implementation
type mockConfigurator struct {
	loadCalled bool
	loadError  error
	config     any
}

func (m *mockConfigurator) Load(ctx context.Context, cfg any) error {
	m.loadCalled = true
	m.config = cfg
	return m.loadError
}

func TestRegister(t *testing.T) {
	// Register a test loader
	err := Register("test_loader", func(l *slog.Logger) (Configurator, error) {
		return &mockConfigurator{}, nil
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// Try to register again - should fail
	err = Register("test_loader", func(l *slog.Logger) (Configurator, error) {
		return &mockConfigurator{}, nil
	})
	if err == nil {
		t.Fatal("Register() should have failed for duplicate name")
	}
}

func TestGet(t *testing.T) {
	// Register a loader for this test
	_ = Register("get_test_loader", func(l *slog.Logger) (Configurator, error) {
		return &mockConfigurator{}, nil
	})

	// Get existing loader factory
	factory, ok := Get("get_test_loader")
	if !ok {
		t.Fatal("Get() should find registered factory")
	}
	if factory == nil {
		t.Fatal("Get() returned nil factory")
	}

	// Get non-existent loader factory
	_, ok = Get("nonexistent_loader")
	if ok {
		t.Fatal("Get() should not find non-existent factory")
	}
}

func TestList(t *testing.T) {
	// Register a loader to ensure List() has something to return
	_ = Register("list_test_loader", func(l *slog.Logger) (Configurator, error) {
		return &mockConfigurator{}, nil
	})

	names := List()
	if len(names) == 0 {
		t.Fatal("List() should return registered loaders")
	}

	// Check that our registered loader is in the list
	found := false
	for _, name := range names {
		if name == "list_test_loader" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("List() should contain 'list_test_loader'")
	}
}

func TestLoader_Load(t *testing.T) {
	l := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Register a loader
	_ = Register("load_test_loader", func(l *slog.Logger) (Configurator, error) {
		return &mockConfigurator{}, nil
	})

	// Get factory and create loader
	factory, ok := Get("load_test_loader")
	if !ok {
		t.Fatal("Get() should find registered factory")
	}

	loader, err := factory(l)
	if err != nil {
		t.Fatalf("factory() error = %v", err)
	}
	if loader == nil {
		t.Fatal("factory() should return a loader")
	}

	// Test Load
	var testConfig struct {
		Test string
	}
	ctx := context.Background()
	err = loader.Load(ctx, &testConfig)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify mock was called
	mock, ok := loader.(*mockConfigurator)
	if !ok {
		t.Fatal("loader should be *mockLoader")
	}
	if !mock.loadCalled {
		t.Fatal("Load() should have been called")
	}
}

func TestLoader_Load_WithError(t *testing.T) {
	l := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Register a loader that returns error
	_ = Register("error_loader", func(l *slog.Logger) (Configurator, error) {
		return &mockConfigurator{
			loadError: errors.New("load error"),
		}, nil
	})

	// Get factory and create loader
	factory, ok := Get("error_loader")
	if !ok {
		t.Fatal("Get() should find registered factory")
	}

	loader, err := factory(l)
	if err != nil {
		t.Fatalf("factory() error = %v", err)
	}

	// Test Load with error
	var testConfig struct {
		Test string
	}
	ctx := context.Background()
	err = loader.Load(ctx, &testConfig)
	if err == nil {
		t.Fatal("Load() should return error when loader fails")
	}
}

func TestLoader_Factory_WithError(t *testing.T) {
	l := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Register a factory that returns error
	_ = Register("error_factory", func(l *slog.Logger) (Configurator, error) {
		return nil, errors.New("factory error")
	})

	// Get factory and try to create loader
	factory, ok := Get("error_factory")
	if !ok {
		t.Fatal("Get() should find registered factory")
	}

	_, err := factory(l)
	if err == nil {
		t.Fatal("factory() should return error when factory fails")
	}
}

func TestLoader_Load_WithNilConfig(t *testing.T) {
	l := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Register a loader
	_ = Register("nil_config_loader", func(l *slog.Logger) (Configurator, error) {
		return &mockConfigurator{}, nil
	})

	// Get factory and create loader
	factory, ok := Get("nil_config_loader")
	if !ok {
		t.Fatal("Get() should find registered factory")
	}

	loader, err := factory(l)
	if err != nil {
		t.Fatalf("factory() error = %v", err)
	}

	// Test Load with nil config (should still work, loader decides how to handle it)
	ctx := context.Background()
	err = loader.Load(ctx, nil)
	// Some loaders might accept nil, others might error - both are valid
	// We just check that it doesn't panic
	_ = err
}

func TestRegister_Concurrent(t *testing.T) {
	// Test concurrent registration (should be safe due to mutex)
	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			err := Register("concurrent_test_loader", func(l *slog.Logger) (Configurator, error) {
				return &mockConfigurator{}, nil
			})
			done <- err
		}(i)
	}

	// Collect results
	errors := 0
	successes := 0
	for i := 0; i < 10; i++ {
		err := <-done
		if err != nil {
			errors++
		} else {
			successes++
		}
	}

	// Only one should succeed, rest should fail with duplicate error
	if successes != 1 {
		t.Fatalf("Expected 1 success, got %d", successes)
	}
	if errors != 9 {
		t.Fatalf("Expected 9 errors, got %d", errors)
	}
}

func TestGet_Concurrent(t *testing.T) {
	// Register a loader
	_ = Register("concurrent_get_loader", func(l *slog.Logger) (Configurator, error) {
		return &mockConfigurator{}, nil
	})

	// Test concurrent Get calls (should be safe due to RWMutex)
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_, ok := Get("concurrent_get_loader")
			done <- ok
		}()
	}

	// All should succeed
	for i := 0; i < 10; i++ {
		ok := <-done
		if !ok {
			t.Fatal("Get() should succeed concurrently")
		}
	}
}

func TestList_Concurrent(t *testing.T) {
	// Register a loader
	_ = Register("concurrent_list_loader", func(l *slog.Logger) (Configurator, error) {
		return &mockConfigurator{}, nil
	})

	// Test concurrent List calls (should be safe due to RWMutex)
	done := make(chan []string, 10)
	for i := 0; i < 10; i++ {
		go func() {
			names := List()
			done <- names
		}()
	}

	// All should succeed and return same results
	var firstList []string
	for i := 0; i < 10; i++ {
		names := <-done
		if len(names) == 0 {
			t.Fatal("List() should return registered loaders")
		}
		if i == 0 {
			firstList = names
		} else {
			if len(names) != len(firstList) {
				t.Fatal("List() should return consistent results concurrently")
			}
		}
	}
}
