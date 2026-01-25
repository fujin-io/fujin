// Package decorator provides a plugin system for connector decorators.
// Decorators wrap readers and writers to add cross-cutting functionality
// like observability, rate limiting, retries, etc.
package decorator

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/decorator/config"
)

// Decorator wraps readers and writers with additional functionality.
type Decorator interface {
	// WrapWriter wraps a writer with additional functionality.
	WrapWriter(w connector.Writer, connectorName string) connector.Writer
	// WrapReader wraps a reader with additional functionality.
	WrapReader(r connector.Reader, connectorName string) connector.Reader
}

// Factory creates a decorator from configuration.
// config is the decorator-specific configuration (can be nil).
type Factory func(config any, l *slog.Logger) (Decorator, error)

var (
	factories = make(map[string]Factory)
	mu        sync.RWMutex
)

// Register registers a decorator factory with the given name.
// This is typically called from init() in decorator implementations.
func Register(name string, factory Factory) error {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := factories[name]; exists {
		return fmt.Errorf("decorator %q already registered", name)
	}

	factories[name] = factory
	return nil
}

// Get returns a decorator factory by name.
func Get(name string) (Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()

	factory, ok := factories[name]
	return factory, ok
}

// List returns all registered decorator names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(factories))
	for name := range factories {
		names = append(names, name)
	}
	return names
}

// Chain applies a chain of decorators to a writer and reader.
func Chain(
	w connector.Writer,
	r connector.Reader,
	connectorName string,
	configs []config.Config,
	l *slog.Logger,
) (connector.Writer, connector.Reader, error) {
	for _, cfg := range configs {
		factory, ok := Get(cfg.Name)
		if !ok {
			return nil, nil, fmt.Errorf("decorator %q not found (is it compiled in?)", cfg.Name)
		}

		dec, err := factory(cfg.Config, l)
		if err != nil {
			return nil, nil, fmt.Errorf("create decorator %q: %w", cfg.Name, err)
		}

		if w != nil {
			w = dec.WrapWriter(w, connectorName)
		}
		if r != nil {
			r = dec.WrapReader(r, connectorName)
		}
	}

	return w, r, nil
}

// ChainWriter applies a chain of decorators to a writer only.
func ChainWriter(
	w connector.Writer,
	connectorName string,
	configs []config.Config,
	l *slog.Logger,
) (connector.Writer, error) {
	result, _, err := Chain(w, nil, connectorName, configs, l)
	return result, err
}

// ChainReader applies a chain of decorators to a reader only.
func ChainReader(
	r connector.Reader,
	connectorName string,
	configs []config.Config,
	l *slog.Logger,
) (connector.Reader, error) {
	_, result, err := Chain(nil, r, connectorName, configs, l)
	return result, err
}
