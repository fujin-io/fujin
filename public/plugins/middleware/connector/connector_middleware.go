// Package connector provides a plugin system for connector middlewares.
// Connector middlewares wrap readers and writers to add cross-cutting functionality
// like observability, rate limiting, retries, etc.
package connector

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/middleware/connector/config"
)

// Middleware wraps readers and writers with additional functionality.
type Middleware interface {
	// WrapWriter wraps a writer with additional functionality.
	WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser
	// WrapReader wraps a reader with additional functionality.
	WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser
}

// Factory creates a connector middleware from configuration.
// config is the connector middleware-specific configuration (can be nil).
type Factory func(config any, l *slog.Logger) (Middleware, error)

var (
	factories = make(map[string]Factory)
	mu        sync.RWMutex
)

// Register registers a connector middleware factory with the given name.
// This is typically called from init() in connector middleware implementations.
func Register(name string, factory Factory) error {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := factories[name]; exists {
		return fmt.Errorf("connector middleware %q already registered", name)
	}

	factories[name] = factory
	return nil
}

// Get returns a connector middleware factory by name.
func Get(name string) (Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()

	factory, ok := factories[name]
	return factory, ok
}

// List returns all registered connector middleware names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(factories))
	for name := range factories {
		names = append(names, name)
	}
	return names
}

// Chain applies a chain of connector middlewares to a writer and reader.
func Chain(
	w connector.WriteCloser,
	r connector.ReadCloser,
	connectorName string,
	configs []config.Config,
	l *slog.Logger,
) (connector.WriteCloser, connector.ReadCloser, error) {
	for _, cfg := range configs {
		// Check if middleware is disabled
		if cfg.Disabled {
			continue
		}

		factory, ok := Get(cfg.Name)
		if !ok {
			return nil, nil, fmt.Errorf("connector middleware %q not found (is it compiled in?)", cfg.Name)
		}

		dec, err := factory(cfg.Config, l)
		if err != nil {
			return nil, nil, fmt.Errorf("create connector middleware %q: %w", cfg.Name, err)
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

// ChainWriter applies a chain of connector middlewares to a writer only.
func ChainWriter(
	w connector.WriteCloser,
	connectorName string,
	configs []config.Config,
	l *slog.Logger,
) (connector.WriteCloser, error) {
	result, _, err := Chain(w, nil, connectorName, configs, l)
	return result, err
}

// ChainReader applies a chain of connector middlewares to a reader only.
func ChainReader(
	r connector.ReadCloser,
	connectorName string,
	configs []config.Config,
	l *slog.Logger,
) (connector.ReadCloser, error) {
	_, result, err := Chain(nil, r, connectorName, configs, l)
	return result, err
}
