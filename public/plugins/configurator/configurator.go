// Package configurator provides a plugin system for configuration loaders.
// Config loaders load configuration from various sources (files, vault, etcd, etc.)
// instead of static YAML files.
package configurator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// Boot loads configuration from a source.
// Load is called once at application startup.
type Configurator interface {
	// Load loads and parses configuration from the source into the provided config struct.
	// The loader is responsible for determining the format (JSON, YAML, etc.)
	// and parsing it directly into the config struct.
	// cfg must be a pointer to the configuration struct.
	Load(ctx context.Context, cfg any) error
}

// Factory creates a configurator from configuration.
// config is the loader-specific configuration (can be nil).
type Factory func(l *slog.Logger) (Configurator, error)

var (
	factories = make(map[string]Factory)
	mu        sync.RWMutex
)

// Register registers a configurator factory with the given name.
// This is typically called from init() in loader implementations.
// Returns an error if the loader is already registered.
func Register(name string, factory Factory) error {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := factories[name]; exists {
		return fmt.Errorf("configurator %q already registered", name)
	}

	factories[name] = factory
	return nil
}

// Get returns a configurator factory by name.
func Get(name string) (Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()

	factory, ok := factories[name]
	return factory, ok
}

// List returns all registered configurator names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(factories))
	for name := range factories {
		names = append(names, name)
	}
	return names
}
