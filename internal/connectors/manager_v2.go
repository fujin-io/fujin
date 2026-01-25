package connectors

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/internal/common/pool"
	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/plugins/decorator"
)

type ManagerV2 struct {
	conf          connectorconfig.ConnectorConfig
	connectorName string
	cpools        map[string]*pool.Pool // a map of client pools grouped by client name
	cmu           sync.RWMutex
	l             *slog.Logger
}

func NewManagerV2(conf connectorconfig.ConnectorConfig, connectorName string, l *slog.Logger) *ManagerV2 {
	cman := &ManagerV2{
		conf:          conf,
		connectorName: connectorName,
		cpools:        make(map[string]*pool.Pool),

		l: l,
	}

	return cman
}

func (m *ManagerV2) GetReader(name string, autoCommit bool) (connector.ReadCloser, error) {
	r, err := connector.NewReader(m.conf, name, autoCommit, m.l)
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}

	// Apply decorators
	if len(m.conf.Decorators) > 0 {
		// Decorators are already in the correct type (decoratorconfig.Config)
		wrapped, err := decorator.ChainReader(r, m.connectorName, m.conf.Decorators, m.l)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("apply decorators: %w", err)
		}
		return &decoratedReadCloser{Reader: wrapped, closer: r}, nil
	}

	return r, nil
}

// decoratedReadCloser wraps a decorated reader with the original closer
type decoratedReadCloser struct {
	connector.Reader
	closer connector.ReadCloser
}

func (d *decoratedReadCloser) Close() error {
	return d.closer.Close()
}

func (m *ManagerV2) GetWriter(name string) (connector.WriteCloser, error) {
	m.cmu.RLock()
	p, ok := m.cpools[name]
	m.cmu.RUnlock()

	if !ok {
		m.cmu.Lock()
		defer m.cmu.Unlock()

		// Double-check after acquiring write lock
		if p, ok = m.cpools[name]; ok {
			w, err := p.Get()
			if err != nil {
				return nil, fmt.Errorf("get writer: %w", err)
			}
			return w.(connector.WriteCloser), nil
		}

		p = pool.NewPool(func() (any, error) {
			w, err := connector.NewWriter(m.conf, name, m.l)
			if err != nil {
				return nil, err
			}

			// Apply decorators
			if len(m.conf.Decorators) > 0 {
				// Decorators are already in the correct type (decoratorconfig.Config)
				wrapped, err := decorator.ChainWriter(w, m.connectorName, m.conf.Decorators, m.l)
				if err != nil {
					w.Close()
					return nil, fmt.Errorf("apply decorators: %w", err)
				}
				return &decoratedWriteCloser{Writer: wrapped, closer: w}, nil
			}

			return w, nil
		})
		m.cpools[name] = p
	}

	w, err := p.Get()
	if err != nil {
		return nil, fmt.Errorf("get writer: %w", err)
	}

	return w.(connector.WriteCloser), nil
}

// decoratedWriteCloser wraps a decorated writer with the original closer
type decoratedWriteCloser struct {
	connector.Writer
	closer connector.WriteCloser
}

func (d *decoratedWriteCloser) Close() error {
	return d.closer.Close()
}

func (m *ManagerV2) PutWriter(c connector.WriteCloser, name string) {
	m.cmu.RLock()
	p, ok := m.cpools[name]
	m.cmu.RUnlock()

	if !ok {
		// Pool doesn't exist, just close the writer
		c.Close()
		return
	}

	p.Put(c)
}

func (m *ManagerV2) Close() {
	for _, p := range m.cpools {
		p.Close()
	}
}
