package connectors

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/internal/common/pool"
	v2 "github.com/fujin-io/fujin/public/connectors/v2"
)

type ManagerV2 struct {
	conf   v2.ConnectorConfig
	cpools map[string]*pool.Pool // a map of client pools grouped by client name
	cmu    sync.RWMutex
	l      *slog.Logger
}

func NewManagerV2(conf v2.ConnectorConfig, l *slog.Logger) *ManagerV2 {
	cman := &ManagerV2{
		conf:   conf,
		cpools: make(map[string]*pool.Pool),

		l: l,
	}

	return cman
}

func (m *ManagerV2) GetReader(name string, autoCommit bool) (v2.ReadCloser, error) {
	r, err := v2.NewReader(m.conf, name, autoCommit, m.l)
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}

	return r, nil
}

func (m *ManagerV2) GetWriter(name string) (v2.WriteCloser, error) {
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
			return w.(v2.Client), nil
		}

		p = pool.NewPool(func() (any, error) {
			w, err := v2.NewWriter(m.conf, name, m.l)
			if err != nil {
				return nil, err
			}
			return w, nil
		})
		m.cpools[name] = p
	}

	w, err := p.Get()
	if err != nil {
		return nil, fmt.Errorf("get writer: %w", err)
	}

	return w.(v2.Client), nil
}

func (m *ManagerV2) PutWriter(c v2.WriteCloser, name string) {
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
