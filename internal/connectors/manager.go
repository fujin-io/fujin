package connectors

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/internal/common/pool"
	"github.com/fujin-io/fujin/internal/connectors/observability"
	"github.com/fujin-io/fujin/public/connectors"
	"github.com/fujin-io/fujin/public/connectors/reader"
	"github.com/fujin-io/fujin/public/connectors/writer"
)

var (
	ErrReaderNotFound = errors.New("reader not found")
	ErrWriterNotFound = errors.New("writer not found")
)

type Manager struct {
	conf connectors.Config

	readers map[string]reader.Reader
	wpools  map[string]*pool.Pool // a map of writer pools grouped by writer name

	pmu sync.RWMutex

	l *slog.Logger
}

func NewManager(conf connectors.Config, l *slog.Logger) *Manager {
	cman := &Manager{
		conf: conf,

		readers: make(map[string]reader.Reader, len(conf.Readers)),
		wpools:  make(map[string]*pool.Pool, len(conf.Writers)),

		l: l,
	}

	return cman
}

func (m *Manager) GetReader(name string, autoCommit bool) (reader.Reader, error) {
	conf, ok := m.conf.Readers[name]
	if !ok {
		return nil, ErrReaderNotFound
	}

	r, err := reader.New(conf, autoCommit, m.l)
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}

	r = observability.OtelReaderWrapper(observability.MetricsReaderWrapper(r, name), name)
	return r, nil
}

func (m *Manager) GetWriter(name string) (writer.Writer, error) {
	m.pmu.RLock()
	p, ok := m.wpools[name]
	m.pmu.RUnlock()

	if !ok {
		m.pmu.Lock()
		defer m.pmu.Unlock()

		// Double-check after acquiring write lock
		if p, ok = m.wpools[name]; ok {
			w, err := p.Get()
			if err != nil {
				return nil, fmt.Errorf("get writer: %w", err)
			}
			return w.(writer.Writer), nil
		}

		conf, ok := m.conf.Writers[name]
		if !ok {
			return nil, ErrWriterNotFound
		}

		p = pool.NewPool(func() (any, error) {
			w, err := writer.NewWriter(conf, m.l)
			if err != nil {
				return nil, err
			}
			w = observability.OtelWriterWrapper(observability.MetricsWriterWrapper(w, name), name)
			return w, nil
		})
		m.wpools[name] = p
	}

	w, err := p.Get()
	if err != nil {
		return nil, fmt.Errorf("get writer: %w", err)
	}

	return w.(writer.Writer), nil
}

func (m *Manager) PutWriter(w writer.Writer, name string) {
	m.pmu.RLock()
	p, ok := m.wpools[name]
	m.pmu.RUnlock()

	if !ok {
		// Pool doesn't exist, just close the writer
		w.Close()
		return
	}

	p.Put(w)
}

func (m *Manager) Close() {
	for _, p := range m.wpools {
		p.Close()
	}
}

func (m *Manager) WriterMatchEndpoint(w writer.Writer, pub string) bool {
	writerConf, ok := m.conf.Writers[pub]
	if !ok {
		return false
	}
	endpoint, err := writer.ParseEndpoint(writerConf)
	if err != nil {
		m.l.Error("parse endpoint", "err", err)
		return false
	}

	return endpoint == w.Endpoint()
}
