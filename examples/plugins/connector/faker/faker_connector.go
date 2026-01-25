package faker

import (
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

// fakerConnector implements connector.Connector interface
type fakerConnector struct {
	l *slog.Logger
}

// NewFakerConnector creates a new Faker connector instance
func NewFakerConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Faker connector doesn't need any configuration
	return &fakerConnector{
		l: l,
	}, nil
}

// NewReader creates a reader from configuration
func (f *fakerConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	return NewReader(autoCommit, l)
}

// NewWriter creates a writer from configuration
func (f *fakerConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	return NewWriter(l)
}

// GetConfigValueConverter returns nil as faker doesn't support config overrides
func (f *fakerConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return nil
}

