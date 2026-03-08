package zeromq

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// zeromqConnector implements connector.Connector interface for ZeroMQ.
type zeromqConnector struct {
	config Config
	l      *slog.Logger
}

// newZeroMQConnector creates a new ZeroMQ connector instance.
func newZeroMQConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only.
	if config == nil {
		return &zeromqConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("zeromq connector: failed to convert config: %w", err)
		}
	}

	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("zeromq connector: invalid config: %w", err)
	}

	return &zeromqConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration.
func (z *zeromqConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := z.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("zeromq: client not found by name: %s", name)
	}

	connConf := ConnectorConfig{
		CommonSettings:         z.config.Common,
		ClientSpecificSettings: clientConf,
	}

	if err := connConf.ValidateReader(); err != nil {
		return nil, err
	}

	return NewReader(connConf, autoCommit, l)
}

// NewWriter creates a writer from configuration.
func (z *zeromqConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := z.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("zeromq: client not found by name: %s", name)
	}

	connConf := ConnectorConfig{
		CommonSettings:         z.config.Common,
		ClientSpecificSettings: clientConf,
	}

	if err := connConf.ValidateWriter(); err != nil {
		return nil, err
	}

	return NewWriter(connConf, l)
}

// GetConfigValueConverter returns the config value converter for ZeroMQ.
// No special conversion is required at the moment.
func (z *zeromqConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return nil
}

