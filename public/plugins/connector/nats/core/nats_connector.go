package core

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// natsConnector implements connector.Connector interface for NATS Core
type natsConnector struct {
	config Config
	l      *slog.Logger
}

// NewNATSConnector creates a new NATS Core connector instance
func NewNATSConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &natsConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("nats_core connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("nats_core connector: invalid config: %w", err)
	}

	return &natsConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (n *natsConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := n.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("nats_core: client not found by name: %s", name)
	}

	return NewReader(ConnectorConfig{
		CommonSettings:         n.config.Common,
		ClientSpecificSettings: clientConf,
	}, autoCommit, l)
}

// NewWriter creates a writer from configuration
func (n *natsConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := n.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("nats_core: client not found by name: %s", name)
	}

	return NewWriter(ConnectorConfig{
		CommonSettings:         n.config.Common,
		ClientSpecificSettings: clientConf,
	}, l)
}

// GetConfigValueConverter returns the config value converter for NATS Core
func (n *natsConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
