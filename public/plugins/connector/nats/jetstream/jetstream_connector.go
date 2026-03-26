package jetstream

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// jetStreamConnector implements connector.Connector for NATS JetStream.
type jetStreamConnector struct {
	config Config
	l      *slog.Logger
}

// newJetStreamConnector creates a new NATS JetStream connector instance.
func newJetStreamConnector(config any, l *slog.Logger) (connector.Connector, error) {
	if config == nil {
		return &jetStreamConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("nats_jetstream connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("nats_jetstream connector: invalid config: %w", err)
	}

	return &jetStreamConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

func (c *jetStreamConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := c.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("nats_jetstream: client not found by name: %s", name)
	}

	return NewReader(ConnectorConfig{
		CommonSettings:         c.config.Common,
		ClientSpecificSettings: clientConf,
	}, autoCommit, l)
}

func (c *jetStreamConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := c.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("nats_jetstream: client not found by name: %s", name)
	}

	return NewWriter(ConnectorConfig{
		CommonSettings:         c.config.Common,
		ClientSpecificSettings: clientConf,
	}, l)
}

func (c *jetStreamConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
