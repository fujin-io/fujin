package amqp10

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/connectors/util"
	"github.com/fujin-io/fujin/public/plugins/connector"
)

// amqp10Connector implements connector.Connector interface
type amqp10Connector struct {
	config Config
	l      *slog.Logger
}

// NewAMQP10Connector creates a new AMQP10 connector instance
func NewAMQP10Connector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &amqp10Connector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("amqp10 connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("amqp10 connector: invalid config: %w", err)
	}

	return &amqp10Connector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (a *amqp10Connector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := a.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("amqp10: client not found by name: %s", name)
	}

	if clientConf.Receiver == nil {
		return nil, fmt.Errorf("amqp10: client %q is not configured as a reader (receiver not defined)", name)
	}

	return NewReader(ConnectorConfig{
		CommonSettings:         a.config.Common,
		ClientSpecificSettings: clientConf,
	}, autoCommit, l)
}

// NewWriter creates a writer from configuration
func (a *amqp10Connector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := a.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("amqp10: client not found by name: %s", name)
	}

	if clientConf.Sender == nil {
		return nil, fmt.Errorf("amqp10: client %q is not configured as a writer (sender not defined)", name)
	}

	return NewWriter(ConnectorConfig{
		CommonSettings:         a.config.Common,
		ClientSpecificSettings: clientConf,
	}, l)
}

// GetConfigValueConverter returns the config value converter for AMQP10
func (a *amqp10Connector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}

