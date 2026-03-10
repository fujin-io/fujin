package amqp09

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// rabbitmqAMQP09Connector implements connector.Connector interface
type rabbitmqAMQP09Connector struct {
	config Config
	l      *slog.Logger
}

// NewRabbitMQAMQP09Connector creates a new AMQP091 connector instance
func NewRabbitMQAMQP09Connector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &rabbitmqAMQP09Connector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("rabbitmq_amqp09 connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("rabbitmq_amqp09 connector: invalid config: %w", err)
	}

	return &rabbitmqAMQP09Connector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (a *rabbitmqAMQP09Connector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := a.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("rabbitmq_amqp09: client not found by name: %s", name)
	}

	if clientConf.Consume == nil {
		return nil, fmt.Errorf("rabbitmq_amqp09: client %q is not configured as a reader (consume not defined)", name)
	}

	return NewReader(ConnectorConfig{
		CommonSettings:         a.config.Common,
		ClientSpecificSettings: clientConf,
	}, autoCommit, l)
}

// NewWriter creates a writer from configuration
func (a *rabbitmqAMQP09Connector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := a.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("rabbitmq_amqp09: client not found by name: %s", name)
	}

	if clientConf.Publish == nil {
		return nil, fmt.Errorf("rabbitmq_amqp09: client %q is not configured as a writer (publish not defined)", name)
	}

	return NewWriter(ConnectorConfig{
		CommonSettings:         a.config.Common,
		ClientSpecificSettings: clientConf,
	}, l)
}

// GetConfigValueConverter returns the config value converter for AMQP091
func (a *rabbitmqAMQP09Connector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
