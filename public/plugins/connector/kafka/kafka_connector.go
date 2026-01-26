package kafka

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/connectors/util"
	"github.com/fujin-io/fujin/public/plugins/connector"
)

// kafkaConnector implements connector.Connector interface
type kafkaConnector struct {
	config Config
	l      *slog.Logger
}

// NewKafkaConnector creates a new Kafka connector instance
func NewKafkaConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &kafkaConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("kafka connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("kafka connector: invalid config: %w", err)
	}

	return &kafkaConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (k *kafkaConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := k.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("kafka: client not found by name: %s", name)
	}

	return NewConnector(ConnectorConfig{
		CommonSettings:         k.config.Common,
		ClientSpecificSettings: clientConf,
	}, autoCommit, l)
}

// NewWriter creates a writer from configuration
func (k *kafkaConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := k.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("kafka: client not found by name: %s", name)
	}

	return NewConnector(ConnectorConfig{
		CommonSettings:         k.config.Common,
		ClientSpecificSettings: clientConf,
	}, false, l)
}

// GetConfigValueConverter returns the config value converter for Kafka
func (k *kafkaConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
