package franz

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// kafkaFranzConnector implements connector.Connector interface using the [Franz Kafka client library](https://github.com/twmb/franz-go)
type kafkaFranzConnector struct {
	config Config
	l      *slog.Logger
}

// newKafkaFranzConnector creates a new kafka franz-go connector instance
func newKafkaFranzConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &kafkaFranzConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("kafka_franz connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("kafka_franz connector: invalid config: %w", err)
	}

	return &kafkaFranzConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (k *kafkaFranzConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := k.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("kafka_franz: client not found by name: %s", name)
	}

	return NewConnector(ConnectorConfig{
		CommonSettings:         k.config.Common,
		ClientSpecificSettings: clientConf,
	}, autoCommit, l)
}

// NewWriter creates a writer from configuration
func (k *kafkaFranzConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := k.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("kafka_franz: client not found by name: %s", name)
	}

	return NewConnector(ConnectorConfig{
		CommonSettings:         k.config.Common,
		ClientSpecificSettings: clientConf,
	}, false, l)
}

// GetConfigValueConverter returns the config value converter for Kafka
func (k *kafkaFranzConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
