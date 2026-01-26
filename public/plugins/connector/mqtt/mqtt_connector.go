package mqtt

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// mqttConnector implements connector.Connector interface
type mqttConnector struct {
	config Config
	l      *slog.Logger
}

// NewMQTTConnector creates a new MQTT connector instance
func NewMQTTConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &mqttConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("mqtt connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("mqtt connector: invalid config: %w", err)
	}

	return &mqttConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (m *mqttConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := m.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("mqtt: client not found by name: %s", name)
	}

	return NewReader(ConnectorConfig{
		CommonSettings:         m.config.Common,
		ClientSpecificSettings: clientConf,
	}, autoCommit, l)
}

// NewWriter creates a writer from configuration
func (m *mqttConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := m.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("mqtt: client not found by name: %s", name)
	}

	return NewWriter(ConnectorConfig{
		CommonSettings:         m.config.Common,
		ClientSpecificSettings: clientConf,
	}, l)
}

// GetConfigValueConverter returns the config value converter for MQTT
func (m *mqttConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
