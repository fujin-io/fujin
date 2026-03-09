package paho

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// mqttPahoConnector implements connector.Connector interface
type mqttPahoConnector struct {
	config Config
	l      *slog.Logger
}

// newMQTTPahoConnector creates a new MQTT connector instance
func newMQTTPahoConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &mqttPahoConnector{
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

	return &mqttPahoConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (m *mqttPahoConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
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
func (m *mqttPahoConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
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
func (m *mqttPahoConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
