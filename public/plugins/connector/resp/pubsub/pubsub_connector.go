package pubsub

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// pubsubConnector implements connector.Connector interface for Redis PubSub
type pubsubConnector struct {
	config Config
	l      *slog.Logger
}

// newRESPPubSubConnector creates a new Redis PubSub connector instance
func newRESPPubSubConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &pubsubConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("resp_pubsub connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("resp_pubsub connector: invalid config: %w", err)
	}

	return &pubsubConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (p *pubsubConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := p.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("resp_pubsub: client not found by name: %s", name)
	}

	connConf := ConnectorConfig{
		CommonSettings:         p.config.Common,
		ClientSpecificSettings: clientConf,
	}

	if err := connConf.ValidateReader(); err != nil {
		return nil, err
	}

	return NewReader(connConf, autoCommit, l)
}

// NewWriter creates a writer from configuration
func (p *pubsubConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := p.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("resp_pubsub: client not found by name: %s", name)
	}

	connConf := ConnectorConfig{
		CommonSettings:         p.config.Common,
		ClientSpecificSettings: clientConf,
	}

	if err := connConf.ValidateWriter(); err != nil {
		return nil, err
	}

	return NewWriter(connConf, l)
}

// GetConfigValueConverter returns the config value converter for Redis PubSub
func (p *pubsubConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
