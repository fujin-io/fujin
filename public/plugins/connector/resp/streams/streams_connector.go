package streams

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// streamsConnector implements connector.Connector interface for Redis Streams.
type streamsConnector struct {
	config Config
	l      *slog.Logger
}

// NewStreamsConnector creates a new Redis Streams connector instance.
func NewStreamsConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &streamsConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("resp_streams connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("resp_streams connector: invalid config: %w", err)
	}

	return &streamsConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration.
func (s *streamsConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := s.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("resp_streams: client not found by name: %s", name)
	}

	connConf := ConnectorConfig{
		CommonSettings:         s.config.Common,
		ClientSpecificSettings: clientConf,
	}

	if err := connConf.ValidateReader(); err != nil {
		return nil, err
	}

	return NewReader(connConf, autoCommit, l)
}

// NewWriter creates a writer from configuration.
func (s *streamsConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := s.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("resp_streams: client not found by name: %s", name)
	}

	connConf := ConnectorConfig{
		CommonSettings:         s.config.Common,
		ClientSpecificSettings: clientConf,
	}

	if err := connConf.ValidateWriter(); err != nil {
		return nil, err
	}

	return NewWriter(connConf, l)
}

// GetConfigValueConverter returns the config value converter for Redis Streams.
func (s *streamsConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
