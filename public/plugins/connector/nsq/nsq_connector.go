package nsq

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// nsqConnector implements connector.Connector interface for NSQ
type nsqConnector struct {
	config Config
	l      *slog.Logger
}

// newNSQConnector creates a new NSQ connector instance
func newNSQConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &nsqConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("nsq connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("nsq connector: invalid config: %w", err)
	}

	return &nsqConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (n *nsqConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := n.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("nsq: client not found by name: %s", name)
	}

	connConf := ConnectorConfig{
		CommonSettings:         n.config.Common,
		ClientSpecificSettings: clientConf,
	}

	if err := connConf.ValidateReader(); err != nil {
		return nil, err
	}

	return NewReader(connConf, autoCommit, l)
}

// NewWriter creates a writer from configuration
func (n *nsqConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := n.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("nsq: client not found by name: %s", name)
	}

	connConf := ConnectorConfig{
		CommonSettings:         n.config.Common,
		ClientSpecificSettings: clientConf,
	}

	if err := connConf.ValidateWriter(); err != nil {
		return nil, err
	}

	return NewWriter(connConf, l)
}

// GetConfigValueConverter returns the config value converter for NSQ
func (n *nsqConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
