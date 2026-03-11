package zmq4

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// zmqConnector implements connector.Connector for ZeroMQ
type zmqConnector struct {
	config Config
	l      *slog.Logger
}

// newZmqConnector creates a new zeromq_zmq4 connector instance
func newZmqConnector(config any, l *slog.Logger) (connector.Connector, error) {
	if config == nil {
		return &zmqConnector{config: Config{}, l: l}, nil
	}

	var typedConfig Config
	if parsed, ok := config.(Config); ok {
		typedConfig = parsed
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("zeromq_zmq4: convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("zeromq_zmq4: %w", err)
	}

	return &zmqConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (z *zmqConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	clientConf, ok := z.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("zeromq_zmq4: client not found: %s", name)
	}

	_, _, err := z.config.Common.SubscribeEndpoint()
	if err != nil {
		return nil, fmt.Errorf("zeromq_zmq4: %w", err)
	}

	connConf := ConnectorConfig{
		CommonSettings:         z.config.Common,
		ClientSpecificSettings: clientConf,
	}

	return NewReader(context.Background(), connConf, autoCommit, l)
}

// NewWriter creates a writer from configuration
func (z *zmqConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	clientConf, ok := z.config.Clients[name]
	if !ok {
		return nil, fmt.Errorf("zeromq_zmq4: client not found: %s", name)
	}

	_, _, err := z.config.Common.PublishEndpoint()
	if err != nil {
		return nil, fmt.Errorf("zeromq_zmq4: %w", err)
	}

	connConf := ConnectorConfig{
		CommonSettings:         z.config.Common,
		ClientSpecificSettings: clientConf,
	}

	return NewWriter(context.Background(), connConf, name, l)
}

// GetConfigValueConverter returns nil (no runtime overrides)
func (z *zmqConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return nil
}
