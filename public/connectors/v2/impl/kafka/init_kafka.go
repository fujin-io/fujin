package kafka

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/connectors/util"
	v2 "github.com/fujin-io/fujin/public/connectors/v2"
)

func init() {
	fmt.Println("INIT")
	v2.RegisterReaderFactory("kafka",
		func(config any, name string, autoCommit bool, l *slog.Logger) (v2.ReadCloser, error) {
			var typedConfig Config
			if parsedConfig, ok := config.(Config); ok {
				typedConfig = parsedConfig
			} else {
				if err := util.ConvertConfig(config, &typedConfig); err != nil {
					return nil, fmt.Errorf("kafka reader factory: failed to convert config: %w", err)
				}
			}
			if err := typedConfig.Validate(); err != nil {
				return nil, fmt.Errorf("kafka reader factory: invalid config: %w", err)
			}

			clientConf, ok := typedConfig.Clients[name]
			if !ok {
				return nil, fmt.Errorf("kafka: client not found by name: %s", name)
			}

			return NewConnector(ConnectorConfig{
				CommonSettings:         typedConfig.Common,
				ClientSpecificSettings: clientConf,
			}, autoCommit, l)
		})

	v2.RegisterWriterFactory("kafka",
		func(config any, name string, l *slog.Logger) (v2.WriteCloser, error) {
			var typedConfig Config
			if parsedConfig, ok := config.(Config); ok {
				typedConfig = parsedConfig
			} else {
				if err := util.ConvertConfig(config, &typedConfig); err != nil {
					return nil, fmt.Errorf("kafka writer factory: failed to convert config: %w", err)
				}
			}
			if err := typedConfig.Validate(); err != nil {
				return nil, fmt.Errorf("kafka writer factory: invalid config: %w", err)
			}

			clientConf, ok := typedConfig.Clients[name]
			if !ok {
				return nil, fmt.Errorf("kafka: client not found by name: %s", name)
			}

			return NewConnector(ConnectorConfig{
				CommonSettings:         typedConfig.Common,
				ClientSpecificSettings: clientConf,
			}, false, l)
		})

	v2.RegisterConfigValueConverter("kafka", convertConfigValue)
}
