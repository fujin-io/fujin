//go:build amqp091

package amqp091

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/connectors/reader"
	"github.com/fujin-io/fujin/public/connectors/util"
	"github.com/fujin-io/fujin/public/connectors/writer"
)

func init() {
	writer.RegisterWriterFactory("amqp091",
		func(rawBrokerConfig any, l *slog.Logger) (writer.Writer, error) {
			var typedConfig WriterConfig
			if writerConfig, ok := rawBrokerConfig.(WriterConfig); ok {
				typedConfig = writerConfig
			} else {
				if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
					return nil, fmt.Errorf("amqp091 writer factory: failed to convert config: %w", err)
				}
			}
			if err := typedConfig.Validate(); err != nil {
				return nil, fmt.Errorf("amqp091 writer factory: invalid config: %w", err)
			}

			return NewWriter(typedConfig, l)
		},
		func(conf map[string]any) string {
			connConf, ok := conf["conn"].(map[string]any)
			if !ok {
				return ""
			}
			url, ok := connConf["url"].(string)
			if !ok {
				return ""
			}

			return url
		},
	)

	reader.RegisterReaderFactory("amqp091", func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if readerConfig, ok := rawBrokerConfig.(ReaderConfig); ok {
			typedConfig = readerConfig
		} else {
			if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
				return nil, fmt.Errorf("amqp091 reader factory: failed to convert config: %w", err)
			}
		}
		if err := typedConfig.Validate(); err != nil {
			return nil, fmt.Errorf("amqp091 reader factory: invalid config: %w", err)
		}
		return NewReader(typedConfig, autoCommit, l)
	})
	writer.RegisterConfigValueConverter("amqp091", convertWriterConfigValue)
	reader.RegisterConfigValueConverter("amqp091", convertReaderConfigValue)
}
