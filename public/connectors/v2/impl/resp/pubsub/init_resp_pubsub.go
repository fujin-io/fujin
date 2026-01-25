//go:build resp_pubsub

package pubsub

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/connectors/impl/resp"
	"github.com/fujin-io/fujin/public/connectors/reader"
	"github.com/fujin-io/fujin/public/connectors/util"
	"github.com/fujin-io/fujin/public/connectors/writer"
)

func init() {
	writer.RegisterWriterFactory("resp_pubsub", func(rawBrokerConfig any, l *slog.Logger) (writer.Writer, error) {
		var typedConfig WriterConfig
		if writerConfig, ok := rawBrokerConfig.(WriterConfig); ok {
			typedConfig = writerConfig
		} else {
			if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
				return nil, fmt.Errorf("resp_pubsub writer factory: failed to convert config: %w", err)
			}
		}
		if err := typedConfig.Validate(); err != nil {
			return nil, fmt.Errorf("resp_pubsub writer factory: failed to convert config: %w", err)
		}
		return NewWriter(typedConfig, l)
	},
		resp.ParseConfigEndpoint,
	)

	reader.RegisterReaderFactory("resp_pubsub", func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if readerConfig, ok := rawBrokerConfig.(ReaderConfig); ok {
			typedConfig = readerConfig
		} else {
			if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
				return nil, fmt.Errorf("resp_pubsub reader factory: failed to convert config: %w", err)
			}
		}
		if err := typedConfig.Validate(); err != nil {
			return nil, fmt.Errorf("resp_pubsub reader factory: invalid config: %w", err)
		}
		return NewReader(typedConfig, autoCommit, l)
	})
	writer.RegisterConfigValueConverter("resp_pubsub", convertWriterConfigValue)
	reader.RegisterConfigValueConverter("resp_pubsub", convertReaderConfigValue)
}
