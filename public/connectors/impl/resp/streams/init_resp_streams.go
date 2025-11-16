//go:build resp_streams

package streams

import (
	"fmt"
	"log/slog"

	// Добавим, если понадобится для перезаписи Endpoint

	redis "github.com/fujin-io/fujin/public/connectors/impl/resp"
	"github.com/fujin-io/fujin/public/connectors/reader"
	"github.com/fujin-io/fujin/public/connectors/util"
	"github.com/fujin-io/fujin/public/connectors/writer"
)

func init() {
	writer.RegisterWriterFactory("resp_streams", func(rawBrokerConfig any, l *slog.Logger) (writer.Writer, error) {
		var typedConfig WriterConfig
		if writerConfig, ok := rawBrokerConfig.(WriterConfig); ok {
			typedConfig = writerConfig
		} else {
			if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
				return nil, fmt.Errorf("resp_streams writer factory: failed to convert config: %w", err)
			}
		}
		if err := typedConfig.Validate(); err != nil {
			return nil, fmt.Errorf("resp_streams writer factory: failed to convert config: %w", err)
		}
		return NewWriter(typedConfig, l)
	},
		redis.ParseConfigEndpoint,
	)

	reader.RegisterReaderFactory("resp_streams", func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if readerConfig, ok := rawBrokerConfig.(ReaderConfig); ok {
			typedConfig = readerConfig
		} else {
			if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
				return nil, fmt.Errorf("resp_streams reader factory: failed to convert config: %w", err)
			}
		}
		if err := typedConfig.Validate(); err != nil {
			return nil, fmt.Errorf("resp_streams reader factory: invalid config: %w", err)
		}
		return NewReader(typedConfig, autoCommit, l)
	})
}
