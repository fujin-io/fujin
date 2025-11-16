//go:build nsq

package nsq

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/connectors/reader"
	"github.com/fujin-io/fujin/public/connectors/util"
	"github.com/fujin-io/fujin/public/connectors/writer"
)

func init() {
	writer.RegisterWriterFactory("nsq",
		func(rawBrokerConfig any, l *slog.Logger) (writer.Writer, error) {
			var typedConfig WriterConfig
			if writerConfig, ok := rawBrokerConfig.(WriterConfig); ok {
				typedConfig = writerConfig
			} else {
				if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
					return nil, fmt.Errorf("nsq writer factory: failed to convert config: %w", err)
				}
			}
			return NewWriter(typedConfig, l)
		},
		writer.DefaultConfigEndpointParser,
	)

	reader.RegisterReaderFactory("nsq", func(rawBrokerConfig any, autoCommit bool, l *slog.Logger) (reader.Reader, error) {
		var typedConfig ReaderConfig
		if readerConfig, ok := rawBrokerConfig.(ReaderConfig); ok {
			typedConfig = readerConfig
		} else {
			if err := util.ConvertConfig(rawBrokerConfig, &typedConfig); err != nil {
				return nil, fmt.Errorf("nsq reader factory: failed to convert config: %w", err)
			}
		}
		return NewReader(typedConfig, autoCommit, l)
	})
}
