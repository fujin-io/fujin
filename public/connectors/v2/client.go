package v2

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/fujin-io/fujin/public/connectors/v2/reader"
	"github.com/fujin-io/fujin/public/connectors/v2/writer"
)

type Client interface {
	reader.Reader
	writer.Writer
	io.Closer
}

type ReadCloser interface {
	reader.Reader
	io.Closer
}

type WriteCloser interface {
	writer.Writer
	io.Closer
}

type ReaderFactoryFunc func(config any, name string, autoCommit bool, l *slog.Logger) (ReadCloser, error)
type WriterFactoryFunc func(config any, name string, l *slog.Logger) (WriteCloser, error)

// ConfigValueConverterFunc converts and validates a configuration value for a specific setting path
// Returns the converted value and any validation error
type ConfigValueConverterFunc func(settingPath string, value string) (any, error)

var (
	readerFactories             = make(map[string]ReaderFactoryFunc)
	writerFactories             = make(map[string]WriterFactoryFunc)
	readerConfigValueConverters = make(map[string]ConfigValueConverterFunc)
)

func RegisterReaderFactory(protocol string, factory ReaderFactoryFunc) {
	readerFactories[protocol] = factory
}

func RegisterWriterFactory(protocol string, factory WriterFactoryFunc) {
	writerFactories[protocol] = factory
}

// RegisterConfigValueConverter registers a value converter for a reader protocol
func RegisterConfigValueConverter(protocol string, converter ConfigValueConverterFunc) {
	readerConfigValueConverters[protocol] = converter
}

// GetConfigValueConverter returns the value converter for a protocol, or nil if not registered
func GetConfigValueConverter(protocol string) ConfigValueConverterFunc {
	return readerConfigValueConverters[protocol]
}

func NewWriter(conf ConnectorConfig, name string, l *slog.Logger) (WriteCloser, error) {
	factory, ok := writerFactories[conf.Protocol]
	if !ok {
		return nil, fmt.Errorf("unsupported client protocol: %s (is it compiled in?)", conf.Protocol)
	}

	return factory(conf.Settings, name, l)
}

func NewReader(
	conf ConnectorConfig, name string, autoCommit bool, l *slog.Logger,
) (ReadCloser, error) {
	factory, ok := readerFactories[conf.Protocol]
	if !ok {
		return nil, fmt.Errorf("unsupported client protocol: %s (is it compiled in?)", conf.Protocol)
	}

	return factory(conf.Settings, name, autoCommit, l)
}
