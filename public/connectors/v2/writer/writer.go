package writer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/connectors/writer/config"
)

var (
	writerFactories                = make(map[string]WriterFactoryFunc)
	writerConfigEndpointParseFuncs = make(map[string]WriterConfigEndpointParseFunc)
	writerConfigValueConverters    = make(map[string]ConfigValueConverterFunc)

	DefaultConfigEndpointParser = func(conf map[string]any) string {
		return ""
	}
)

// ConfigValueConverterFunc converts and validates a configuration value for a specific setting path
// Returns the converted value and any validation error
type ConfigValueConverterFunc func(settingPath string, value string) (any, error)

type Writer interface {
	Produce(ctx context.Context, msg []byte, callback func(err error))
	HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error))
	Flush(ctx context.Context) error
	BeginTx(ctx context.Context) error
	CommitTx(ctx context.Context) error
	RollbackTx(ctx context.Context) error
}

type WriterFactoryFunc func(brokerSpecificConfig any, l *slog.Logger) (Writer, error)

type WriterConfigEndpointParseFunc func(conf map[string]any) string

func RegisterWriterFactory(protocol string, factory WriterFactoryFunc, parser WriterConfigEndpointParseFunc) {
	writerFactories[protocol] = factory
	writerConfigEndpointParseFuncs[protocol] = parser
}

// RegisterConfigValueConverter registers a value converter for a writer protocol
func RegisterConfigValueConverter(protocol string, converter ConfigValueConverterFunc) {
	writerConfigValueConverters[protocol] = converter
}

// GetConfigValueConverter returns the value converter for a protocol, or nil if not registered
func GetConfigValueConverter(protocol string) ConfigValueConverterFunc {
	return writerConfigValueConverters[protocol]
}

func NewWriter(conf config.Config, l *slog.Logger) (Writer, error) {
	factory, ok := writerFactories[conf.Protocol]
	if !ok {
		return nil, fmt.Errorf("unsupported writer protocol: %s (is it compiled in?)", conf.Protocol)
	}

	return factory(conf.Settings, l)
}

func ParseEndpoint(conf config.Config) (string, error) {
	parser, ok := writerConfigEndpointParseFuncs[conf.Protocol]
	if !ok {
		return "", fmt.Errorf("writer config endpoint parser not found for protocol: %s (is it compiled in?)", conf.Protocol)
	}

	return parser(conf.Settings.(map[string]any)), nil
}
