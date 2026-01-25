package reader

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/connectors/reader/config"
)

type ReaderType byte

const (
	Unknown ReaderType = iota
	Subscriber
	Consumer
)

type Reader interface {
	Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error
	HSubscribe(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error
	Fetch(
		ctx context.Context, n uint32,
		fetchResponseHandler func(n uint32, err error),
		msgHandler func(message []byte, topic string, args ...any),
	)
	HFetch(
		ctx context.Context, n uint32,
		fetchResponseHandler func(n uint32, err error),
		msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
	)
	Ack(
		ctx context.Context, msgIDs [][]byte,
		ackHandler func(error),
		ackMsgHandler func([]byte, error),
	)
	Nack(
		ctx context.Context, msgIDs [][]byte,
		nackHandler func(error),
		nackMsgHandler func([]byte, error),
	)
	MsgIDStaticArgsLen() int
	EncodeMsgID(buf []byte, topic string, args ...any) []byte
	IsAutoCommit() bool
	Close()
}

type ReaderFactoryFunc func(brokerSpecificConfig any, autoCommit bool, l *slog.Logger) (Reader, error)

// ConfigValueConverterFunc converts and validates a configuration value for a specific setting path
// Returns the converted value and any validation error
type ConfigValueConverterFunc func(settingPath string, value string) (any, error)

var (
	readerFactories             = make(map[string]ReaderFactoryFunc)
	readerConfigValueConverters = make(map[string]ConfigValueConverterFunc)
)

func RegisterReaderFactory(protocol string, factory ReaderFactoryFunc) {
	readerFactories[protocol] = factory
}

// RegisterConfigValueConverter registers a value converter for a protocol
func RegisterConfigValueConverter(protocol string, converter ConfigValueConverterFunc) {
	readerConfigValueConverters[protocol] = converter
}

// GetConfigValueConverter returns the value converter for a protocol, or nil if not registered
func GetConfigValueConverter(protocol string) ConfigValueConverterFunc {
	return readerConfigValueConverters[protocol]
}

func New(conf config.Config, autoCommit bool, l *slog.Logger) (Reader, error) {
	factory, ok := readerFactories[conf.Protocol]
	if !ok {
		return nil, fmt.Errorf("unsupported reader protocol: %s (is it compiled in?)", conf.Protocol)
	}

	return factory(conf.Settings, autoCommit, l)
}
