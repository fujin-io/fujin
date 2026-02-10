package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	pconfig "github.com/fujin-io/fujin/public/config"
	"github.com/fujin-io/fujin/public/plugins/configurator"
	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	bmw "github.com/fujin-io/fujin/public/plugins/middleware/bind"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
	"github.com/fujin-io/fujin/public/server"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	"github.com/quic-go/quic-go"
)

var (
	ErrNilConfig = errors.New("nil config")
)

type Config struct {
	Fujin      FujinConfig                      `yaml:"fujin"`
	GRPC       GRPCConfig                       `yaml:"grpc"`
	Connectors connectorconfig.ConnectorsConfig `yaml:"connectors"`
}

type FujinConfig struct {
	Enabled               bool              `yaml:"enabled"`
	Addr                  string            `yaml:"addr"`
	WriteDeadline         time.Duration     `yaml:"write_deadline"`
	ForceTerminateTimeout time.Duration     `yaml:"force_terminate_timeout"`
	PingInterval          time.Duration     `yaml:"ping_interval"`
	PingTimeout           time.Duration     `yaml:"ping_timeout"`
	PingStream            bool              `yaml:"ping_stream"`
	PingMaxRetries        int               `yaml:"ping_max_retries"`
	TLS                   pconfig.TLSConfig `yaml:"tls"`
	QUIC                  QUICConfig        `yaml:"quic"`
}

type GRPCConfig struct {
	Enabled               bool                  `yaml:"enabled"`
	Addr                  string                `yaml:"addr"`
	ConnectionTimeout     time.Duration         `yaml:"connection_timeout"`
	MaxConcurrentStreams  uint32                `yaml:"max_concurrent_streams"`
	MaxRecvMsgSize        int                   `yaml:"max_recv_msg_size"`
	MaxSendMsgSize        int                   `yaml:"max_send_msg_size"`
	InitialWindowSize     int32                 `yaml:"initial_window_size"`
	InitialConnWindowSize int32                 `yaml:"initial_conn_window_size"`
	ServerKeepAlive       ServerKeepAliveConfig `yaml:"server_keepalive"`
	ClientKeepAlive       ClientKeepAliveConfig `yaml:"client_keepalive"`
	TLS                   pconfig.TLSConfig     `yaml:"tls"`
	ObservabilityEnabled  bool                  `yaml:"observability_enabled"`
}

type ServerKeepAliveConfig struct {
	Time                  time.Duration `yaml:"time"`
	Timeout               time.Duration `yaml:"timeout"`
	MaxConnectionIdle     time.Duration `yaml:"max_connection_idle"`
	MaxConnectionAge      time.Duration `yaml:"max_connection_age"`
	MaxConnectionAgeGrace time.Duration `yaml:"max_connection_age_grace"`
}

type ClientKeepAliveConfig struct {
	MinTime             time.Duration `yaml:"min_time"`
	PermitWithoutStream bool          `yaml:"permit_without_stream"`
}

type QUICConfig struct {
	MaxIncomingStreams   int64         `yaml:"max_incoming_streams"`
	KeepAlivePeriod      time.Duration `yaml:"keepalive_period"`
	HandshakeIdleTimeout time.Duration `yaml:"handshake_idle_timeout"`
	MaxIdleTimeout       time.Duration `yaml:"max_idle_timeout"`
}

func (c *Config) parse() (serverconfig.Config, error) {
	var (
		fujinConf serverconfig.FujinServerConfig
		grpcConf  serverconfig.GRPCServerConfig
		err       error
	)

	fujinConf, err = c.parseFujinServerConfig()
	if err != nil {
		return serverconfig.Config{}, fmt.Errorf("parse fujin server config: %w", err)
	}

	grpcConf, err = c.parseGRPCConfig()
	if err != nil {
		return serverconfig.Config{}, fmt.Errorf("parse grpc server config: %w", err)
	}

	// TODO: Validate connectors config
	// if err := c.Connectors.Validate(); err != nil {
	// 	return serverconfig.Config{}, fmt.Errorf("validate connectors config: %w", err)
	// }

	return serverconfig.Config{
		Fujin:      fujinConf,
		GRPC:       grpcConf,
		Connectors: c.Connectors,
	}, nil
}

func (c *Config) parseFujinServerConfig() (serverconfig.FujinServerConfig, error) {
	if c == nil {
		return serverconfig.FujinServerConfig{}, ErrNilConfig
	}

	if !c.Fujin.Enabled {
		return serverconfig.FujinServerConfig{
			Enabled: c.Fujin.Enabled,
		}, nil
	}

	err := c.Fujin.TLS.Parse()
	if err != nil {
		return serverconfig.FujinServerConfig{}, fmt.Errorf("parse tls conf: %w", err)
	}

	return serverconfig.FujinServerConfig{
		Enabled:               c.Fujin.Enabled,
		Addr:                  c.Fujin.Addr,
		WriteDeadline:         c.Fujin.WriteDeadline,
		ForceTerminateTimeout: c.Fujin.ForceTerminateTimeout,
		PingInterval:          c.Fujin.PingInterval,
		PingTimeout:           c.Fujin.PingTimeout,
		PingStream:            c.Fujin.PingStream,
		PingMaxRetries:        c.Fujin.PingMaxRetries,
		TLS:                   c.Fujin.TLS.Config,
		QUIC:                  c.Fujin.QUIC.parse(),
	}, nil
}

func (c *Config) parseGRPCConfig() (serverconfig.GRPCServerConfig, error) {
	if c == nil {
		return serverconfig.GRPCServerConfig{}, ErrNilConfig
	}

	if !c.GRPC.Enabled {
		return serverconfig.GRPCServerConfig{
			Enabled: c.GRPC.Enabled,
		}, nil
	}

	err := c.GRPC.TLS.Parse()
	if err != nil {
		return serverconfig.GRPCServerConfig{}, fmt.Errorf("parse tls conf: %w", err)
	}

	return serverconfig.GRPCServerConfig{
		Enabled:               c.GRPC.Enabled,
		Addr:                  c.GRPC.Addr,
		ConnectionTimeout:     c.GRPC.ConnectionTimeout,
		MaxConcurrentStreams:  c.GRPC.MaxConcurrentStreams,
		MaxRecvMsgSize:        c.GRPC.MaxRecvMsgSize,
		MaxSendMsgSize:        c.GRPC.MaxSendMsgSize,
		InitialWindowSize:     c.GRPC.InitialWindowSize,
		InitialConnWindowSize: c.GRPC.InitialConnWindowSize,
		ServerKeepAlive:       c.GRPC.ServerKeepAlive.parse(),
		ClientKeepAlive:       c.GRPC.ClientKeepAlive.parse(),
		TLS:                   c.GRPC.TLS.Config,
		ObservabilityEnabled:  c.GRPC.ObservabilityEnabled,
	}, nil
}

func (c *ServerKeepAliveConfig) parse() serverconfig.ServerKeepAliveConfig {
	return serverconfig.ServerKeepAliveConfig{
		Time:                  c.Time,
		Timeout:               c.Timeout,
		MaxConnectionIdle:     c.MaxConnectionIdle,
		MaxConnectionAge:      c.MaxConnectionAge,
		MaxConnectionAgeGrace: c.MaxConnectionAgeGrace,
	}
}

func (c *ClientKeepAliveConfig) parse() serverconfig.ClientKeepAliveConfig {
	return serverconfig.ClientKeepAliveConfig{
		MinTime:             c.MinTime,
		PermitWithoutStream: c.PermitWithoutStream,
	}
}

func (c *QUICConfig) parse() *quic.Config {
	return &quic.Config{
		MaxIncomingStreams:   c.MaxIncomingStreams,
		KeepAlivePeriod:      c.KeepAlivePeriod,
		HandshakeIdleTimeout: c.HandshakeIdleTimeout,
		MaxIdleTimeout:       c.MaxIdleTimeout,
	}
}

var (
	Version string
	conf    Config
)

func RunCLI(ctx context.Context) {
	log.Printf("version: %s", Version)

	if err := loadConfig(&conf); err != nil {
		log.Fatal(err)
	}
	serverConf, err := conf.parse()
	if err != nil {
		log.Fatal(err)
	}

	logLevel := os.Getenv("FUJIN_LOG_LEVEL")
	logType := os.Getenv("FUJIN_LOG_TYPE")
	logger := configureLogger(logLevel, logType)

	logRegisteredPlugins(logger)

	s, err := server.NewServer(serverConf, logger)
	if err != nil {
		logger.Error("new server", "err", err)
		os.Exit(1)
	}

	if err := s.ListenAndServe(ctx); err != nil {
		logger.Error("listen and serve", "err", err)
	}
}

func configureLogger(logLevel, logType string) *slog.Logger {
	var parsedLogLevel slog.Level
	switch strings.ToUpper(logLevel) {
	case "DEBUG":
		parsedLogLevel = slog.LevelDebug
	case "WARN":
		parsedLogLevel = slog.LevelWarn
	case "ERROR":
		parsedLogLevel = slog.LevelError
	default:
		parsedLogLevel = slog.LevelInfo
	}

	var handler slog.Handler
	switch strings.ToLower(logType) {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: parsedLogLevel,
		})
	default:
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: parsedLogLevel,
		})
	}

	return slog.New(handler)
}

// loadConfig loads configuration
func loadConfig(cfg *Config) error {
	ctx := context.Background()

	if loaderType := os.Getenv("FUJIN_CONFIGURATOR"); loaderType != "" {
		if err := loadConfigWithLoader(ctx, loaderType, cfg); err != nil {
			return fmt.Errorf("load config from env: loader type %s: %w", loaderType, err)
		}
		log.Printf("loaded config using configurator from environment: %s\n", loaderType)
		return nil
	}

	return errors.New("failed to load config: configurator not specified")
}

// loadConfigWithLoader loads configuration using a configurator plugin.
// The loader directly fills the provided config struct.
func loadConfigWithLoader(ctx context.Context, loaderType string, cfg *Config) error {
	factory, ok := configurator.Get(loaderType)
	if !ok {
		return fmt.Errorf("configurator %q not found (available: %v)", loaderType, configurator.List())
	}

	// Create a temporary logger for config loading
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	loader, err := factory(logger)
	if err != nil {
		return fmt.Errorf("create configurator: %w", err)
	}

	if err := loader.Load(ctx, cfg); err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	return nil
}

// logRegisteredPlugins logs all registered connectors, connector middlewares, bind middlewares, and configurators
func logRegisteredPlugins(l *slog.Logger) {
	connectors := connector.List()
	cmws := cmw.List()
	bmws := bmw.List()
	configurators := configurator.List()

	sort.Strings(connectors)
	sort.Strings(cmws)
	sort.Strings(bmws)
	sort.Strings(configurators)

	if len(connectors) > 0 {
		l.Info("registered connectors", "list", strings.Join(connectors, ", "))
	} else {
		l.Warn("no connectors registered")
	}

	if len(cmws) > 0 {
		l.Info("registered connector middlewares", "list", strings.Join(cmws, ", "))
	} else {
		l.Warn("no connector middlewares registered")
	}

	if len(bmws) > 0 {
		l.Info("registered bind middlewares", "list", strings.Join(bmws, ", "))
	} else {
		l.Warn("no bind middlewares registered")
	}

	if len(configurators) > 0 {
		l.Info("registered configurators", "list", strings.Join(configurators, ", "))
	} else {
		l.Warn("no configurators registered")
	}
}
