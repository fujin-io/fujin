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
	"github.com/fujin-io/fujin/public/plugins/transport"
	"github.com/fujin-io/fujin/public/server"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
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
	Transports []transport.Config `yaml:"transports"`
}

type GRPCConfig struct {
	Enabled               *bool                 `yaml:"enabled,omitempty"` // nil = true (default)
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
	ObservabilityEnabled  *bool                 `yaml:"observability_enabled,omitempty"` // nil = false (default)
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

func (c *Config) parse() (serverconfig.Config, error) {
	grpcConf, err := c.parseGRPCConfig()
	if err != nil {
		return serverconfig.Config{}, fmt.Errorf("parse grpc server config: %w", err)
	}

	return serverconfig.Config{
		Transports: c.Fujin.Transports,
		GRPC:       grpcConf,
		Connectors: c.Connectors,
	}, nil
}

func (c *Config) parseGRPCConfig() (serverconfig.GRPCServerConfig, error) {
	if c == nil {
		return serverconfig.GRPCServerConfig{}, ErrNilConfig
	}

	grpcEnabled := c.GRPC.Enabled == nil || *c.GRPC.Enabled
	if !grpcEnabled {
		return serverconfig.GRPCServerConfig{
			Enabled: false,
		}, nil
	}

	err := c.GRPC.TLS.Parse()
	if err != nil {
		return serverconfig.GRPCServerConfig{}, fmt.Errorf("parse tls conf: %w", err)
	}

	obsEnabled := c.GRPC.ObservabilityEnabled != nil && *c.GRPC.ObservabilityEnabled
	return serverconfig.GRPCServerConfig{
		Enabled:               grpcEnabled,
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
		ObservabilityEnabled:  obsEnabled,
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
	transports := transport.List()
	connectors := connector.List()
	cmws := cmw.List()
	bmws := bmw.List()
	configurators := configurator.List()

	sort.Strings(transports)
	sort.Strings(connectors)
	sort.Strings(cmws)
	sort.Strings(bmws)
	sort.Strings(configurators)

	if len(transports) > 0 {
		l.Info("registered transports", "list", strings.Join(transports, ", "))
	} else {
		l.Warn("no transports registered")
	}

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
