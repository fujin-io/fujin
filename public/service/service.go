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
	"github.com/fujin-io/fujin/public/plugins/configloader"
	_ "github.com/fujin-io/fujin/public/plugins/configloader/all"
	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/plugins/decorator"
	"github.com/fujin-io/fujin/public/server"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	"github.com/quic-go/quic-go"
	"gopkg.in/yaml.v3"
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
	ObservabilityEnabled  bool              `yaml:"observability_enabled"`
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
		ObservabilityEnabled:  c.Fujin.ObservabilityEnabled,
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

	// Parse command line arguments
	// Support: ./fujin --bootstrap=bootstrap.yaml
	var bootstrapPath string

	// Simple argument parsing (can be enhanced with flag package later)
	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		if strings.HasPrefix(arg, "--bootstrap=") {
			bootstrapPath = strings.TrimPrefix(arg, "--bootstrap=")
		} else if strings.HasPrefix(arg, "--") {
			log.Fatalf("unknown flag: %s", arg)
		} else {
			log.Fatalf("unexpected argument: %s (use --bootstrap=path to specify bootstrap config)", arg)
		}
	}

	// Check environment variable for bootstrap path (highest priority)
	if bootstrapPath == "" {
		bootstrapPath = os.Getenv("FUJIN_BOOTSTRAP_CONFIG")
	}

	if err := loadConfig(bootstrapPath, &conf); err != nil {
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

// BootstrapConfig represents the bootstrap configuration that contains
// only the config loader settings.
type BootstrapConfig struct {
	ConfigLoader struct {
		Type     string         `yaml:"type"`
		Settings map[string]any `yaml:",inline"`
	} `yaml:"config_loader"`
}

// loadConfig loads configuration using the priority system:
// 1. Environment variables (highest priority)
// 2. Bootstrap configuration file
// 3. File system fallback (default)
func loadConfig(bootstrapPath string, cfg *Config) error {
	ctx := context.Background()

	// Priority 1: Check environment variables
	if loaderType := os.Getenv("FUJIN_CONFIG_LOADER"); loaderType != "" {
		loaderConfig := parseLoaderConfigFromEnv(loaderType)
		if err := loadConfigWithLoader(ctx, loaderType, loaderConfig, cfg); err != nil {
			return fmt.Errorf("load config from env: loader type %s: %w", loaderType, err)
		}
		log.Printf("loaded config using config loader from environment: %s\n", loaderType)
		return nil
	}

	// Priority 2: Check bootstrap configuration
	// First, try explicitly specified path (from flag or env var)
	if bootstrapPath != "" {
		data, err := os.ReadFile(bootstrapPath)
		if err != nil {
			return fmt.Errorf("read bootstrap config from %q: %w", bootstrapPath, err)
		}
		var bootstrap BootstrapConfig
		if err := yaml.Unmarshal(data, &bootstrap); err != nil {
			return fmt.Errorf("unmarshal bootstrap config: %w", err)
		}
		if bootstrap.ConfigLoader.Type != "" {
			if err := loadConfigWithLoader(ctx, bootstrap.ConfigLoader.Type, bootstrap.ConfigLoader.Settings, cfg); err != nil {
				return fmt.Errorf("load config from bootstrap loader %q: %w", bootstrap.ConfigLoader.Type, err)
			}
			log.Printf("loaded config using config loader from bootstrap file: %s loader type: %s\n", bootstrapPath, bootstrap.ConfigLoader.Type)
			return nil
		}
	}

	// If no explicit bootstrap path, try default locations
	bootstrapPaths := []string{"./bootstrap.dev.yaml", "./config.bootstrap.yaml", "./bootstrap.yaml"}
	for _, p := range bootstrapPaths {
		data, err := os.ReadFile(p)
		if err == nil {
			var bootstrap BootstrapConfig
			if err := yaml.Unmarshal(data, &bootstrap); err == nil {
				if bootstrap.ConfigLoader.Type != "" {
					if err := loadConfigWithLoader(ctx, bootstrap.ConfigLoader.Type, bootstrap.ConfigLoader.Settings, cfg); err != nil {
						return fmt.Errorf("load config from bootstrap loader %q: %w", bootstrap.ConfigLoader.Type, err)
					}
					log.Printf("loaded config using config loader from bootstrap: %s (bootstrap: %s)\n", bootstrap.ConfigLoader.Type, p)
					return nil
				}
			}
		}
	}

	return errors.New("failed to load config: config loader not specified")
}

// loadConfigWithLoader loads configuration using a config loader plugin.
// The loader directly fills the provided config struct.
func loadConfigWithLoader(ctx context.Context, loaderType string, loaderConfig any, cfg *Config) error {
	factory, ok := configloader.Get(loaderType)
	if !ok {
		return fmt.Errorf("config loader %q not found (available: %v)", loaderType, configloader.List())
	}

	// Create a temporary logger for config loading
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	loader, err := factory(loaderConfig, logger)
	if err != nil {
		return fmt.Errorf("create config loader: %w", err)
	}

	if err := loader.Load(ctx, cfg); err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	return nil
}

// parseLoaderConfigFromEnv parses config loader configuration from environment variables.
// Currently supports only the "file" loader type.
func parseLoaderConfigFromEnv(loaderType string) any {
	switch loaderType {
	case "file":
		// Parse paths from environment variable
		pathsEnv := os.Getenv("FUJIN_CONFIG_LOADER_FILE_PATHS")
		if pathsEnv == "" {
			// Default paths if not specified
			return map[string]any{
				"paths": []string{"./config.yaml", "conf/config.yaml", "config/config.yaml"},
			}
		}
		// Split comma-separated paths
		paths := strings.Split(pathsEnv, ",")
		for i := range paths {
			paths[i] = strings.TrimSpace(paths[i])
		}
		return map[string]any{
			"paths": paths,
		}
	default:
		// For other loaders, return empty config
		// They should parse their own env vars
		return nil
	}
}

// logRegisteredPlugins logs all registered connectors, decorators, and config loaders
func logRegisteredPlugins(l *slog.Logger) {
	connectors := connector.List()
	decorators := decorator.List()
	configLoaders := configloader.List()

	sort.Strings(connectors)
	sort.Strings(decorators)
	sort.Strings(configLoaders)

	if len(connectors) > 0 {
		l.Info("registered connectors", "list", strings.Join(connectors, ", "))
	} else {
		l.Warn("no connectors registered")
	}

	if len(decorators) > 0 {
		l.Info("registered decorators", "list", strings.Join(decorators, ", "))
	} else {
		l.Warn("no decorators registered")
	}

	if len(configLoaders) > 0 {
		l.Info("registered config loaders", "list", strings.Join(configLoaders, ", "))
	}
}
