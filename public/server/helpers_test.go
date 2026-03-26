package server

import (
	"log/slog"
	"os"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/server/config"
)

func testConfig(cc connectorconfig.ConnectorsConfig) config.Config {
	return config.Config{
		Connectors: cc,
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}
