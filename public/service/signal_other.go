//go:build !unix

package service

import (
	"log/slog"
	"os"
	"syscall"

	"github.com/fujin-io/fujin/public/server"
)

// shutdownSignals are the OS signals that trigger graceful shutdown.
// On Windows: SIGINT, SIGTERM only (no SIGQUIT).
var shutdownSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}

// startReloadLoop is a no-op on non-Unix platforms (no SIGHUP).
func startReloadLoop(_ *server.Server, _ *slog.LevelVar, _ *slog.Logger) {}
