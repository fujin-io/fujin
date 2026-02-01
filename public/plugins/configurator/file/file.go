package file

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/fujin-io/fujin/public/plugins/configurator"
	"gopkg.in/yaml.v3"
)

// fileLoader implements configurator.Configurator for file-based configuration.
type fileLoader struct {
	config Config
	l      *slog.Logger
}

func init() {
	if err := configurator.Register("file", newFileLoader); err != nil {
		panic(fmt.Sprintf("register file configurator: %v", err))
	}
}

// newFileLoader creates a new file configurator instance.
func newFileLoader(l *slog.Logger) (configurator.Configurator, error) {
	var config Config
	// Parse paths from environment variable
	pathsEnv := os.Getenv("FUJIN_CONFIGURATOR_FILE_PATHS")
	if pathsEnv == "" {
		// Default paths if not specified
		config.Paths = []string{"./config.yaml", "conf/config.yaml", "config/config.yaml"}
	}
	// Split comma-separated paths
	paths := strings.Split(pathsEnv, ",")
	for i := range paths {
		paths[i] = strings.TrimSpace(paths[i])
	}
	config.Paths = paths

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("file configurator: invalid config: %w", err)
	}

	return &fileLoader{
		config: config,
		l:      l.With("config_loader", "file"),
	}, nil
}

// Load loads and parses configuration from the first existing file in the paths list.
// Automatically detects and parses JSON or YAML format directly into the config struct.
func (f *fileLoader) Load(ctx context.Context, cfg any) error {
	for _, path := range f.config.Paths {
		file, err := os.Open(path)
		if err == nil {
			f.l.Info("loading config from file", "path", path)
			data, err := io.ReadAll(file)
			file.Close()
			if err != nil {
				return fmt.Errorf("file configurator: read file %q: %w", path, err)
			}

			// Try JSON first
			if err := json.Unmarshal(data, cfg); err == nil {
				return nil
			}

			// Fall back to YAML
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return fmt.Errorf("file configurator: failed to parse %q as JSON or YAML: %w", path, err)
			}

			return nil
		}
	}

	return fmt.Errorf("file configurator: failed to find config in paths: %v", f.config.Paths)
}
