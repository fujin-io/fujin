package yaml

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

// yamlLoader implements configurator.Configurator for file-based configuration.
type yamlLoader struct {
	config Config
	l      *slog.Logger
}

func init() {
	if err := configurator.Register("yaml", newYAMLLoader); err != nil {
		panic(fmt.Sprintf("register yaml configurator: %v", err))
	}
}

// newYAMLLoader creates a new file configurator instance.
func newYAMLLoader(l *slog.Logger) (configurator.Configurator, error) {
	var config Config
	// Parse paths from environment variable
	pathsEnv := os.Getenv("FUJIN_CONFIGURATOR_YAML_PATHS")
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
		return nil, fmt.Errorf("yaml configurator: invalid config: %w", err)
	}

	return &yamlLoader{
		config: config,
		l:      l.With("config_loader", "yaml"),
	}, nil
}

// Load loads and parses configuration from the first existing file in the paths list.
// Automatically detects and parses JSON or YAML format directly into the config struct.
func (f *yamlLoader) Load(ctx context.Context, cfg any) error {
	for _, path := range f.config.Paths {
		file, err := os.Open(path)
		if err == nil {
			f.l.Info("loading config from yaml", "path", path)
			data, err := io.ReadAll(file)
			file.Close()
			if err != nil {
				return fmt.Errorf("yaml configurator: read file %q: %w", path, err)
			}

			// Try JSON first
			if err := json.Unmarshal(data, cfg); err == nil {
				return nil
			}

			// Fall back to YAML
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return fmt.Errorf("yaml configurator: failed to parse %q as JSON or YAML: %w", path, err)
			}

			return nil
		}
	}

	return fmt.Errorf("yaml configurator: failed to find config in paths: %v", f.config.Paths)
}
