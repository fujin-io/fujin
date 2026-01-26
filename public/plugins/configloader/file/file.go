package file

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/fujin-io/fujin/public/plugins/configloader"
	"github.com/fujin-io/fujin/public/util"
	"gopkg.in/yaml.v3"
)

// fileLoader implements configloader.ConfigLoader for file-based configuration.
type fileLoader struct {
	config Config
	l      *slog.Logger
}

// NewFileLoader creates a new file config loader instance.
func NewFileLoader(config any, l *slog.Logger) (configloader.ConfigLoader, error) {
	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("file config loader: failed to convert config: %w", err)
		}
	}

	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("file config loader: invalid config: %w", err)
	}

	return &fileLoader{
		config: typedConfig,
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
				return fmt.Errorf("file config loader: read file %q: %w", path, err)
			}

			// Try JSON first
			if err := json.Unmarshal(data, cfg); err == nil {
				return nil
			}

			// Fall back to YAML
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return fmt.Errorf("file config loader: failed to parse %q as JSON or YAML: %w", path, err)
			}

			return nil
		}
	}

	return fmt.Errorf("file config loader: failed to find config in paths: %v", f.config.Paths)
}
