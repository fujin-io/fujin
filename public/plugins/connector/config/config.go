package config

import (
	decoratorconfig "github.com/fujin-io/fujin/public/plugins/decorator/config"
)

// ConnectorsConfig maps connector names to their configurations
type ConnectorsConfig map[string]ConnectorConfig

// ConnectorConfig represents the configuration for a single connector
type ConnectorConfig struct {
	Protocol    string                 `yaml:"protocol"`
	Overridable []string               `yaml:"overridable,omitempty"` // Whitelist of paths that clients can override
	Decorators  []decoratorconfig.Config `yaml:"decorators,omitempty"`  // Decorators to apply to this connector
	Settings    any                    `yaml:"settings"`
}

