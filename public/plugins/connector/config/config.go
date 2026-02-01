package config

import (
	bmwconfig "github.com/fujin-io/fujin/public/plugins/middleware/bind/config"
	cmwconfig "github.com/fujin-io/fujin/public/plugins/middleware/connector/config"
)

// ConnectorsConfig maps connector names to their configurations
type ConnectorsConfig map[string]ConnectorConfig

// ConnectorConfig represents the configuration for a single connector
type ConnectorConfig struct {
	Protocol             string             `yaml:"protocol"`
	Overridable          []string           `yaml:"overridable,omitempty"`           // Whitelist of paths that clients can override
	BindMiddlewares      []bmwconfig.Config `yaml:"bind_middlewares,omitempty"`     // Middlewares to apply to BIND requests
	ConnectorMiddlewares []cmwconfig.Config `yaml:"connector_middlewares,omitempty"` // Middlewares to apply to this connector
	Settings             any                `yaml:"settings"`
}
