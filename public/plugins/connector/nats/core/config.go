package core

import (
	"fmt"
)

// CommonSettings contains settings shared across all NATS clients
type CommonSettings struct {
	URL string `yaml:"url"`
}

// ClientSpecificSettings contains settings specific to a client
type ClientSpecificSettings struct {
	Subject string `yaml:"subject"`
}

// Config is the top-level configuration structure for NATS Core connector
type Config struct {
	Common  CommonSettings                    `yaml:"common"`
	Clients map[string]ClientSpecificSettings `yaml:"clients"`
}

// ConnectorConfig combines common and client-specific settings for a single client
type ConnectorConfig struct {
	CommonSettings
	ClientSpecificSettings
}

// NewConnectorConfig creates a new ConnectorConfig from common and client-specific settings
func NewConnectorConfig(common CommonSettings, client ClientSpecificSettings) ConnectorConfig {
	return ConnectorConfig{
		CommonSettings:         common,
		ClientSpecificSettings: client,
	}
}

// Validate validates the NATS Core configuration
func (c *Config) Validate() error {
	if c.Common.URL == "" {
		return fmt.Errorf("nats_core: url is required")
	}
	if len(c.Clients) == 0 {
		return fmt.Errorf("nats_core: at least one client must be configured")
	}
	for name, client := range c.Clients {
		if client.Subject == "" {
			return fmt.Errorf("nats_core: client %q: subject is required", name)
		}
	}
	return nil
}

// Endpoint returns the NATS URL
func (c *ConnectorConfig) Endpoint() string {
	return c.URL
}

