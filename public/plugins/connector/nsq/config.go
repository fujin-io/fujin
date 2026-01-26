package nsq

import (
	"fmt"
	"time"
)

// PoolConfig contains pool configuration for writers
type PoolConfig struct {
	Size           int           `yaml:"size"`
	PreAlloc       bool          `yaml:"pre_alloc"`
	ReleaseTimeout time.Duration `yaml:"release_timeout"`
}

// CommonSettings contains settings shared across all NSQ clients
type CommonSettings struct {
	// For writers: single nsqd address
	Address string `yaml:"address,omitempty"`
	// For readers: multiple nsqd addresses or lookupd addresses
	Addresses        []string `yaml:"addresses,omitempty"`
	LookupdAddresses []string `yaml:"lookupd_addresses,omitempty"`
}

// ClientSpecificSettings contains settings specific to a client
type ClientSpecificSettings struct {
	Topic       string     `yaml:"topic"`
	Channel     string     `yaml:"channel,omitempty"`     // Only for readers
	MaxInFlight int        `yaml:"max_in_flight,omitempty"` // Only for readers
	Pool        PoolConfig `yaml:"pool,omitempty"`        // Only for writers
}

// Config is the top-level configuration structure for NSQ connector
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

// Validate validates the NSQ configuration
func (c *Config) Validate() error {
	// At least one connection method must be specified
	if c.Common.Address == "" && len(c.Common.Addresses) == 0 && len(c.Common.LookupdAddresses) == 0 {
		return fmt.Errorf("nsq: at least one of address, addresses, or lookupd_addresses is required")
	}
	if len(c.Clients) == 0 {
		return fmt.Errorf("nsq: at least one client must be configured")
	}
	for name, client := range c.Clients {
		if client.Topic == "" {
			return fmt.Errorf("nsq: client %q: topic is required", name)
		}
	}
	return nil
}

// Endpoint returns the NSQ address
func (c *ConnectorConfig) Endpoint() string {
	if c.Address != "" {
		return c.Address
	}
	if len(c.Addresses) > 0 {
		return c.Addresses[0]
	}
	if len(c.LookupdAddresses) > 0 {
		return c.LookupdAddresses[0]
	}
	return ""
}

// applyDefaults sets default values for optional fields
func (c *ConnectorConfig) applyDefaults() {
	if c.MaxInFlight == 0 {
		c.MaxInFlight = 1
	}
	if c.Pool.Size == 0 {
		c.Pool.Size = 1000
	}
	if c.Pool.ReleaseTimeout == 0 {
		c.Pool.ReleaseTimeout = 5 * time.Second
	}
}

// ValidateWriter validates writer-specific settings
func (c *ConnectorConfig) ValidateWriter() error {
	c.applyDefaults()
	if c.Address == "" {
		return fmt.Errorf("nsq: address is required for writer")
	}
	return nil
}

// ValidateReader validates reader-specific settings  
func (c *ConnectorConfig) ValidateReader() error {
	c.applyDefaults()
	if len(c.Addresses) == 0 && len(c.LookupdAddresses) == 0 {
		return fmt.Errorf("nsq: addresses or lookupd_addresses is required for reader")
	}
	if c.Channel == "" {
		return fmt.Errorf("nsq: channel is required for reader")
	}
	return nil
}

