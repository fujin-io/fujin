package pubsub

import (
	"fmt"

	respconfig "github.com/fujin-io/fujin/public/plugins/connector/resp/config"
)

// CommonSettings contains settings shared across all Redis PubSub clients
type CommonSettings struct {
	respconfig.RedisConfig `yaml:",inline"`
	respconfig.WriterBatchConfig `yaml:",inline"`
}

// ClientSpecificSettings contains settings specific to a client
type ClientSpecificSettings struct {
	// For readers: multiple channels to subscribe
	Channels []string `yaml:"channels,omitempty"`
	// For writers: single channel to publish
	Channel string `yaml:"channel,omitempty"`
}

// Config is the top-level configuration structure for Redis PubSub connector
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

// Validate validates the Redis PubSub configuration
func (c *Config) Validate() error {
	if err := c.Common.RedisConfig.Validate(); err != nil {
		return fmt.Errorf("resp_pubsub: %w", err)
	}
	if len(c.Clients) == 0 {
		return fmt.Errorf("resp_pubsub: at least one client must be configured")
	}
	return nil
}

// Endpoint returns the Redis endpoint
func (c *ConnectorConfig) Endpoint() string {
	return c.RedisConfig.Endpoint()
}

// ValidateWriter validates writer-specific settings
func (c *ConnectorConfig) ValidateWriter() error {
	c.WriterBatchConfig.ApplyBatchDefaults()
	if err := c.WriterBatchConfig.ValidateBatch(); err != nil {
		return err
	}
	if c.Channel == "" {
		return fmt.Errorf("resp_pubsub: channel is required for writer")
	}
	return nil
}

// ValidateReader validates reader-specific settings
func (c *ConnectorConfig) ValidateReader() error {
	if len(c.Channels) == 0 {
		return fmt.Errorf("resp_pubsub: at least one channel is required for reader")
	}
	return nil
}

