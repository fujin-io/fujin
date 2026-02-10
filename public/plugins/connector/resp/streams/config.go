package streams

import (
	"fmt"
	"time"

	respconfig "github.com/fujin-io/fujin/public/plugins/connector/resp/config"
)

type Marshaller string

const (
	JSON Marshaller = "json"
)

type StreamConf struct {
	StartID       string `yaml:"start_id"`
	GroupCreateID string `yaml:"group_create_id"`
}

type GroupConf struct {
	Name     string `yaml:"name"`
	Consumer string `yaml:"consumer"`
}

// CommonSettings contains settings shared across all Redis Streams clients.
type CommonSettings struct {
	respconfig.RedisConfig       `yaml:",inline"`
	respconfig.WriterBatchConfig `yaml:",inline"`
}

// ClientSpecificSettings contains settings specific to a client.
type ClientSpecificSettings struct {
	// Reader settings
	Streams map[string]StreamConf `yaml:"streams,omitempty"`
	Block   time.Duration         `yaml:"block,omitempty"`
	Count   int64                 `yaml:"count,omitempty"`
	Group   GroupConf             `yaml:"group,omitempty"`

	// Writer settings
	Stream string `yaml:"stream,omitempty"`

	// Common settings
	Marshaller Marshaller `yaml:"marshaller,omitempty"`
}

// Config is the top-level configuration for RESP Streams connector.
type Config struct {
	Common  CommonSettings                    `yaml:"common"`
	Clients map[string]ClientSpecificSettings `yaml:"clients"`
}

// ConnectorConfig combines common and client-specific settings for a single client.
type ConnectorConfig struct {
	CommonSettings
	ClientSpecificSettings
}

// Validate validates the Redis Streams configuration.
func (c *Config) Validate() error {
	if err := c.Common.RedisConfig.Validate(); err != nil {
		return fmt.Errorf("resp_streams: %w", err)
	}
	if len(c.Clients) == 0 {
		return fmt.Errorf("resp_streams: at least one client must be configured")
	}
	return nil
}

// Endpoint returns the Redis endpoint.
func (c *ConnectorConfig) Endpoint() string {
	return c.RedisConfig.Endpoint()
}

// ValidateWriter validates writer-specific settings.
func (c *ConnectorConfig) ValidateWriter() error {
	c.WriterBatchConfig.ApplyBatchDefaults()
	if err := c.WriterBatchConfig.ValidateBatch(); err != nil {
		return err
	}
	if c.Stream == "" {
		return fmt.Errorf("resp_streams: stream is required for writer")
	}
	return nil
}

// ValidateReader validates reader-specific settings.
func (c *ConnectorConfig) ValidateReader() error {
	if len(c.Streams) == 0 {
		return fmt.Errorf("resp_streams: at least one stream is required for reader")
	}
	return nil
}
