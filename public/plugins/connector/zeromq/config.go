package zeromq

import "fmt"

// CommonSettings contains settings shared across all ZeroMQ clients.
type CommonSettings struct {
	// Endpoint is the ZeroMQ endpoint to connect to, e.g. "tcp://localhost:5555".
	Endpoint string `yaml:"endpoint"`

	// Pattern defines the messaging pattern.
	// Currently only "pubsub" is supported (PUB writer, SUB reader).
	Pattern string `yaml:"pattern"`
}

// ClientSpecificSettings contains settings specific to a client.
type ClientSpecificSettings struct {
	// Topics is the list of topics to subscribe to (reader).
	// If empty, the reader subscribes to all topics.
	Topics []string `yaml:"topics,omitempty"`

	// Topic is the topic to publish messages to (writer).
	// Required for writers.
	Topic string `yaml:"topic,omitempty"`
}

// Config is the top-level configuration structure for the ZeroMQ connector.
//
// Example:
//
//	common:
//	  endpoint: tcp://zeromq:5555
//	  pattern: pubsub
//	clients:
//	  pub:
//	    topic: events
//	  sub:
//	    topics: [events]
type Config struct {
	Common  CommonSettings                    `yaml:"common"`
	Clients map[string]ClientSpecificSettings `yaml:"clients"`
}

// ConnectorConfig combines common and client-specific settings for a single client.
type ConnectorConfig struct {
	CommonSettings
	ClientSpecificSettings
}

// Validate validates the ZeroMQ configuration.
func (c *Config) Validate() error {
	if c.Common.Endpoint == "" {
		return fmt.Errorf("zeromq: common.endpoint is required")
	}

	if c.Common.Pattern == "" {
		c.Common.Pattern = "pubsub"
	}

	if c.Common.Pattern != "pubsub" {
		return fmt.Errorf("zeromq: unsupported pattern %q (only \"pubsub\" is supported)", c.Common.Pattern)
	}

	if len(c.Clients) == 0 {
		return fmt.Errorf("zeromq: at least one client must be configured")
	}

	return nil
}

// ValidateWriter validates writer-specific settings.
func (c *ConnectorConfig) ValidateWriter() error {
	if c.Topic == "" {
		return fmt.Errorf("zeromq: topic is required for writer")
	}
	if c.Endpoint == "" {
		return fmt.Errorf("zeromq: endpoint is required for writer")
	}
	return nil
}

// ValidateReader validates reader-specific settings.
func (c *ConnectorConfig) ValidateReader() error {
	if c.Endpoint == "" {
		return fmt.Errorf("zeromq: endpoint is required for reader")
	}
	// Topics may be empty, which means "subscribe to all topics".
	return nil
}

