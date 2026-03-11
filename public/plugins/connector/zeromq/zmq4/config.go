package zmq4

import (
	"fmt"

	"github.com/fujin-io/fujin/public/util"
)

// CommonSettings contains settings shared across all ZMQ clients
type CommonSettings struct {
	// Publish: mutually exclusive. Bind = Fujin listens, external SUBs connect. Connect = Fujin connects to remote XSUB/proxy.
	PublishBind    string `yaml:"publish_bind"`
	PublishConnect string `yaml:"publish_connect"`
	// Subscribe: mutually exclusive. Bind = Fujin listens, external PUBs connect. Connect = Fujin connects to remote PUB.
	SubscribeBind    string `yaml:"subscribe_bind"`
	SubscribeConnect string `yaml:"subscribe_connect"`
}

// ClientSpecificSettings contains settings per client (topic)
type ClientSpecificSettings struct {
	// Writer: ZMQ topic frame (defaults to client name if empty)
	ProduceTopic string `yaml:"produce_topic"`
	// Reader: ZMQ subscription filters (prefix match). Empty = subscribe to all.
	ConsumeTopics []string `yaml:"consume_topics"`
}

// Config is the top-level configuration for zeromq_zmq4 connector
type Config struct {
	Common  CommonSettings                    `yaml:"common"`
	Clients map[string]ClientSpecificSettings `yaml:"clients"`
}

// ConnectorConfig combines common and client-specific settings
type ConnectorConfig struct {
	CommonSettings
	ClientSpecificSettings
}

// Validate validates the zeromq_zmq4 configuration
func (c *Config) Validate() error {
	hasPub := c.Common.PublishBind != "" || c.Common.PublishConnect != ""
	hasSub := c.Common.SubscribeBind != "" || c.Common.SubscribeConnect != ""
	if !hasPub && !hasSub {
		return fmt.Errorf("zeromq_zmq4: at least one of publish_bind, publish_connect, subscribe_bind, subscribe_connect is required")
	}
	if c.Common.PublishBind != "" && c.Common.PublishConnect != "" {
		return fmt.Errorf("zeromq_zmq4: publish_bind and publish_connect are mutually exclusive")
	}
	if c.Common.SubscribeBind != "" && c.Common.SubscribeConnect != "" {
		return fmt.Errorf("zeromq_zmq4: subscribe_bind and subscribe_connect are mutually exclusive")
	}
	if len(c.Clients) == 0 {
		return fmt.Errorf("zeromq_zmq4: at least one client must be configured")
	}
	return nil
}

// PublishEndpoint returns the publish endpoint and whether to bind (true) or connect (false)
func (c *CommonSettings) PublishEndpoint() (ep string, bind bool, err error) {
	if c.PublishBind != "" {
		return c.PublishBind, true, nil
	}
	if c.PublishConnect != "" {
		return c.PublishConnect, false, nil
	}
	return "", false, util.ValidationErr("no publish endpoint configured")
}

// SubscribeEndpoint returns the subscribe endpoint and whether to bind (true) or connect (false)
func (c *CommonSettings) SubscribeEndpoint() (ep string, bind bool, err error) {
	if c.SubscribeBind != "" {
		return c.SubscribeBind, true, nil
	}
	if c.SubscribeConnect != "" {
		return c.SubscribeConnect, false, nil
	}
	return "", false, util.ValidationErr("no subscribe endpoint configured")
}

// Topic returns the ZMQ topic for produce (client name or ProduceTopic)
func (c *ConnectorConfig) Topic() string {
	if c.ProduceTopic != "" {
		return c.ProduceTopic
	}
	return "" // caller uses client name
}
