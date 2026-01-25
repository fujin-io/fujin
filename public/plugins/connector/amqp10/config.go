package amqp10

import (
	"fmt"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/fujin-io/fujin/public/connectors/cerr"
)

type ConnConfig struct {
	Addr         string         `yaml:"addr"`
	ContainerID  string         `yaml:"container_id"`
	HostName     string         `yaml:"host_name"`
	IdleTimeout  time.Duration  `yaml:"idle_timeout"`
	MaxFrameSize uint32         `yaml:"max_frame_size"`
	MaxSessions  uint16         `yaml:"max_sessions"`
	Properties   map[string]any `yaml:"properties"`
	WriteTimeout time.Duration  `yaml:"write_timeout"`
}

type SessionConfig struct {
	MaxLinks uint32 `yaml:"max_links"`
}

type SenderConfig struct {
	Target                      string                   `yaml:"target"`
	Capabilities                []string                 `yaml:"capabilities"`
	Durability                  amqp.Durability          `yaml:"durability"`
	DynamicAddress              bool                     `yaml:"dynamic_address"`
	DesiredCapabilities         []string                 `yaml:"desired_capabilities"`
	ExpiryPolicy                amqp.ExpiryPolicy        `yaml:"expiry_policy"`
	ExpiryTimeout               uint32                   `yaml:"expiry_timeout"`
	Name                        string                   `yaml:"name"`
	Properties                  map[string]any           `yaml:"properties"`
	RequestedReceiverSettleMode *amqp.ReceiverSettleMode `yaml:"requested_receiver_settle_mode"`
	SettlementMode              *amqp.SenderSettleMode   `yaml:"settlement_mode"`
	SourceAddress               string                   `yaml:"source_address"`
	TargetCapabilities          []string                 `yaml:"target_capabilities"`
	TargetDurability            amqp.Durability          `yaml:"target_durability"`
	TargetExpiryPolicy          amqp.ExpiryPolicy        `yaml:"target_expiry_policy"`
	TargetExpiryTimeout         uint32                   `yaml:"target_expiry_timeout"`
}

type ReceiverConfig struct {
	Source                    string                 `yaml:"source"`
	Credit                    int32                  `yaml:"credit"`
	Durability                amqp.Durability        `yaml:"durability"`
	DynamicAddress            bool                   `yaml:"dynamic_address"`
	ExpiryPolicy              amqp.ExpiryPolicy      `yaml:"expiry_policy"`
	ExpiryTimeout             uint32                 `yaml:"expiry_timeout"`
	Filters                   []amqp.LinkFilter      `yaml:"filters"`
	Name                      string                 `yaml:"name"`
	Properties                map[string]any         `yaml:"properties"`
	RequestedSenderSettleMode *amqp.SenderSettleMode `yaml:"requested_sender_settle_mode"`
}

type SendConfig struct {
	Settled bool `yaml:"settled"`
}

// CommonSettings contains settings shared across all clients
type CommonSettings struct {
	// Common connection settings can be added here if needed
	// For now, connection settings are per-client
}

// ClientSpecificSettings contains settings specific to a client
// A client can be either a reader or a writer
type ClientSpecificSettings struct {
	// Connection settings
	Conn ConnConfig `yaml:"conn"`
	
	// Session settings
	Session SessionConfig `yaml:"session"`
	
	// Writer-specific settings (optional, only for writers)
	Sender *SenderConfig `yaml:"sender,omitempty"`
	Send   *SendConfig   `yaml:"send,omitempty"`
	
	// Reader-specific settings (optional, only for readers)
	Receiver *ReceiverConfig `yaml:"receiver,omitempty"`
}

// Config is the top-level configuration structure
type Config struct {
	Common  CommonSettings                    `yaml:"common"`
	Clients map[string]ClientSpecificSettings `yaml:"clients"`
}

// ConnectorConfig combines common and client-specific settings
type ConnectorConfig struct {
	CommonSettings
	ClientSpecificSettings
}

func NewConnectorConfig(common CommonSettings, client ClientSpecificSettings) ConnectorConfig {
	return ConnectorConfig{
		CommonSettings:         common,
		ClientSpecificSettings: client,
	}
}

func (c *Config) Validate() error {
	if len(c.Clients) == 0 {
		return cerr.ValidationErr("at least one client must be defined")
	}

	for name, client := range c.Clients {
		if client.Conn.Addr == "" {
			return cerr.ValidationErr(fmt.Sprintf("client %q: addr is not defined", name))
		}
		
		// Validate writer config
		if client.Sender != nil {
			if client.Sender.Target == "" {
				return cerr.ValidationErr(fmt.Sprintf("client %q: sender.target is not defined", name))
			}
		}
		
		// Validate reader config
		if client.Receiver != nil {
			if client.Receiver.Source == "" {
				return cerr.ValidationErr(fmt.Sprintf("client %q: receiver.source is not defined", name))
			}
		}
		
		// Client must have either sender (writer) or receiver (reader)
		if client.Sender == nil && client.Receiver == nil {
			return cerr.ValidationErr(fmt.Sprintf("client %q: must have either sender (writer) or receiver (reader) configured", name))
		}
	}

	return nil
}

func (c *ConnectorConfig) Endpoint() string {
	return c.Conn.Addr
}

