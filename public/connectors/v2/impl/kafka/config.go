package kafka

import (
	"time"

	pconfig "github.com/fujin-io/fujin/public/config"
	"github.com/fujin-io/fujin/public/connectors/cerr"
)

type Balancer string

const (
	BalancerUnknown           Balancer = ""
	BalancerSticky            Balancer = "sticky"
	BalancerCooperativeSticky Balancer = "cooperative_sticky"
	BalancerRange             Balancer = "range"
	BalancerRoundRobin        Balancer = "round_robin"
)

type IsolationLevel string

const (
	IsolationLevelDefault        = ""
	IsolationLevelReadUncommited = "read_uncommited"
	IsolationLevelReadCommited   = "read_commited"
)

type CommonSettings struct {
	Brokers                []string          `yaml:"brokers"`
	AllowAutoTopicCreation bool              `yaml:"allow_auto_topic_creation"`
	PingTimeout            time.Duration     `yaml:"ping_timeout"`
	TLS                    pconfig.TLSConfig `yaml:"tls"`
}

type ClientSpecificSettings struct {
	// reader settings
	ConsumeTopics        []string       `yaml:"consume_topics"`
	Group                string         `yaml:"group"`
	MaxPollRecords       int            `yaml:"max_poll_records"`
	FetchIsolationLevel  IsolationLevel `yaml:"fetch_isolation_level"`
	AutoCommitInterval   time.Duration  `yaml:"auto_commit_interval"`
	AutoCommitMarks      bool           `yaml:"auto_commit_marks"`
	Balancers            []Balancer     `yaml:"balancers"`
	BlockRebalanceOnPoll bool           `yaml:"block_rebalance_on_poll"`
	// writer settings
	ProduceTopic           string        `yaml:"produce_topic"`
	Linger                 time.Duration `yaml:"linger"`
	MaxBufferedRecords     int           `yaml:"max_buffered_records"`
	DisableIdempotentWrite bool          `yaml:"disable_idempotent_write"`
	TransactionalID        string        `yaml:"transactional_id"` // Transactional ID for Kafka transactions
}

type Config struct {
	Common  CommonSettings                    `yaml:"common"`
	Clients map[string]ClientSpecificSettings `yaml:"clients"`
}

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
	if len(c.Common.Brokers) <= 0 {
		return cerr.ValidationErr("brokers not defined")
	}

	for _, c := range c.Clients {
		if len(c.ConsumeTopics) <= 0 && c.ProduceTopic == "" {
			return cerr.ValidationErr("consume topic or produce topic must be defined")
		}
	}

	return nil
}
