package config

import (
	std_tls "crypto/tls"
	"strings"
	"time"

	"github.com/fujin-io/fujin/public/server/config/tls"
	"github.com/fujin-io/fujin/public/util"
)

// RedisConfig contains common Redis connection settings
type RedisConfig struct {
	InitAddress  []string             `yaml:"init_address"`
	Username     string               `yaml:"username"`
	Password     string               `yaml:"password"`
	DisableCache bool                 `yaml:"disable_cache"`
	TLS          *tls.ClientTLSConfig `yaml:"tls,omitempty"`
}

// Validate validates the Redis configuration
func (c RedisConfig) Validate() error {
	if len(c.InitAddress) == 0 {
		return util.ValidationErr("init_address is required")
	}

	if c.TLS != nil {
		if err := c.TLS.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// TLSConfig returns the TLS configuration
func (c RedisConfig) TLSConfig() (*std_tls.Config, error) {
	if c.TLS != nil {
		return c.TLS.Parse()
	}
	return nil, nil
}

// Endpoint returns the connection endpoint string
func (c RedisConfig) Endpoint() string {
	return strings.Join(c.InitAddress, ",")
}

// WriterBatchConfig contains batching configuration for writers
type WriterBatchConfig struct {
	BatchSize int           `yaml:"batch_size"`
	Linger    time.Duration `yaml:"linger"`
}

// ValidateBatch validates batch configuration
func (c WriterBatchConfig) ValidateBatch() error {
	if c.BatchSize <= 0 {
		return util.ValidationErr("batch_size must be greater than 0")
	}
	if c.Linger <= 0 {
		return util.ValidationErr("linger must be greater than 0")
	}
	return nil
}

// ApplyBatchDefaults sets default values for batch configuration
func (c *WriterBatchConfig) ApplyBatchDefaults() {
	if c.BatchSize == 0 {
		c.BatchSize = 100
	}
	if c.Linger == 0 {
		c.Linger = 10 * time.Millisecond
	}
}
