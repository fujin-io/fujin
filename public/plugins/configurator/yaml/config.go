package yaml

import "fmt"

// Config is the configuration for the yaml configurator.
type Config struct {
	// Paths is a list of file paths to try in order.
	// The first existing file will be used.
	Paths []string `yaml:"paths"`
}

// Validate validates the yaml configurator configuration.
func (c *Config) Validate() error {
	if len(c.Paths) == 0 {
		return fmt.Errorf("yaml configurator: at least one path must be specified")
	}
	return nil
}
