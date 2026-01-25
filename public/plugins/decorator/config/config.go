package config

// Config represents a decorator configuration in YAML.
type Config struct {
	Name   string `yaml:"name"`
	Config any    `yaml:"config,omitempty"`
}

