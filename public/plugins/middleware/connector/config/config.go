package config

// Config represents a connector middleware configuration in YAML.
type Config struct {
	// Name is the name of the connector middleware plugin to use.
	Name string `yaml:"name"`

	// Disabled allows disabling a middleware without removing it from the config.
	Disabled bool `yaml:"disabled,omitempty"`

	// Config is the plugin-specific configuration.
	// The structure depends on the middleware plugin type.
	Config map[string]any `yaml:",inline"`
}
