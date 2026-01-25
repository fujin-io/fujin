package v2

type ConnectorsConfig map[string]ConnectorConfig

type ConnectorConfig struct {
	Protocol string `yaml:"protocol"`
	Settings any    `yaml:"settings"`
	// Overridable    []string       `yaml:"overridable,omitempty"`
	// CommonSettings any            `yaml:"common_settings,omitempty"`
	// Clients        map[string]any `yaml:"clients,omitempty"`
}
