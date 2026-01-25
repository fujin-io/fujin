package common

type CommonConfig struct {
	Overridable    []string       `yaml:"overridable,omitempty"`
	CommonSettings any            `yaml:"common_settings,omitempty"`
	Clients        map[string]any `yaml:"clients,omitempty"`
}
