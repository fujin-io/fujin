//go:build resp_pubsub

package pubsub

import (
	"fmt"
	"strings"
)

// allowedWriterSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedWriterSettings = map[string]bool{
	// Currently no runtime-overridable settings for RESP PubSub writer
	// Channel is a topic identifier and should not be changed at runtime
	// TLS and connection settings are security critical
}

// allowedReaderSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedReaderSettings = map[string]bool{
	// Currently no runtime-overridable settings for RESP PubSub reader
	// Channels are topic identifiers and should not be changed at runtime
	// TLS and connection settings are security critical
}

// convertWriterConfigValue converts and validates a configuration value for RESP PubSub writer
func convertWriterConfigValue(settingPath string, value string) (any, error) {
	// Check if this setting is allowed
	if !allowedWriterSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
	default:
		// This should not happen if allowedWriterSettings matches the switch cases
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}

// convertReaderConfigValue converts and validates a configuration value for RESP PubSub reader
func convertReaderConfigValue(settingPath string, value string) (any, error) {
	// Check if this setting is allowed
	if !allowedReaderSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
	default:
		// This should not happen if allowedReaderSettings matches the switch cases
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}
