//go:build nsq

package nsq

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// allowedWriterSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedWriterSettings = map[string]bool{
	"pool.size":            true, // Pool size
	"pool.release_timeout": true, // Pool release timeout
}

// allowedReaderSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedReaderSettings = map[string]bool{
	"max_in_flight": true, // Maximum in-flight messages
}

// convertWriterConfigValue converts and validates a configuration value for NSQ writer
func convertWriterConfigValue(settingPath string, value string) (any, error) {
	// Check if this setting is allowed
	if !allowedWriterSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
	case "pool.size":
		// Integer value
		i, err := strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("invalid integer format for 'pool.size': %w", err)
		}
		if i <= 0 {
			return nil, fmt.Errorf("pool.size must be positive, got: %d", i)
		}
		return i, nil

	case "pool.release_timeout":
		// Duration value
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for 'pool.release_timeout': %w (expected format: '10ms', '5s', '1h', etc.)", err)
		}
		if d < 0 {
			return nil, fmt.Errorf("pool.release_timeout must be non-negative, got: %v", d)
		}
		return d, nil

	default:
		// This should not happen if allowedWriterSettings matches the switch cases
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}

// convertReaderConfigValue converts and validates a configuration value for NSQ reader
func convertReaderConfigValue(settingPath string, value string) (any, error) {
	// Check if this setting is allowed
	if !allowedReaderSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
	case "max_in_flight":
		// Integer value
		i, err := strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("invalid integer format for 'max_in_flight': %w", err)
		}
		if i <= 0 {
			return nil, fmt.Errorf("max_in_flight must be positive, got: %d", i)
		}
		return i, nil

	default:
		// This should not happen if allowedReaderSettings matches the switch cases
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}
