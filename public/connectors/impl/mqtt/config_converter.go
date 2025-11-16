//go:build mqtt

package mqtt

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// allowedWriterSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedWriterSettings = map[string]bool{
	"qos":                  true, // Quality of Service level
	"retain":               true, // Retain flag
	"keep_alive":           true, // Keep alive interval
	"disconnect_timeout":   true, // Disconnect timeout
	"pool.size":            true, // Pool size
	"pool.release_timeout": true, // Pool release timeout
}

// allowedReaderSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedReaderSettings = map[string]bool{
	"qos":           true, // Quality of Service level
	"retain":        true, // Retain flag
	"keep_alive":    true, // Keep alive interval
	"clean_session": true, // Clean session flag
}

// convertWriterConfigValue converts and validates a configuration value for MQTT writer
func convertWriterConfigValue(settingPath string, value string) (any, error) {
	// Check if this setting is allowed
	if !allowedWriterSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
	case "qos":
		// QoS level (0, 1, or 2)
		qos, err := strconv.ParseUint(value, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("invalid QoS format: %w (expected 0, 1, or 2)", err)
		}
		if qos > 2 {
			return nil, fmt.Errorf("QoS must be 0, 1, or 2, got: %d", qos)
		}
		return byte(qos), nil

	case "retain":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for 'retain': %w (expected 'true' or 'false')", err)
		}
		return b, nil

	case "keep_alive":
		// Duration value
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for 'keep_alive': %w (expected format: '10ms', '5s', '1h', etc.)", err)
		}
		if d < 0 {
			return nil, fmt.Errorf("keep_alive must be non-negative, got: %v", d)
		}
		return d, nil

	case "disconnect_timeout":
		// Duration value
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for 'disconnect_timeout': %w (expected format: '10ms', '5s', '1h', etc.)", err)
		}
		if d < 0 {
			return nil, fmt.Errorf("disconnect_timeout must be non-negative, got: %v", d)
		}
		return d, nil

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

// convertReaderConfigValue converts and validates a configuration value for MQTT reader
func convertReaderConfigValue(settingPath string, value string) (any, error) {
	// Check if this setting is allowed
	if !allowedReaderSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
	case "qos":
		// QoS level (0, 1, or 2)
		qos, err := strconv.ParseUint(value, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("invalid QoS format: %w (expected 0, 1, or 2)", err)
		}
		if qos > 2 {
			return nil, fmt.Errorf("QoS must be 0, 1, or 2, got: %d", qos)
		}
		return byte(qos), nil

	case "retain":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for 'retain': %w (expected 'true' or 'false')", err)
		}
		return b, nil

	case "keep_alive":
		// Duration value
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for 'keep_alive': %w (expected format: '10ms', '5s', '1h', etc.)", err)
		}
		if d < 0 {
			return nil, fmt.Errorf("keep_alive must be non-negative, got: %v", d)
		}
		return d, nil

	case "clean_session":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for 'clean_session': %w (expected 'true' or 'false')", err)
		}
		return b, nil

	default:
		// This should not happen if allowedReaderSettings matches the switch cases
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}
