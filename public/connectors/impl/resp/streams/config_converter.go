//go:build resp_streams

package streams

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// allowedWriterSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedWriterSettings = map[string]bool{
	// Currently no runtime-overridable settings for RESP Streams writer
	// Stream is a topic identifier and should not be changed at runtime
	// TLS and connection settings are security critical
}

// allowedReaderSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedReaderSettings = map[string]bool{
	"block": true, // Block duration for XREAD
	"count": true, // Count for XREAD
}

// convertWriterConfigValue converts and validates a configuration value for RESP Streams writer
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

// convertReaderConfigValue converts and validates a configuration value for RESP Streams reader
func convertReaderConfigValue(settingPath string, value string) (any, error) {
	// Check if this setting is allowed
	if !allowedReaderSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
	case "block":
		// Duration value (can be -1 for no blocking)
		if value == "-1" {
			return time.Duration(-1), nil
		}
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for 'block': %w (expected format: '10ms', '5s', '1h', etc., or '-1' for no blocking)", err)
		}
		return d, nil

	case "count":
		// Int64 value
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int64 format for 'count': %w", err)
		}
		if i < 0 {
			return nil, fmt.Errorf("count must be non-negative, got: %d", i)
		}
		return i, nil

	default:
		// This should not happen if allowedReaderSettings matches the switch cases
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}
