//go:build amqp10

package amqp10

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// allowedWriterSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedWriterSettings = map[string]bool{
	"conn.idle_timeout":   true, // Connection idle timeout
	"conn.max_frame_size": true, // Maximum frame size
	"conn.write_timeout":  true, // Write timeout
	"send.settled":        true, // Send settled flag
}

// allowedReaderSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedReaderSettings = map[string]bool{
	"conn.idle_timeout":   true, // Connection idle timeout
	"conn.max_frame_size": true, // Maximum frame size
	"conn.write_timeout":  true, // Write timeout
	"receiver.credit":     true, // Receiver credit
}

// convertWriterConfigValue converts and validates a configuration value for AMQP10 writer
func convertWriterConfigValue(settingPath string, value string) (any, error) {
	// Check if this setting is allowed
	if !allowedWriterSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") || strings.HasPrefix(settingPath, "conn.tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
	case "conn.idle_timeout", "conn.write_timeout":
		// Duration value
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for '%s': %w (expected format: '10ms', '5s', '1h', etc.)", settingPath, err)
		}
		if d < 0 {
			return nil, fmt.Errorf("%s must be non-negative, got: %v", settingPath, d)
		}
		return d, nil

	case "conn.max_frame_size":
		// Uint32 value
		i, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid uint32 format for 'conn.max_frame_size': %w", err)
		}
		if i == 0 {
			return nil, fmt.Errorf("conn.max_frame_size must be positive, got: %d", i)
		}
		return uint32(i), nil

	case "send.settled":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for 'send.settled': %w (expected 'true' or 'false')", err)
		}
		return b, nil

	default:
		// This should not happen if allowedWriterSettings matches the switch cases
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}

// convertReaderConfigValue converts and validates a configuration value for AMQP10 reader
func convertReaderConfigValue(settingPath string, value string) (any, error) {
	// Check if this setting is allowed
	if !allowedReaderSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") || strings.HasPrefix(settingPath, "conn.tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
	case "conn.idle_timeout", "conn.write_timeout":
		// Duration value
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for '%s': %w (expected format: '10ms', '5s', '1h', etc.)", settingPath, err)
		}
		if d < 0 {
			return nil, fmt.Errorf("%s must be non-negative, got: %v", settingPath, d)
		}
		return d, nil

	case "conn.max_frame_size":
		// Uint32 value
		i, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid uint32 format for 'conn.max_frame_size': %w", err)
		}
		if i == 0 {
			return nil, fmt.Errorf("conn.max_frame_size must be positive, got: %d", i)
		}
		return uint32(i), nil

	case "receiver.credit":
		// Int32 value
		i, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid int32 format for 'receiver.credit': %w", err)
		}
		if i <= 0 {
			return nil, fmt.Errorf("receiver.credit must be positive, got: %d", i)
		}
		return int32(i), nil

	default:
		// This should not happen if allowedReaderSettings matches the switch cases
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}
