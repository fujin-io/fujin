package amqp091

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// convertConfigValue converts and validates a configuration value for AMQP091
// This function handles both reader and writer settings
func convertConfigValue(settingPath string, value string) (any, error) {
	// Normalize the path (remove "clients.client_name." prefix if present)
	settingName := normalizePath(settingPath)

	switch settingName {
	case "conn.heartbeat":
		// Duration value
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for 'conn.heartbeat': %w (expected format: '10ms', '5s', '1h', etc.)", err)
		}
		if d < 0 {
			return nil, fmt.Errorf("conn.heartbeat must be non-negative, got: %v", d)
		}
		return d, nil

	case "conn.channel_max":
		// Uint16 value
		i, err := strconv.ParseUint(value, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("invalid uint16 format for 'conn.channel_max': %w", err)
		}
		return uint16(i), nil

	case "conn.frame_size":
		// Integer value
		i, err := strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("invalid integer format for 'conn.frame_size': %w", err)
		}
		if i <= 0 {
			return nil, fmt.Errorf("conn.frame_size must be positive, got: %d", i)
		}
		return i, nil

	case "exchange.durable", "exchange.auto_delete", "exchange.internal":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingName, err)
		}
		return b, nil

	case "queue.durable", "queue.auto_delete", "queue.exclusive":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingName, err)
		}
		return b, nil

	case "publish.mandatory", "publish.immediate":
		// Boolean value (writer only)
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingName, err)
		}
		return b, nil

	case "consume.exclusive", "consume.no_local":
		// Boolean value (reader only)
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingName, err)
		}
		return b, nil

	case "ack.multiple", "nack.multiple", "nack.requeue":
		// Boolean value (reader only)
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingName, err)
		}
		return b, nil

	default:
		// This should not happen if the setting is in the whitelist
		// If it does, it indicates a programming error or unsupported setting
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingName)
	}
}

// normalizePath removes the "clients.client_name." prefix from the path if present
// and returns the normalized setting name
func normalizePath(path string) string {
	// Remove "clients." prefix if present
	if strings.HasPrefix(path, "clients.") {
		// Find the next dot after "clients."
		idx := strings.Index(path[8:], ".")
		if idx != -1 {
			// Return everything after "clients.client_name."
			return path[8+idx+1:]
		}
	}
	return path
}

