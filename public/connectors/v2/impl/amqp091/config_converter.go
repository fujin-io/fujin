//go:build amqp091

package amqp091

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// allowedWriterSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedWriterSettings = map[string]bool{
	"conn.heartbeat":       true, // Connection heartbeat interval
	"conn.channel_max":     true, // Maximum channels
	"conn.frame_size":      true, // Frame size
	"exchange.durable":     true, // Exchange durability
	"exchange.auto_delete": true, // Exchange auto-delete
	"exchange.internal":    true, // Exchange internal flag
	"queue.durable":        true, // Queue durability
	"queue.auto_delete":    true, // Queue auto-delete
	"queue.exclusive":      true, // Queue exclusive flag
	"publish.mandatory":    true, // Publish mandatory flag
	"publish.immediate":    true, // Publish immediate flag
}

// allowedReaderSettings contains settings that can be overridden at runtime
// Only settings listed here can be dynamically configured per session
var allowedReaderSettings = map[string]bool{
	"conn.heartbeat":       true, // Connection heartbeat interval
	"conn.channel_max":     true, // Maximum channels
	"conn.frame_size":      true, // Frame size
	"exchange.durable":     true, // Exchange durability
	"exchange.auto_delete": true, // Exchange auto-delete
	"exchange.internal":    true, // Exchange internal flag
	"queue.durable":        true, // Queue durability
	"queue.auto_delete":    true, // Queue auto-delete
	"queue.exclusive":      true, // Queue exclusive flag
	"consume.exclusive":    true, // Consume exclusive flag
	"consume.no_local":     true, // Consume no-local flag
	"ack.multiple":         true, // ACK multiple flag
	"nack.multiple":        true, // NACK multiple flag
	"nack.requeue":         true, // NACK requeue flag
}

// convertWriterConfigValue converts and validates a configuration value for AMQP091 writer
func convertWriterConfigValue(settingPath string, value string) (any, error) {
	// Check if this setting is allowed
	if !allowedWriterSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") || strings.HasPrefix(settingPath, "conn.tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
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
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingPath, err)
		}
		return b, nil

	case "queue.durable", "queue.auto_delete", "queue.exclusive":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingPath, err)
		}
		return b, nil

	case "publish.mandatory", "publish.immediate":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingPath, err)
		}
		return b, nil

	default:
		// This should not happen if allowedWriterSettings matches the switch cases
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}

// convertReaderConfigValue converts and validates a configuration value for AMQP091 reader
func convertReaderConfigValue(settingPath string, value string) (any, error) {
	// Check if this setting is allowed
	if !allowedReaderSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") || strings.HasPrefix(settingPath, "conn.tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
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
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingPath, err)
		}
		return b, nil

	case "queue.durable", "queue.auto_delete", "queue.exclusive":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingPath, err)
		}
		return b, nil

	case "consume.exclusive", "consume.no_local":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingPath, err)
		}
		return b, nil

	case "ack.multiple", "nack.multiple", "nack.requeue":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingPath, err)
		}
		return b, nil

	default:
		// This should not happen if allowedReaderSettings matches the switch cases
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}
