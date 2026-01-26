package streams

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// allowedSettings contains settings that can be overridden at runtime.
var allowedSettings = map[string]bool{
	"block": true, // Block duration for XREAD
	"count": true, // Count for XREAD
}

// convertConfigValue converts and validates a configuration value for Redis Streams.
func convertConfigValue(settingPath string, value string) (any, error) {
	if !allowedSettings[settingPath] {
		if strings.HasPrefix(settingPath, "tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch settingPath {
	case "block":
		if value == "-1" {
			return time.Duration(-1), nil
		}
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for 'block': %w (expected format: '10ms', '5s', '1h', etc., or '-1' for no blocking)", err)
		}
		return d, nil

	case "count":
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int64 format for 'count': %w", err)
		}
		if i < 0 {
			return nil, fmt.Errorf("count must be non-negative, got: %d", i)
		}
		return i, nil

	default:
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}
