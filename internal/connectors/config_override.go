package connectors

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	v2 "github.com/fujin-io/fujin/public/connectors/v2"
	writer_pkg "github.com/fujin-io/fujin/public/connectors/writer"
	"gopkg.in/yaml.v3"
)

// DeepCopyConfig creates a deep copy of the connectors configuration
func DeepCopyConfig(original v2.ConnectorConfig) (v2.ConnectorConfig, error) {
	// Use YAML marshaling/unmarshaling for deep copy
	// This works because all config types have YAML tags
	data, err := yaml.Marshal(original)
	if err != nil {
		return v2.ConnectorConfig{}, fmt.Errorf("marshal config for deep copy: %w", err)
	}

	var copy v2.ConnectorConfig
	if err := yaml.Unmarshal(data, &copy); err != nil {
		return v2.ConnectorConfig{}, fmt.Errorf("unmarshal config for deep copy: %w", err)
	}

	return copy, nil
}

// ApplyOverrides applies configuration overrides to a base configuration
// Overrides format: "{type}.{setting_path}" -> "value"
// Example: "settings.client1.transactional_id" -> "my-tx-id-12345"
// Example: "settings.client2.group" -> "my-consumer-group"
func ApplyOverrides(baseConfig v2.ConnectorConfig, overrides map[string]string) (v2.ConnectorConfig, error) {
	// Create a deep copy to avoid modifying the original
	config, err := DeepCopyConfig(baseConfig)
	if err != nil {
		return v2.ConnectorConfig{}, fmt.Errorf("deep copy config: %w", err)
	}

	// Process each override
	for path, value := range overrides {
		if err != nil {
			return v2.ConnectorConfig{}, fmt.Errorf("parse config path '%s': %w", path, err)
		}

		overridenSettings, err := applySetting(config.Protocol, config.Settings, path, value)
		if err != nil {
			return v2.ConnectorConfig{}, fmt.Errorf("apply setting '%s': %w", path, err)
		}

		config.Settings = overridenSettings
	}

	return config, nil
}

func applySetting(protocol string, settings any, settingPath, value string) (any, error) {
	// Convert Settings to map[string]any if it's not already
	settingsMap, err := settingsToMap(settings)
	if err != nil {
		return nil, fmt.Errorf("convert settings to map: %w", err)
	}

	// Try to use protocol-specific converter if available
	var convertedValue any
	converter := writer_pkg.GetConfigValueConverter(protocol)
	if converter != nil {
		converted, err := converter(settingPath, value)
		if err != nil {
			return nil, fmt.Errorf("convert value for setting '%s': %w", settingPath, err)
		}
		convertedValue = converted
	} else {
		// Fall back to generic conversion
		converted, err := convertValue(value)
		if err != nil {
			return nil, fmt.Errorf("convert value '%s': %w", value, err)
		}
		convertedValue = converted
	}

	// Apply the setting using dot notation (e.g., "transactional_id" or "conn.addr")
	if err := setNestedValueWithValue(settingsMap, settingPath, convertedValue); err != nil {
		return nil, fmt.Errorf("set nested value: %w", err)
	}

	return settingsMap, nil
}

// settingsToMap converts Settings (any) to map[string]any
func settingsToMap(settings any) (map[string]any, error) {
	if settings == nil {
		return make(map[string]any), nil
	}

	// If it's already a map, return it
	if m, ok := settings.(map[string]any); ok {
		return m, nil
	}

	// Convert via YAML marshaling/unmarshaling
	data, err := yaml.Marshal(settings)
	if err != nil {
		return nil, fmt.Errorf("marshal settings: %w", err)
	}

	var result map[string]any
	if err := yaml.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("unmarshal settings to map: %w", err)
	}

	return result, nil
}

// setNestedValueWithValue sets a value in a nested map using dot notation
// Example: "transactional_id" -> set top-level key
// Example: "conn.addr" -> set nested key
func setNestedValueWithValue(m map[string]any, path string, value any) error {
	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return fmt.Errorf("empty path")
	}

	// Navigate to the target map
	current := m
	for i := 0; i < len(parts)-1; i++ {
		key := parts[i]
		next, exists := current[key]
		if !exists {
			// Create nested map if it doesn't exist
			next = make(map[string]any)
			current[key] = next
		}

		nextMap, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("path '%s' contains non-map value at '%s'", path, strings.Join(parts[:i+1], "."))
		}
		current = nextMap
	}

	// Set the final value
	finalKey := parts[len(parts)-1]
	current[finalKey] = value
	return nil
}

// convertValue attempts to convert a string value to an appropriate type
// Tries: bool, int, float64, duration, then falls back to string
func convertValue(s string) (any, error) {
	// Try bool
	if s == "true" {
		return true, nil
	}
	if s == "false" {
		return false, nil
	}

	// Try int
	if i, err := strconv.Atoi(s); err == nil {
		return i, nil
	}

	// Try float64
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}

	// Try duration (e.g., "10ms", "5s", "1h")
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// Try []string (comma-separated)
	if strings.Contains(s, ",") {
		parts := strings.Split(s, ",")
		result := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				result = append(result, part)
			}
		}
		if len(result) > 0 {
			return result, nil
		}
	}

	// Fall back to string
	return s, nil
}
