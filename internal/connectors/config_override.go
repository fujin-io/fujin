package connectors

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/fujin-io/fujin/public/connectors"
	reader_pkg "github.com/fujin-io/fujin/public/connectors/reader"
	reader_config "github.com/fujin-io/fujin/public/connectors/reader/config"
	writer_pkg "github.com/fujin-io/fujin/public/connectors/writer"
	writer_config "github.com/fujin-io/fujin/public/connectors/writer/config"
	"gopkg.in/yaml.v3"
)

// ConfigPath represents a parsed configuration path
type ConfigPath struct {
	ConnectorName string // "pub" or "sub" (writer or reader name)
	SettingPath   string // "transactional_id" or "linger" etc.
	IsWriter      bool   // true for writers, false for readers
}

// ParseConfigPath parses a configuration path like "writer.pub.transactional_id" or "reader.sub.group"
// Format: {type}.{connector_name}.{setting_path}
// Returns connector name, setting path, and whether it's a writer
func ParseConfigPath(path string) (ConfigPath, error) {
	parts := strings.SplitN(path, ".", 3)
	if len(parts) != 3 {
		return ConfigPath{}, fmt.Errorf("invalid config path format: expected 'writer.connector.setting' or 'reader.connector.setting', got '%s'", path)
	}

	connectorKind := strings.ToLower(parts[0])
	connectorName := parts[1]
	settingPath := parts[2]

	// Validate connector kind
	var isWriter bool
	switch connectorKind {
	case "writer":
		isWriter = true
	case "reader":
		isWriter = false
	default:
		return ConfigPath{}, fmt.Errorf("invalid connector type '%s': expected 'writer' or 'reader'", connectorKind)
	}

	if connectorName == "" {
		return ConfigPath{}, fmt.Errorf("connector name cannot be empty in path '%s'", path)
	}
	if settingPath == "" {
		return ConfigPath{}, fmt.Errorf("setting path cannot be empty in path '%s'", path)
	}

	return ConfigPath{
		ConnectorName: connectorName,
		SettingPath:   settingPath,
		IsWriter:      isWriter,
	}, nil
}

// DeepCopyConfig creates a deep copy of the connectors configuration
func DeepCopyConfig(original connectors.Config) (connectors.Config, error) {
	// Use YAML marshaling/unmarshaling for deep copy
	// This works because all config types have YAML tags
	data, err := yaml.Marshal(original)
	if err != nil {
		return connectors.Config{}, fmt.Errorf("marshal config for deep copy: %w", err)
	}

	var copy connectors.Config
	if err := yaml.Unmarshal(data, &copy); err != nil {
		return connectors.Config{}, fmt.Errorf("unmarshal config for deep copy: %w", err)
	}

	return copy, nil
}

// ApplyOverrides applies configuration overrides to a base configuration
// Overrides format: "{type}.{connector_name}.{setting_path}" -> "value"
// Example: "writer.pub.transactional_id" -> "my-tx-id-12345"
// Example: "reader.sub.group" -> "my-consumer-group"
func ApplyOverrides(baseConfig connectors.Config, overrides map[string]string) (connectors.Config, error) {

	// Create a deep copy to avoid modifying the original
	config, err := DeepCopyConfig(baseConfig)
	if err != nil {
		return connectors.Config{}, fmt.Errorf("deep copy config: %w", err)
	}

	// Process each override
	for path, value := range overrides {
		parsed, err := ParseConfigPath(path)
		if err != nil {
			return connectors.Config{}, fmt.Errorf("parse config path '%s': %w", path, err)
		}

		// Apply based on explicitly specified type
		if parsed.IsWriter {
			writer, exists := config.Writers[parsed.ConnectorName]
			if !exists {
				return connectors.Config{}, fmt.Errorf("writer '%s' not found in config", parsed.ConnectorName)
			}
			if err := applySettingToWriter(&writer, parsed.SettingPath, value); err != nil {
				return connectors.Config{}, fmt.Errorf("apply setting '%s' to writer '%s': %w", parsed.SettingPath, parsed.ConnectorName, err)
			}
			config.Writers[parsed.ConnectorName] = writer
		} else {
			reader, exists := config.Readers[parsed.ConnectorName]
			if !exists {
				return connectors.Config{}, fmt.Errorf("reader '%s' not found in config", parsed.ConnectorName)
			}
			if err := applySettingToReader(&reader, parsed.SettingPath, value); err != nil {
				return connectors.Config{}, fmt.Errorf("apply setting '%s' to reader '%s': %w", parsed.SettingPath, parsed.ConnectorName, err)
			}
			config.Readers[parsed.ConnectorName] = reader
		}
	}

	return config, nil
}

// applySettingToWriter applies a setting value to a writer's settings
func applySettingToWriter(writer *writer_config.Config, settingPath string, value string) error {
	// Convert Settings to map[string]any if it's not already
	settingsMap, err := settingsToMap(writer.Settings)
	if err != nil {
		return fmt.Errorf("convert settings to map: %w", err)
	}

	// Try to use protocol-specific converter if available
	var convertedValue any
	converter := writer_pkg.GetConfigValueConverter(writer.Protocol)
	if converter != nil {
		converted, err := converter(settingPath, value)
		if err != nil {
			return fmt.Errorf("convert value for setting '%s': %w", settingPath, err)
		}
		convertedValue = converted
	} else {
		// Fall back to generic conversion
		converted, err := convertValue(value)
		if err != nil {
			return fmt.Errorf("convert value '%s': %w", value, err)
		}
		convertedValue = converted
	}

	// Apply the setting using dot notation (e.g., "transactional_id" or "conn.addr")
	if err := setNestedValueWithValue(settingsMap, settingPath, convertedValue); err != nil {
		return fmt.Errorf("set nested value: %w", err)
	}

	writer.Settings = settingsMap
	return nil
}

// applySettingToReader applies a setting value to a reader's settings
func applySettingToReader(reader *reader_config.Config, settingPath string, value string) error {
	// Convert Settings to map[string]any if it's not already
	settingsMap, err := settingsToMap(reader.Settings)
	if err != nil {
		return fmt.Errorf("convert settings to map: %w", err)
	}

	// Try to use protocol-specific converter if available
	var convertedValue any
	converter := reader_pkg.GetConfigValueConverter(reader.Protocol)
	if converter != nil {
		converted, err := converter(settingPath, value)
		if err != nil {
			return fmt.Errorf("convert value for setting '%s': %w", settingPath, err)
		}
		convertedValue = converted
	} else {
		// Fall back to generic conversion
		converted, err := convertValue(value)
		if err != nil {
			return fmt.Errorf("convert value '%s': %w", value, err)
		}
		convertedValue = converted
	}

	// Apply the setting using dot notation (e.g., "group" or "conn.addr")
	if err := setNestedValueWithValue(settingsMap, settingPath, convertedValue); err != nil {
		return fmt.Errorf("set nested value: %w", err)
	}

	reader.Settings = settingsMap
	return nil
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
