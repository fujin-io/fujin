package connectors

import (
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/connectors"
	reader_config "github.com/fujin-io/fujin/public/connectors/reader/config"
	writer_config "github.com/fujin-io/fujin/public/connectors/writer/config"
)

func TestParseConfigPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    ConfigPath
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid writer path",
			path: "writer.pub.setting_name",
			want: ConfigPath{
				ConnectorName: "pub",
				SettingPath:   "setting_name",
				IsWriter:      true,
			},
			wantErr: false,
		},
		{
			name: "valid reader path",
			path: "reader.sub.group",
			want: ConfigPath{
				ConnectorName: "sub",
				SettingPath:   "group",
				IsWriter:      false,
			},
			wantErr: false,
		},
		{
			name: "nested setting path",
			path: "writer.pub.conn.addr",
			want: ConfigPath{
				ConnectorName: "pub",
				SettingPath:   "conn.addr",
				IsWriter:      true,
			},
			wantErr: false,
		},
		{
			name:    "invalid format - too few parts",
			path:    "writer.pub",
			wantErr: true,
			errMsg:  "invalid config path format",
		},
		{
			name: "invalid format - too many parts",
			path: "writer.pub.setting.extra",
			want: ConfigPath{
				ConnectorName: "pub",
				SettingPath:   "setting.extra",
				IsWriter:      true,
			},
			wantErr: false, // This is actually valid - extra parts go to settingPath
		},
		{
			name:    "invalid connector type",
			path:    "invalid.pub.setting",
			wantErr: true,
			errMsg:  "invalid connector type",
		},
		{
			name:    "empty connector name",
			path:    "writer..setting",
			wantErr: true,
			errMsg:  "connector name cannot be empty",
		},
		{
			name:    "empty setting path",
			path:    "writer.pub.",
			wantErr: true,
			errMsg:  "setting path cannot be empty",
		},
		{
			name: "case insensitive connector type",
			path: "WRITER.pub.setting",
			want: ConfigPath{
				ConnectorName: "pub",
				SettingPath:   "setting",
				IsWriter:      true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseConfigPath(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseConfigPath() expected error containing '%s', got nil", tt.errMsg)
				} else if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Errorf("ParseConfigPath() error = %v, want error containing '%s'", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ParseConfigPath() unexpected error = %v", err)
					return
				}
				if got.ConnectorName != tt.want.ConnectorName {
					t.Errorf("ParseConfigPath() ConnectorName = %v, want %v", got.ConnectorName, tt.want.ConnectorName)
				}
				if got.SettingPath != tt.want.SettingPath {
					t.Errorf("ParseConfigPath() SettingPath = %v, want %v", got.SettingPath, tt.want.SettingPath)
				}
				if got.IsWriter != tt.want.IsWriter {
					t.Errorf("ParseConfigPath() IsWriter = %v, want %v", got.IsWriter, tt.want.IsWriter)
				}
			}
		})
	}
}

func TestDeepCopyConfig(t *testing.T) {
	original := connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "test",
				Settings: map[string]any{
					"key1": "value1",
					"key2": 42,
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "test",
				Settings: map[string]any{
					"key1": "value1",
				},
			},
		},
	}

	copy, err := DeepCopyConfig(original)
	if err != nil {
		t.Fatalf("DeepCopyConfig() error = %v", err)
	}

	// Modify copy
	copy.Writers["pub"].Settings.(map[string]any)["key1"] = "modified"
	copy.Readers["sub"].Settings.(map[string]any)["key1"] = "modified"

	// Original should not be modified
	if original.Writers["pub"].Settings.(map[string]any)["key1"] == "modified" {
		t.Error("DeepCopyConfig() modified original config")
	}
	if original.Readers["sub"].Settings.(map[string]any)["key1"] == "modified" {
		t.Error("DeepCopyConfig() modified original config")
	}
}

func TestApplyOverrides_Generic(t *testing.T) {
	baseConfig := connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "generic",
				Settings: map[string]any{
					"key1": "value1",
					"key2": 42,
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "generic",
				Settings: map[string]any{
					"key1": "value1",
				},
			},
		},
	}

	overrides := map[string]string{
		"writer.pub.key1": "new-value1",
		"writer.pub.key3": "value3",
		"reader.sub.key1": "new-value1",
		"reader.sub.key2": "value2",
	}

	result, err := ApplyOverrides(baseConfig, overrides)
	if err != nil {
		t.Fatalf("ApplyOverrides() error = %v", err)
	}

	// Check writer overrides
	pubSettings := result.Writers["pub"].Settings.(map[string]any)
	if pubSettings["key1"] != "new-value1" {
		t.Errorf("ApplyOverrides() writer key1 = %v, want new-value1", pubSettings["key1"])
	}
	if pubSettings["key3"] != "value3" {
		t.Errorf("ApplyOverrides() writer key3 = %v, want value3", pubSettings["key3"])
	}

	// Check reader overrides
	subSettings := result.Readers["sub"].Settings.(map[string]any)
	if subSettings["key1"] != "new-value1" {
		t.Errorf("ApplyOverrides() reader key1 = %v, want new-value1", subSettings["key1"])
	}
	if subSettings["key2"] != "value2" {
		t.Errorf("ApplyOverrides() reader key2 = %v, want value2", subSettings["key2"])
	}

	// Check that original config was not modified
	if baseConfig.Writers["pub"].Settings.(map[string]any)["key1"] == "new-value1" {
		t.Error("ApplyOverrides() modified original config")
	}
}

func TestApplyOverrides_NestedPath(t *testing.T) {
	baseConfig := connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "generic",
				Settings: map[string]any{
					"conn": map[string]any{
						"addr": "localhost:9092",
					},
				},
			},
		},
	}

	overrides := map[string]string{
		"writer.pub.conn.addr":        "localhost:9093",
		"writer.pub.conn.tls.enabled": "true",
	}

	result, err := ApplyOverrides(baseConfig, overrides)
	if err != nil {
		t.Fatalf("ApplyOverrides() error = %v", err)
	}

	pubSettings := result.Writers["pub"].Settings.(map[string]any)
	conn := pubSettings["conn"].(map[string]any)
	if conn["addr"] != "localhost:9093" {
		t.Errorf("ApplyOverrides() conn.addr = %v, want localhost:9093", conn["addr"])
	}

	tls := conn["tls"].(map[string]any)
	if tls["enabled"] != true {
		t.Errorf("ApplyOverrides() conn.tls.enabled = %v, want true", tls["enabled"])
	}
}

func TestApplyOverrides_Errors(t *testing.T) {
	baseConfig := connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "generic",
				Settings: map[string]any{},
			},
		},
	}

	tests := []struct {
		name      string
		overrides map[string]string
		wantErr   bool
		errMsg    string
	}{
		{
			name: "invalid path format",
			overrides: map[string]string{
				"invalid.path": "value",
			},
			wantErr: true,
			errMsg:  "invalid config path format",
		},
		{
			name: "writer not found",
			overrides: map[string]string{
				"writer.nonexistent.setting": "value",
			},
			wantErr: true,
			errMsg:  "writer 'nonexistent' not found",
		},
		{
			name: "reader not found",
			overrides: map[string]string{
				"reader.nonexistent.setting": "value",
			},
			wantErr: true,
			errMsg:  "reader 'nonexistent' not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ApplyOverrides(baseConfig, tt.overrides)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ApplyOverrides() expected error containing '%s', got nil", tt.errMsg)
				} else if !contains(err.Error(), tt.errMsg) {
					t.Errorf("ApplyOverrides() error = %v, want error containing '%s'", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ApplyOverrides() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestSetNestedValueWithValue(t *testing.T) {
	m := make(map[string]any)

	// Test top-level setting
	if err := setNestedValueWithValue(m, "setting_key", "my-value"); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	if m["setting_key"] != "my-value" {
		t.Errorf("setNestedValueWithValue() setting_key = %v, want my-value", m["setting_key"])
	}

	// Test nested setting
	if err := setNestedValueWithValue(m, "conn.addr", "localhost:9092"); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	conn, ok := m["conn"].(map[string]any)
	if !ok {
		t.Fatalf("setNestedValueWithValue() conn is not a map")
	}
	if conn["addr"] != "localhost:9092" {
		t.Errorf("setNestedValueWithValue() conn.addr = %v, want localhost:9092", conn["addr"])
	}

	// Test deeply nested
	if err := setNestedValueWithValue(m, "conn.tls.cert_path", "/path/to/cert"); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	tls, ok := conn["tls"].(map[string]any)
	if !ok {
		t.Fatalf("setNestedValueWithValue() conn.tls is not a map")
	}
	if tls["cert_path"] != "/path/to/cert" {
		t.Errorf("setNestedValueWithValue() conn.tls.cert_path = %v, want /path/to/cert", tls["cert_path"])
	}

	// Test with different types
	if err := setNestedValueWithValue(m, "count", 42); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	if m["count"] != 42 {
		t.Errorf("setNestedValueWithValue() count = %v, want 42", m["count"])
	}

	if err := setNestedValueWithValue(m, "enabled", true); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	if m["enabled"] != true {
		t.Errorf("setNestedValueWithValue() enabled = %v, want true", m["enabled"])
	}

	if err := setNestedValueWithValue(m, "duration", 5*time.Second); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	if m["duration"] != 5*time.Second {
		t.Errorf("setNestedValueWithValue() duration = %v, want 5s", m["duration"])
	}
}

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantType  string
		wantValue any
	}{
		{
			name:      "boolean true",
			input:     "true",
			wantType:  "bool",
			wantValue: true,
		},
		{
			name:      "boolean false",
			input:     "false",
			wantType:  "bool",
			wantValue: false,
		},
		{
			name:      "integer",
			input:     "42",
			wantType:  "int",
			wantValue: 42,
		},
		{
			name:      "float",
			input:     "3.14",
			wantType:  "float64",
			wantValue: 3.14,
		},
		{
			name:      "duration",
			input:     "5s",
			wantType:  "duration",
			wantValue: 5 * time.Second,
		},
		{
			name:      "comma-separated strings",
			input:     "a,b,c",
			wantType:  "[]string",
			wantValue: []string{"a", "b", "c"},
		},
		{
			name:      "string fallback",
			input:     "just a string",
			wantType:  "string",
			wantValue: "just a string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertValue(tt.input)
			if err != nil {
				t.Errorf("convertValue() error = %v", err)
				return
			}

			switch tt.wantType {
			case "bool":
				if b, ok := result.(bool); !ok || b != tt.wantValue {
					t.Errorf("convertValue() = %v (%T), want %v (bool)", result, result, tt.wantValue)
				}
			case "int":
				if i, ok := result.(int); !ok || i != tt.wantValue {
					t.Errorf("convertValue() = %v (%T), want %v (int)", result, result, tt.wantValue)
				}
			case "float64":
				if f, ok := result.(float64); !ok || f != tt.wantValue {
					t.Errorf("convertValue() = %v (%T), want %v (float64)", result, result, tt.wantValue)
				}
			case "duration":
				if d, ok := result.(time.Duration); !ok || d != tt.wantValue {
					t.Errorf("convertValue() = %v (%T), want %v (time.Duration)", result, result, tt.wantValue)
				}
			case "[]string":
				if arr, ok := result.([]string); !ok {
					t.Errorf("convertValue() = %v (%T), want []string", result, result)
				} else {
					wantArr := tt.wantValue.([]string)
					if len(arr) != len(wantArr) {
						t.Errorf("convertValue() []string length = %d, want %d", len(arr), len(wantArr))
					} else {
						for i, v := range arr {
							if v != wantArr[i] {
								t.Errorf("convertValue() []string[%d] = %v, want %v", i, v, wantArr[i])
							}
						}
					}
				}
			case "string":
				if s, ok := result.(string); !ok || s != tt.wantValue {
					t.Errorf("convertValue() = %v (%T), want %v (string)", result, result, tt.wantValue)
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
