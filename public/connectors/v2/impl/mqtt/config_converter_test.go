//go:build mqtt

package mqtt

import (
	"strings"
	"testing"
	"time"
)

func TestConvertWriterConfigValue(t *testing.T) {
	tests := []struct {
		name        string
		settingPath string
		value       string
		wantType    string
		wantValue   any
		wantErr     bool
		errMsg      string
	}{
		{
			name:        "qos valid 0",
			settingPath: "qos",
			value:       "0",
			wantType:    "byte",
			wantValue:   byte(0),
			wantErr:     false,
		},
		{
			name:        "qos valid 1",
			settingPath: "qos",
			value:       "1",
			wantType:    "byte",
			wantValue:   byte(1),
			wantErr:     false,
		},
		{
			name:        "qos valid 2",
			settingPath: "qos",
			value:       "2",
			wantType:    "byte",
			wantValue:   byte(2),
			wantErr:     false,
		},
		{
			name:        "qos invalid too high",
			settingPath: "qos",
			value:       "3",
			wantErr:     true,
			errMsg:      "QoS must be 0, 1, or 2",
		},
		{
			name:        "qos invalid format",
			settingPath: "qos",
			value:       "invalid",
			wantErr:     true,
			errMsg:      "invalid QoS format",
		},
		{
			name:        "retain true",
			settingPath: "retain",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "retain false",
			settingPath: "retain",
			value:       "false",
			wantType:    "bool",
			wantValue:   false,
			wantErr:     false,
		},
		{
			name:        "retain invalid",
			settingPath: "retain",
			value:       "yes",
			wantErr:     true,
			errMsg:      "invalid boolean format",
		},
		{
			name:        "keep_alive valid",
			settingPath: "keep_alive",
			value:       "30s",
			wantType:    "duration",
			wantValue:   30 * time.Second,
			wantErr:     false,
		},
		{
			name:        "keep_alive invalid format",
			settingPath: "keep_alive",
			value:       "invalid",
			wantErr:     true,
			errMsg:      "invalid duration format",
		},
		{
			name:        "keep_alive negative",
			settingPath: "keep_alive",
			value:       "-10s",
			wantErr:     true,
			errMsg:      "must be non-negative",
		},
		{
			name:        "disconnect_timeout valid",
			settingPath: "disconnect_timeout",
			value:       "5s",
			wantType:    "duration",
			wantValue:   5 * time.Second,
			wantErr:     false,
		},
		{
			name:        "disconnect_timeout invalid format",
			settingPath: "disconnect_timeout",
			value:       "invalid",
			wantErr:     true,
			errMsg:      "invalid duration format",
		},
		{
			name:        "pool.size valid",
			settingPath: "pool.size",
			value:       "10",
			wantType:    "int",
			wantValue:   10,
			wantErr:     false,
		},
		{
			name:        "pool.size invalid format",
			settingPath: "pool.size",
			value:       "not-a-number",
			wantErr:     true,
			errMsg:      "invalid integer format",
		},
		{
			name:        "pool.size zero",
			settingPath: "pool.size",
			value:       "0",
			wantErr:     true,
			errMsg:      "must be positive",
		},
		{
			name:        "pool.size negative",
			settingPath: "pool.size",
			value:       "-5",
			wantErr:     true,
			errMsg:      "must be positive",
		},
		{
			name:        "pool.release_timeout valid",
			settingPath: "pool.release_timeout",
			value:       "10s",
			wantType:    "duration",
			wantValue:   10 * time.Second,
			wantErr:     false,
		},
		{
			name:        "pool.release_timeout invalid format",
			settingPath: "pool.release_timeout",
			value:       "invalid",
			wantErr:     true,
			errMsg:      "invalid duration format",
		},
		// Forbidden settings tests
		{
			name:        "broker forbidden",
			settingPath: "broker",
			value:       "localhost:1883",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "topic forbidden",
			settingPath: "topic",
			value:       "my-topic",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "tls forbidden",
			settingPath: "tls",
			value:       "enabled",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "tls nested forbidden",
			settingPath: "tls.cert_path",
			value:       "/path/to/cert",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertWriterConfigValue(tt.settingPath, tt.value)
			if tt.wantErr {
				if err == nil {
					t.Errorf("convertWriterConfigValue() expected error containing '%s', got nil", tt.errMsg)
				} else if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("convertWriterConfigValue() error = %v, want error containing '%s'", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("convertWriterConfigValue() unexpected error = %v", err)
					return
				}

				// Check type and value
				switch tt.wantType {
				case "string":
					if s, ok := result.(string); !ok || s != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (string)", result, result, tt.wantValue)
					}
				case "int":
					if i, ok := result.(int); !ok || i != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (int)", result, result, tt.wantValue)
					}
				case "bool":
					if b, ok := result.(bool); !ok || b != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (bool)", result, result, tt.wantValue)
					}
				case "byte":
					if b, ok := result.(byte); !ok || b != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (byte)", result, result, tt.wantValue)
					}
				case "duration":
					if d, ok := result.(time.Duration); !ok || d != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (time.Duration)", result, result, tt.wantValue)
					}
				}
			}
		})
	}
}

func TestConvertReaderConfigValue(t *testing.T) {
	tests := []struct {
		name        string
		settingPath string
		value       string
		wantType    string
		wantValue   any
		wantErr     bool
		errMsg      string
	}{
		{
			name:        "qos valid 0",
			settingPath: "qos",
			value:       "0",
			wantType:    "byte",
			wantValue:   byte(0),
			wantErr:     false,
		},
		{
			name:        "qos valid 1",
			settingPath: "qos",
			value:       "1",
			wantType:    "byte",
			wantValue:   byte(1),
			wantErr:     false,
		},
		{
			name:        "qos valid 2",
			settingPath: "qos",
			value:       "2",
			wantType:    "byte",
			wantValue:   byte(2),
			wantErr:     false,
		},
		{
			name:        "qos invalid too high",
			settingPath: "qos",
			value:       "3",
			wantErr:     true,
			errMsg:      "QoS must be 0, 1, or 2",
		},
		{
			name:        "retain true",
			settingPath: "retain",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "retain invalid",
			settingPath: "retain",
			value:       "yes",
			wantErr:     true,
			errMsg:      "invalid boolean format",
		},
		{
			name:        "keep_alive valid",
			settingPath: "keep_alive",
			value:       "30s",
			wantType:    "duration",
			wantValue:   30 * time.Second,
			wantErr:     false,
		},
		{
			name:        "keep_alive invalid format",
			settingPath: "keep_alive",
			value:       "invalid",
			wantErr:     true,
			errMsg:      "invalid duration format",
		},
		{
			name:        "clean_session true",
			settingPath: "clean_session",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "clean_session false",
			settingPath: "clean_session",
			value:       "false",
			wantType:    "bool",
			wantValue:   false,
			wantErr:     false,
		},
		{
			name:        "clean_session invalid",
			settingPath: "clean_session",
			value:       "yes",
			wantErr:     true,
			errMsg:      "invalid boolean format",
		},
		// Forbidden settings tests
		{
			name:        "broker forbidden",
			settingPath: "broker",
			value:       "localhost:1883",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "topic forbidden",
			settingPath: "topic",
			value:       "my-topic",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "tls forbidden",
			settingPath: "tls",
			value:       "enabled",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "tls nested forbidden",
			settingPath: "tls.cert_path",
			value:       "/path/to/cert",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertReaderConfigValue(tt.settingPath, tt.value)
			if tt.wantErr {
				if err == nil {
					t.Errorf("convertReaderConfigValue() expected error containing '%s', got nil", tt.errMsg)
				} else if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("convertReaderConfigValue() error = %v, want error containing '%s'", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("convertReaderConfigValue() unexpected error = %v", err)
					return
				}

				// Check type and value
				switch tt.wantType {
				case "string":
					if s, ok := result.(string); !ok || s != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (string)", result, result, tt.wantValue)
					}
				case "int":
					if i, ok := result.(int); !ok || i != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (int)", result, result, tt.wantValue)
					}
				case "bool":
					if b, ok := result.(bool); !ok || b != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (bool)", result, result, tt.wantValue)
					}
				case "byte":
					if b, ok := result.(byte); !ok || b != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (byte)", result, result, tt.wantValue)
					}
				case "duration":
					if d, ok := result.(time.Duration); !ok || d != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (time.Duration)", result, result, tt.wantValue)
					}
				}
			}
		})
	}
}
