//go:build amqp10

package amqp10

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
			name:        "conn.idle_timeout valid",
			settingPath: "conn.idle_timeout",
			value:       "30s",
			wantType:    "duration",
			wantValue:   30 * time.Second,
			wantErr:     false,
		},
		{
			name:        "conn.idle_timeout invalid format",
			settingPath: "conn.idle_timeout",
			value:       "invalid",
			wantErr:     true,
			errMsg:      "invalid duration format",
		},
		{
			name:        "conn.idle_timeout negative",
			settingPath: "conn.idle_timeout",
			value:       "-10s",
			wantErr:     true,
			errMsg:      "must be non-negative",
		},
		{
			name:        "conn.write_timeout valid",
			settingPath: "conn.write_timeout",
			value:       "5s",
			wantType:    "duration",
			wantValue:   5 * time.Second,
			wantErr:     false,
		},
		{
			name:        "conn.max_frame_size valid",
			settingPath: "conn.max_frame_size",
			value:       "65536",
			wantType:    "uint32",
			wantValue:   uint32(65536),
			wantErr:     false,
		},
		{
			name:        "conn.max_frame_size invalid format",
			settingPath: "conn.max_frame_size",
			value:       "not-a-number",
			wantErr:     true,
			errMsg:      "invalid uint32 format",
		},
		{
			name:        "conn.max_frame_size zero",
			settingPath: "conn.max_frame_size",
			value:       "0",
			wantErr:     true,
			errMsg:      "must be positive",
		},
		{
			name:        "send.settled true",
			settingPath: "send.settled",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "send.settled false",
			settingPath: "send.settled",
			value:       "false",
			wantType:    "bool",
			wantValue:   false,
			wantErr:     false,
		},
		{
			name:        "send.settled invalid",
			settingPath: "send.settled",
			value:       "yes",
			wantErr:     true,
			errMsg:      "invalid boolean format",
		},
		// Forbidden settings tests
		{
			name:        "url forbidden",
			settingPath: "url",
			value:       "amqp://localhost:5672",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "address forbidden",
			settingPath: "address",
			value:       "my-address",
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
			name:        "conn.tls forbidden",
			settingPath: "conn.tls.cert_path",
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
				case "bool":
					if b, ok := result.(bool); !ok || b != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (bool)", result, result, tt.wantValue)
					}
				case "uint32":
					if u, ok := result.(uint32); !ok || u != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (uint32)", result, result, tt.wantValue)
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
			name:        "conn.idle_timeout valid",
			settingPath: "conn.idle_timeout",
			value:       "30s",
			wantType:    "duration",
			wantValue:   30 * time.Second,
			wantErr:     false,
		},
		{
			name:        "conn.idle_timeout invalid format",
			settingPath: "conn.idle_timeout",
			value:       "invalid",
			wantErr:     true,
			errMsg:      "invalid duration format",
		},
		{
			name:        "conn.write_timeout valid",
			settingPath: "conn.write_timeout",
			value:       "5s",
			wantType:    "duration",
			wantValue:   5 * time.Second,
			wantErr:     false,
		},
		{
			name:        "conn.max_frame_size valid",
			settingPath: "conn.max_frame_size",
			value:       "65536",
			wantType:    "uint32",
			wantValue:   uint32(65536),
			wantErr:     false,
		},
		{
			name:        "conn.max_frame_size zero",
			settingPath: "conn.max_frame_size",
			value:       "0",
			wantErr:     true,
			errMsg:      "must be positive",
		},
		{
			name:        "receiver.credit valid",
			settingPath: "receiver.credit",
			value:       "100",
			wantType:    "int32",
			wantValue:   int32(100),
			wantErr:     false,
		},
		{
			name:        "receiver.credit invalid format",
			settingPath: "receiver.credit",
			value:       "not-a-number",
			wantErr:     true,
			errMsg:      "invalid int32 format",
		},
		{
			name:        "receiver.credit zero",
			settingPath: "receiver.credit",
			value:       "0",
			wantErr:     true,
			errMsg:      "must be positive",
		},
		{
			name:        "receiver.credit negative",
			settingPath: "receiver.credit",
			value:       "-5",
			wantErr:     true,
			errMsg:      "must be positive",
		},
		// Forbidden settings tests
		{
			name:        "url forbidden",
			settingPath: "url",
			value:       "amqp://localhost:5672",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "address forbidden",
			settingPath: "address",
			value:       "my-address",
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
			name:        "conn.tls forbidden",
			settingPath: "conn.tls.cert_path",
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
				case "uint32":
					if u, ok := result.(uint32); !ok || u != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (uint32)", result, result, tt.wantValue)
					}
				case "int32":
					if i, ok := result.(int32); !ok || i != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (int32)", result, result, tt.wantValue)
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
