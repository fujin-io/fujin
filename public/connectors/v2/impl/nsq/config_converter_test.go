//go:build nsq

package nsq

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
		{
			name:        "pool.release_timeout negative",
			settingPath: "pool.release_timeout",
			value:       "-10s",
			wantErr:     true,
			errMsg:      "must be non-negative",
		},
		// Forbidden settings tests
		{
			name:        "nsqd_addr forbidden",
			settingPath: "nsqd_addr",
			value:       "localhost:4150",
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
				case "int":
					if i, ok := result.(int); !ok || i != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (int)", result, result, tt.wantValue)
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
			name:        "max_in_flight valid",
			settingPath: "max_in_flight",
			value:       "100",
			wantType:    "int",
			wantValue:   100,
			wantErr:     false,
		},
		{
			name:        "max_in_flight invalid format",
			settingPath: "max_in_flight",
			value:       "not-a-number",
			wantErr:     true,
			errMsg:      "invalid integer format",
		},
		{
			name:        "max_in_flight zero",
			settingPath: "max_in_flight",
			value:       "0",
			wantErr:     true,
			errMsg:      "must be positive",
		},
		{
			name:        "max_in_flight negative",
			settingPath: "max_in_flight",
			value:       "-5",
			wantErr:     true,
			errMsg:      "must be positive",
		},
		// Forbidden settings tests
		{
			name:        "lookupd_addrs forbidden",
			settingPath: "lookupd_addrs",
			value:       "localhost:4161",
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
			name:        "channel forbidden",
			settingPath: "channel",
			value:       "my-channel",
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
				case "int":
					if i, ok := result.(int); !ok || i != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (int)", result, result, tt.wantValue)
					}
				}
			}
		})
	}
}
