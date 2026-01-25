//go:build resp_streams

package streams

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
		wantErr     bool
		errMsg      string
	}{
		// Forbidden settings tests
		{
			name:        "url forbidden",
			settingPath: "url",
			value:       "redis://localhost:6379",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "stream forbidden",
			settingPath: "stream",
			value:       "my-stream",
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
				if result != nil {
					t.Errorf("convertWriterConfigValue() = %v, want nil", result)
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
			name:        "block valid",
			settingPath: "block",
			value:       "5s",
			wantType:    "duration",
			wantValue:   5 * time.Second,
			wantErr:     false,
		},
		{
			name:        "block no blocking",
			settingPath: "block",
			value:       "-1",
			wantType:    "duration",
			wantValue:   time.Duration(-1),
			wantErr:     false,
		},
		{
			name:        "block invalid format",
			settingPath: "block",
			value:       "invalid",
			wantErr:     true,
			errMsg:      "invalid duration format",
		},
		{
			name:        "count valid",
			settingPath: "count",
			value:       "100",
			wantType:    "int64",
			wantValue:   int64(100),
			wantErr:     false,
		},
		{
			name:        "count zero",
			settingPath: "count",
			value:       "0",
			wantType:    "int64",
			wantValue:   int64(0),
			wantErr:     false,
		},
		{
			name:        "count invalid format",
			settingPath: "count",
			value:       "not-a-number",
			wantErr:     true,
			errMsg:      "invalid int64 format",
		},
		{
			name:        "count negative",
			settingPath: "count",
			value:       "-5",
			wantErr:     true,
			errMsg:      "must be non-negative",
		},
		// Forbidden settings tests
		{
			name:        "url forbidden",
			settingPath: "url",
			value:       "redis://localhost:6379",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "stream forbidden",
			settingPath: "stream",
			value:       "my-stream",
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
				case "int64":
					if i, ok := result.(int64); !ok || i != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (int64)", result, result, tt.wantValue)
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
