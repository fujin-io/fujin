//go:build nats_core

package core

import (
	"strings"
	"testing"
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
			value:       "nats://localhost:4222",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "subject forbidden",
			settingPath: "subject",
			value:       "my-subject",
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
		wantErr     bool
		errMsg      string
	}{
		// Forbidden settings tests
		{
			name:        "url forbidden",
			settingPath: "url",
			value:       "nats://localhost:4222",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "subject forbidden",
			settingPath: "subject",
			value:       "my-subject",
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
				if result != nil {
					t.Errorf("convertReaderConfigValue() = %v, want nil", result)
				}
			}
		})
	}
}
