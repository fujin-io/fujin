//go:build nats_core

package core

import (
	"testing"

	internal_connectors "github.com/fujin-io/fujin/internal/connectors"
	public_connectors "github.com/fujin-io/fujin/public/connectors"
	reader_config "github.com/fujin-io/fujin/public/connectors/reader/config"
	writer_config "github.com/fujin-io/fujin/public/connectors/writer/config"
)

func TestApplyOverrides_WithNATSCoreConverter(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "nats_core",
				Settings: map[string]any{
					"url":     "nats://localhost:4222",
					"subject": "test-subject",
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "nats_core",
				Settings: map[string]any{
					"url":     "nats://localhost:4222",
					"subject": "test-subject",
				},
			},
		},
	}

	// NATS Core currently has no overridable settings
	// This test verifies that forbidden settings are properly rejected
	overrides := map[string]string{
		"writer.pub.url":     "nats://other:4222",
		"writer.pub.subject": "other-subject",
	}

	_, err := internal_connectors.ApplyOverrides(baseConfig, overrides)
	if err == nil {
		t.Error("ApplyOverrides() expected error for forbidden settings, got nil")
	}
}

func TestApplyOverrides_NATSCoreConverterValidation(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "nats_core",
				Settings: map[string]any{
					"url":     "nats://localhost:4222",
					"subject": "test-subject",
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "nats_core",
				Settings: map[string]any{
					"url":     "nats://localhost:4222",
					"subject": "test-subject",
				},
			},
		},
	}

	tests := []struct {
		name    string
		path    string
		value   string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "url forbidden",
			path:    "writer.pub.url",
			value:   "nats://other:4222",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
		{
			name:    "subject forbidden",
			path:    "writer.pub.subject",
			value:   "other-subject",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
		{
			name:    "reader url forbidden",
			path:    "reader.sub.url",
			value:   "nats://other:4222",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
		{
			name:    "reader subject forbidden",
			path:    "reader.sub.subject",
			value:   "other-subject",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			overrides := map[string]string{
				tt.path: tt.value,
			}

			_, err := internal_connectors.ApplyOverrides(baseConfig, overrides)
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
