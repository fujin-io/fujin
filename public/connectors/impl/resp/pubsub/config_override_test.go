//go:build resp_pubsub

package pubsub

import (
	"testing"

	internal_connectors "github.com/fujin-io/fujin/internal/connectors"
	public_connectors "github.com/fujin-io/fujin/public/connectors"
	reader_config "github.com/fujin-io/fujin/public/connectors/reader/config"
	writer_config "github.com/fujin-io/fujin/public/connectors/writer/config"
)

func TestApplyOverrides_WithRESPPubSubConverter(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "resp_pubsub",
				Settings: map[string]any{
					"init_address": []string{"localhost:6379"},
					"channel":      "test-channel",
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "resp_pubsub",
				Settings: map[string]any{
					"init_address": []string{"localhost:6379"},
					"channels":     []string{"test-channel"},
				},
			},
		},
	}

	// RESP PubSub currently has no overridable settings
	// This test verifies that forbidden settings are properly rejected
	overrides := map[string]string{
		"writer.pub.channel":      "other-channel",
		"reader.sub.channels":     "other-channel",
		"writer.pub.init_address": "other:6379",
	}

	_, err := internal_connectors.ApplyOverrides(baseConfig, overrides)
	if err == nil {
		t.Error("ApplyOverrides() expected error for forbidden settings, got nil")
	}
}

func TestApplyOverrides_RESPPubSubConverterValidation(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "resp_pubsub",
				Settings: map[string]any{
					"init_address": []string{"localhost:6379"},
					"channel":      "test-channel",
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "resp_pubsub",
				Settings: map[string]any{
					"init_address": []string{"localhost:6379"},
					"channels":     []string{"test-channel"},
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
			name:    "channel forbidden",
			path:    "writer.pub.channel",
			value:   "other-channel",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
		{
			name:    "channels forbidden",
			path:    "reader.sub.channels",
			value:   "other-channel",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
		{
			name:    "init_address forbidden",
			path:    "writer.pub.init_address",
			value:   "other:6379",
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
