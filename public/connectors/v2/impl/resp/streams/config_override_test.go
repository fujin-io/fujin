//go:build resp_streams

package streams

import (
	"testing"
	"time"

	internal_connectors "github.com/fujin-io/fujin/internal/connectors"
	public_connectors "github.com/fujin-io/fujin/public/connectors"
	reader_config "github.com/fujin-io/fujin/public/connectors/reader/config"
	writer_config "github.com/fujin-io/fujin/public/connectors/writer/config"
)

func TestApplyOverrides_WithRESPStreamsConverter(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "resp_streams",
				Settings: map[string]any{
					"init_address": []string{"localhost:6379"},
					"stream":       "test-stream",
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "resp_streams",
				Settings: map[string]any{
					"init_address": []string{"localhost:6379"},
					"streams": map[string]any{
						"test-stream": map[string]any{},
					},
				},
			},
		},
	}

	overrides := map[string]string{
		"reader.sub.block": "-1",
		"reader.sub.count": "100",
	}

	result, err := internal_connectors.ApplyOverrides(baseConfig, overrides)
	if err != nil {
		t.Fatalf("ApplyOverrides() error = %v", err)
	}

	// Check reader overrides
	subSettings := result.Readers["sub"].Settings.(map[string]any)
	if block, ok := subSettings["block"].(time.Duration); !ok || block != -1 {
		t.Errorf("ApplyOverrides() block = %v, want -1", block)
	}

	if count, ok := subSettings["count"].(int64); !ok || count != 100 {
		t.Errorf("ApplyOverrides() count = %v, want 100", count)
	}
}

func TestApplyOverrides_RESPStreamsConverterValidation(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "resp_streams",
				Settings: map[string]any{
					"init_address": []string{"localhost:6379"},
					"stream":       "test-stream",
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "resp_streams",
				Settings: map[string]any{
					"init_address": []string{"localhost:6379"},
					"streams": map[string]any{
						"test-stream": map[string]any{},
					},
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
			name:    "invalid duration format for block",
			path:    "reader.sub.block",
			value:   "invalid-duration",
			wantErr: true,
			errMsg:  "invalid duration format",
		},
		{
			name:    "valid block -1 (no blocking)",
			path:    "reader.sub.block",
			value:   "-1",
			wantErr: false,
		},
		{
			name:    "valid block duration",
			path:    "reader.sub.block",
			value:   "5s",
			wantErr: false,
		},
		{
			name:    "invalid count (negative)",
			path:    "reader.sub.count",
			value:   "-100",
			wantErr: true,
			errMsg:  "must be non-negative",
		},
		{
			name:    "invalid count format",
			path:    "reader.sub.count",
			value:   "not-a-number",
			wantErr: true,
			errMsg:  "invalid int64 format",
		},
		{
			name:    "stream forbidden",
			path:    "writer.pub.stream",
			value:   "other-stream",
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
		{
			name:    "valid count",
			path:    "reader.sub.count",
			value:   "50",
			wantErr: false,
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
