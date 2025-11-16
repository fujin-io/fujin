//go:build nsq

package nsq

import (
	"testing"
	"time"

	internal_connectors "github.com/fujin-io/fujin/internal/connectors"
	public_connectors "github.com/fujin-io/fujin/public/connectors"
	reader_config "github.com/fujin-io/fujin/public/connectors/reader/config"
	writer_config "github.com/fujin-io/fujin/public/connectors/writer/config"
)

func TestApplyOverrides_WithNSQConverter(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "nsq",
				Settings: map[string]any{
					"address": "localhost:4150",
					"topic":   "test-topic",
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "nsq",
				Settings: map[string]any{
					"addresses": []string{"localhost:4150"},
					"topic":     "test-topic",
					"channel":   "test-channel",
				},
			},
		},
	}

	overrides := map[string]string{
		"writer.pub.pool.size":            "1500",
		"writer.pub.pool.release_timeout": "8s",
		"reader.sub.max_in_flight":        "100",
	}

	result, err := internal_connectors.ApplyOverrides(baseConfig, overrides)
	if err != nil {
		t.Fatalf("ApplyOverrides() error = %v", err)
	}

	// Check writer overrides
	pubSettings := result.Writers["pub"].Settings.(map[string]any)
	poolSettings := pubSettings["pool"].(map[string]any)
	if poolSize, ok := poolSettings["size"].(int); !ok || poolSize != 1500 {
		t.Errorf("ApplyOverrides() pool.size = %v, want 1500", poolSize)
	}

	if releaseTimeout, ok := poolSettings["release_timeout"].(time.Duration); !ok || releaseTimeout != 8*time.Second {
		t.Errorf("ApplyOverrides() pool.release_timeout = %v, want 8s", releaseTimeout)
	}

	// Check reader overrides
	subSettings := result.Readers["sub"].Settings.(map[string]any)
	if maxInFlight, ok := subSettings["max_in_flight"].(int); !ok || maxInFlight != 100 {
		t.Errorf("ApplyOverrides() max_in_flight = %v, want 100", maxInFlight)
	}
}

func TestApplyOverrides_NSQConverterValidation(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "nsq",
				Settings: map[string]any{
					"address": "localhost:4150",
					"topic":   "test-topic",
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "nsq",
				Settings: map[string]any{
					"addresses": []string{"localhost:4150"},
					"topic":     "test-topic",
					"channel":   "test-channel",
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
			name:    "invalid pool size (zero)",
			path:    "writer.pub.pool.size",
			value:   "0",
			wantErr: true,
			errMsg:  "must be positive",
		},
		{
			name:    "invalid pool size (negative)",
			path:    "writer.pub.pool.size",
			value:   "-100",
			wantErr: true,
			errMsg:  "must be positive",
		},
		{
			name:    "invalid duration format",
			path:    "writer.pub.pool.release_timeout",
			value:   "invalid-duration",
			wantErr: true,
			errMsg:  "invalid duration format",
		},
		{
			name:    "invalid max_in_flight (zero)",
			path:    "reader.sub.max_in_flight",
			value:   "0",
			wantErr: true,
			errMsg:  "must be positive",
		},
		{
			name:    "address forbidden",
			path:    "writer.pub.address",
			value:   "other:4150",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
		{
			name:    "topic forbidden",
			path:    "writer.pub.topic",
			value:   "another-topic",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
		{
			name:    "valid pool size",
			path:    "writer.pub.pool.size",
			value:   "1000",
			wantErr: false,
		},
		{
			name:    "valid max_in_flight",
			path:    "reader.sub.max_in_flight",
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
