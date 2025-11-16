//go:build mqtt

package mqtt

import (
	"testing"
	"time"

	internal_connectors "github.com/fujin-io/fujin/internal/connectors"
	public_connectors "github.com/fujin-io/fujin/public/connectors"
	reader_config "github.com/fujin-io/fujin/public/connectors/reader/config"
	writer_config "github.com/fujin-io/fujin/public/connectors/writer/config"
)

func TestApplyOverrides_WithMQTTConverter(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "mqtt",
				Settings: map[string]any{
					"broker_url": "tcp://localhost:1883",
					"client_id":  "test-client",
					"topic":      "test-topic",
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "mqtt",
				Settings: map[string]any{
					"broker_url": "tcp://localhost:1883",
					"client_id":  "test-client",
					"topic":      "test-topic",
				},
			},
		},
	}

	overrides := map[string]string{
		"writer.pub.qos":                  "2",
		"writer.pub.retain":               "true",
		"writer.pub.keep_alive":           "30s",
		"writer.pub.disconnect_timeout":   "5s",
		"writer.pub.pool.size":            "2000",
		"writer.pub.pool.release_timeout": "10s",
		"reader.sub.qos":                  "1",
		"reader.sub.retain":               "false",
		"reader.sub.keep_alive":           "60s",
		"reader.sub.clean_session":        "true",
	}

	result, err := internal_connectors.ApplyOverrides(baseConfig, overrides)
	if err != nil {
		t.Fatalf("ApplyOverrides() error = %v", err)
	}

	// Check writer overrides
	pubSettings := result.Writers["pub"].Settings.(map[string]any)
	if qos, ok := pubSettings["qos"].(byte); !ok || qos != 2 {
		t.Errorf("ApplyOverrides() qos = %v, want 2", pubSettings["qos"])
	}

	if retain, ok := pubSettings["retain"].(bool); !ok || retain != true {
		t.Errorf("ApplyOverrides() retain = %v, want true", pubSettings["retain"])
	}

	if keepAlive, ok := pubSettings["keep_alive"].(time.Duration); !ok || keepAlive != 30*time.Second {
		t.Errorf("ApplyOverrides() keep_alive = %v, want 30s", keepAlive)
	}

	if disconnectTimeout, ok := pubSettings["disconnect_timeout"].(time.Duration); !ok || disconnectTimeout != 5*time.Second {
		t.Errorf("ApplyOverrides() disconnect_timeout = %v, want 5s", disconnectTimeout)
	}

	poolSettings := pubSettings["pool"].(map[string]any)
	if poolSize, ok := poolSettings["size"].(int); !ok || poolSize != 2000 {
		t.Errorf("ApplyOverrides() pool.size = %v, want 2000", poolSize)
	}

	if releaseTimeout, ok := poolSettings["release_timeout"].(time.Duration); !ok || releaseTimeout != 10*time.Second {
		t.Errorf("ApplyOverrides() pool.release_timeout = %v, want 10s", releaseTimeout)
	}

	// Check reader overrides
	subSettings := result.Readers["sub"].Settings.(map[string]any)
	if qos, ok := subSettings["qos"].(byte); !ok || qos != 1 {
		t.Errorf("ApplyOverrides() qos = %v, want 1", subSettings["qos"])
	}

	if retain, ok := subSettings["retain"].(bool); !ok || retain != false {
		t.Errorf("ApplyOverrides() retain = %v, want false", subSettings["retain"])
	}

	if keepAlive, ok := subSettings["keep_alive"].(time.Duration); !ok || keepAlive != 60*time.Second {
		t.Errorf("ApplyOverrides() keep_alive = %v, want 60s", keepAlive)
	}

	if cleanSession, ok := subSettings["clean_session"].(bool); !ok || cleanSession != true {
		t.Errorf("ApplyOverrides() clean_session = %v, want true", subSettings["clean_session"])
	}
}

func TestApplyOverrides_MQTTConverterValidation(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "mqtt",
				Settings: map[string]any{
					"broker_url": "tcp://localhost:1883",
					"topic":      "test-topic",
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "mqtt",
				Settings: map[string]any{
					"broker_url": "tcp://localhost:1883",
					"topic":      "test-topic",
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
			name:    "invalid QoS (too high)",
			path:    "writer.pub.qos",
			value:   "3",
			wantErr: true,
			errMsg:  "QoS must be 0, 1, or 2",
		},
		{
			name:    "invalid QoS format",
			path:    "writer.pub.qos",
			value:   "not-a-number",
			wantErr: true,
			errMsg:  "invalid QoS format",
		},
		{
			name:    "invalid duration format",
			path:    "writer.pub.keep_alive",
			value:   "invalid-duration",
			wantErr: true,
			errMsg:  "invalid duration format",
		},
		{
			name:    "negative duration",
			path:    "writer.pub.keep_alive",
			value:   "-10s",
			wantErr: true,
			errMsg:  "must be non-negative",
		},
		{
			name:    "invalid boolean",
			path:    "writer.pub.retain",
			value:   "yes",
			wantErr: true,
			errMsg:  "invalid boolean format",
		},
		{
			name:    "invalid pool size (zero)",
			path:    "writer.pub.pool.size",
			value:   "0",
			wantErr: true,
			errMsg:  "must be positive",
		},
		{
			name:    "broker_url forbidden",
			path:    "writer.pub.broker_url",
			value:   "tcp://other:1883",
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
			name:    "valid values",
			path:    "writer.pub.qos",
			value:   "1",
			wantErr: false,
		},
		{
			name:    "valid reader clean_session",
			path:    "reader.sub.clean_session",
			value:   "false",
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
