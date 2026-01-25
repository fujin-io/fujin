//go:build amqp10

package amqp10

import (
	"testing"
	"time"

	internal_connectors "github.com/fujin-io/fujin/internal/connectors"
	public_connectors "github.com/fujin-io/fujin/public/connectors"
	reader_config "github.com/fujin-io/fujin/public/connectors/reader/config"
	writer_config "github.com/fujin-io/fujin/public/connectors/writer/config"
)

func TestApplyOverrides_WithAMQP10Converter(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "amqp10",
				Settings: map[string]any{
					"conn": map[string]any{
						"addr": "amqp://localhost:5672",
					},
					"sender": map[string]any{
						"target": "test-target",
					},
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "amqp10",
				Settings: map[string]any{
					"conn": map[string]any{
						"addr": "amqp://localhost:5672",
					},
					"receiver": map[string]any{
						"source": "test-source",
					},
				},
			},
		},
	}

	overrides := map[string]string{
		"writer.pub.conn.idle_timeout":   "30s",
		"writer.pub.conn.max_frame_size": "16384",
		"writer.pub.conn.write_timeout":  "5s",
		"writer.pub.send.settled":        "true",
		"reader.sub.conn.idle_timeout":   "45s",
		"reader.sub.receiver.credit":     "200",
	}

	result, err := internal_connectors.ApplyOverrides(baseConfig, overrides)
	if err != nil {
		t.Fatalf("ApplyOverrides() error = %v", err)
	}

	// Check writer overrides
	pubSettings := result.Writers["pub"].Settings.(map[string]any)
	connSettings := pubSettings["conn"].(map[string]any)
	if idleTimeout, ok := connSettings["idle_timeout"].(time.Duration); !ok || idleTimeout != 30*time.Second {
		t.Errorf("ApplyOverrides() conn.idle_timeout = %v, want 30s", idleTimeout)
	}

	if maxFrameSize, ok := connSettings["max_frame_size"].(uint32); !ok || maxFrameSize != 16384 {
		t.Errorf("ApplyOverrides() conn.max_frame_size = %v, want 16384", maxFrameSize)
	}

	if writeTimeout, ok := connSettings["write_timeout"].(time.Duration); !ok || writeTimeout != 5*time.Second {
		t.Errorf("ApplyOverrides() conn.write_timeout = %v, want 5s", writeTimeout)
	}

	sendSettings := pubSettings["send"].(map[string]any)
	if settled, ok := sendSettings["settled"].(bool); !ok || settled != true {
		t.Errorf("ApplyOverrides() send.settled = %v, want true", settled)
	}

	// Check reader overrides
	subSettings := result.Readers["sub"].Settings.(map[string]any)
	subConnSettings := subSettings["conn"].(map[string]any)
	if idleTimeout, ok := subConnSettings["idle_timeout"].(time.Duration); !ok || idleTimeout != 45*time.Second {
		t.Errorf("ApplyOverrides() conn.idle_timeout = %v, want 45s", idleTimeout)
	}

	receiverSettings := subSettings["receiver"].(map[string]any)
	if credit, ok := receiverSettings["credit"].(int32); !ok || credit != 200 {
		t.Errorf("ApplyOverrides() receiver.credit = %v, want 200", credit)
	}
}

func TestApplyOverrides_AMQP10ConverterValidation(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "amqp10",
				Settings: map[string]any{
					"conn": map[string]any{
						"addr": "amqp://localhost:5672",
					},
					"sender": map[string]any{
						"target": "test-target",
					},
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "amqp10",
				Settings: map[string]any{
					"conn": map[string]any{
						"addr": "amqp://localhost:5672",
					},
					"receiver": map[string]any{
						"source": "test-source",
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
			name:    "invalid duration format",
			path:    "writer.pub.conn.idle_timeout",
			value:   "invalid-duration",
			wantErr: true,
			errMsg:  "invalid duration format",
		},
		{
			name:    "negative duration",
			path:    "writer.pub.conn.idle_timeout",
			value:   "-10s",
			wantErr: true,
			errMsg:  "must be non-negative",
		},
		{
			name:    "invalid max_frame_size (zero)",
			path:    "writer.pub.conn.max_frame_size",
			value:   "0",
			wantErr: true,
			errMsg:  "must be positive",
		},
		{
			name:    "invalid max_frame_size (too large)",
			path:    "writer.pub.conn.max_frame_size",
			value:   "5000000000",
			wantErr: true,
			errMsg:  "invalid uint32 format",
		},
		{
			name:    "invalid receiver.credit (zero)",
			path:    "reader.sub.receiver.credit",
			value:   "0",
			wantErr: true,
			errMsg:  "must be positive",
		},
		{
			name:    "invalid receiver.credit (negative)",
			path:    "reader.sub.receiver.credit",
			value:   "-100",
			wantErr: true,
			errMsg:  "must be positive",
		},
		{
			name:    "invalid boolean",
			path:    "writer.pub.send.settled",
			value:   "yes",
			wantErr: true,
			errMsg:  "invalid boolean format",
		},
		{
			name:    "conn.addr forbidden",
			path:    "writer.pub.conn.addr",
			value:   "amqp://other:5672",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
		{
			name:    "sender.target forbidden",
			path:    "writer.pub.sender.target",
			value:   "other-target",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
		{
			name:    "valid values",
			path:    "writer.pub.conn.idle_timeout",
			value:   "60s",
			wantErr: false,
		},
		{
			name:    "valid receiver.credit",
			path:    "reader.sub.receiver.credit",
			value:   "100",
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
