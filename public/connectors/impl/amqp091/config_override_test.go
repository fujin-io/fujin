//go:build amqp091

package amqp091

import (
	"testing"
	"time"

	internal_connectors "github.com/fujin-io/fujin/internal/connectors"
	public_connectors "github.com/fujin-io/fujin/public/connectors"
	reader_config "github.com/fujin-io/fujin/public/connectors/reader/config"
	writer_config "github.com/fujin-io/fujin/public/connectors/writer/config"
)

func TestApplyOverrides_WithAMQP091Converter(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "amqp091",
				Settings: map[string]any{
					"conn": map[string]any{
						"url": "amqp://localhost:5672",
					},
					"exchange": map[string]any{
						"name": "test-exchange",
						"kind": "direct",
					},
					"queue": map[string]any{
						"name": "test-queue",
					},
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "amqp091",
				Settings: map[string]any{
					"conn": map[string]any{
						"url": "amqp://localhost:5672",
					},
					"exchange": map[string]any{
						"name": "test-exchange",
						"kind": "direct",
					},
					"queue": map[string]any{
						"name": "test-queue",
					},
				},
			},
		},
	}

	overrides := map[string]string{
		"writer.pub.conn.heartbeat":       "10s",
		"writer.pub.conn.channel_max":     "100",
		"writer.pub.conn.frame_size":      "4096",
		"writer.pub.exchange.durable":     "true",
		"writer.pub.exchange.auto_delete": "false",
		"writer.pub.queue.durable":        "true",
		"writer.pub.publish.mandatory":    "false",
		"reader.sub.conn.heartbeat":       "15s",
		"reader.sub.queue.exclusive":      "false",
		"reader.sub.consume.exclusive":    "false",
		"reader.sub.ack.multiple":         "true",
		"reader.sub.nack.requeue":         "true",
	}

	result, err := internal_connectors.ApplyOverrides(baseConfig, overrides)
	if err != nil {
		t.Fatalf("ApplyOverrides() error = %v", err)
	}

	// Check writer overrides
	pubSettings := result.Writers["pub"].Settings.(map[string]any)
	connSettings := pubSettings["conn"].(map[string]any)
	if heartbeat, ok := connSettings["heartbeat"].(time.Duration); !ok || heartbeat != 10*time.Second {
		t.Errorf("ApplyOverrides() conn.heartbeat = %v, want 10s", heartbeat)
	}

	if channelMax, ok := connSettings["channel_max"].(uint16); !ok || channelMax != 100 {
		t.Errorf("ApplyOverrides() conn.channel_max = %v, want 100", channelMax)
	}

	if frameSize, ok := connSettings["frame_size"].(int); !ok || frameSize != 4096 {
		t.Errorf("ApplyOverrides() conn.frame_size = %v, want 4096", frameSize)
	}

	exchangeSettings := pubSettings["exchange"].(map[string]any)
	if durable, ok := exchangeSettings["durable"].(bool); !ok || durable != true {
		t.Errorf("ApplyOverrides() exchange.durable = %v, want true", durable)
	}

	queueSettings := pubSettings["queue"].(map[string]any)
	if durable, ok := queueSettings["durable"].(bool); !ok || durable != true {
		t.Errorf("ApplyOverrides() queue.durable = %v, want true", durable)
	}

	publishSettings := pubSettings["publish"].(map[string]any)
	if mandatory, ok := publishSettings["mandatory"].(bool); !ok || mandatory != false {
		t.Errorf("ApplyOverrides() publish.mandatory = %v, want false", mandatory)
	}

	// Check reader overrides
	subSettings := result.Readers["sub"].Settings.(map[string]any)
	subConnSettings := subSettings["conn"].(map[string]any)
	if heartbeat, ok := subConnSettings["heartbeat"].(time.Duration); !ok || heartbeat != 15*time.Second {
		t.Errorf("ApplyOverrides() conn.heartbeat = %v, want 15s", heartbeat)
	}

	subQueueSettings := subSettings["queue"].(map[string]any)
	if exclusive, ok := subQueueSettings["exclusive"].(bool); !ok || exclusive != false {
		t.Errorf("ApplyOverrides() queue.exclusive = %v, want false", exclusive)
	}

	consumeSettings := subSettings["consume"].(map[string]any)
	if exclusive, ok := consumeSettings["exclusive"].(bool); !ok || exclusive != false {
		t.Errorf("ApplyOverrides() consume.exclusive = %v, want false", exclusive)
	}

	ackSettings := subSettings["ack"].(map[string]any)
	if multiple, ok := ackSettings["multiple"].(bool); !ok || multiple != true {
		t.Errorf("ApplyOverrides() ack.multiple = %v, want true", multiple)
	}

	nackSettings := subSettings["nack"].(map[string]any)
	if requeue, ok := nackSettings["requeue"].(bool); !ok || requeue != true {
		t.Errorf("ApplyOverrides() nack.requeue = %v, want true", requeue)
	}
}

func TestApplyOverrides_AMQP091ConverterValidation(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "amqp091",
				Settings: map[string]any{
					"conn": map[string]any{
						"url": "amqp://localhost:5672",
					},
					"exchange": map[string]any{
						"name": "test-exchange",
					},
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "amqp091",
				Settings: map[string]any{
					"conn": map[string]any{
						"url": "amqp://localhost:5672",
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
			path:    "writer.pub.conn.heartbeat",
			value:   "invalid-duration",
			wantErr: true,
			errMsg:  "invalid duration format",
		},
		{
			name:    "negative duration",
			path:    "writer.pub.conn.heartbeat",
			value:   "-10s",
			wantErr: true,
			errMsg:  "must be non-negative",
		},
		{
			name:    "invalid channel_max (too large)",
			path:    "writer.pub.conn.channel_max",
			value:   "70000",
			wantErr: true,
			errMsg:  "invalid uint16 format",
		},
		{
			name:    "invalid frame_size (zero)",
			path:    "writer.pub.conn.frame_size",
			value:   "0",
			wantErr: true,
			errMsg:  "must be positive",
		},
		{
			name:    "invalid boolean",
			path:    "writer.pub.exchange.durable",
			value:   "yes",
			wantErr: true,
			errMsg:  "invalid boolean format",
		},
		{
			name:    "conn.url forbidden",
			path:    "writer.pub.conn.url",
			value:   "amqp://other:5672",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
		{
			name:    "exchange.name forbidden",
			path:    "writer.pub.exchange.name",
			value:   "other-exchange",
			wantErr: true,
			errMsg:  "cannot be overridden at runtime",
		},
		{
			name:    "valid values",
			path:    "writer.pub.conn.heartbeat",
			value:   "30s",
			wantErr: false,
		},
		{
			name:    "valid reader ack.multiple",
			path:    "reader.sub.ack.multiple",
			value:   "true",
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
