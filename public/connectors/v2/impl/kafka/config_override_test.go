//go:build kafka

package kafka

import (
	"testing"
	"time"

	internal_connectors "github.com/fujin-io/fujin/internal/connectors"
	public_connectors "github.com/fujin-io/fujin/public/connectors"
	reader_config "github.com/fujin-io/fujin/public/connectors/reader/config"
	writer_config "github.com/fujin-io/fujin/public/connectors/writer/config"
)

func TestApplyOverrides_WithKafkaConverter(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "kafka",
				Settings: map[string]any{
					"brokers": []string{"localhost:9092"},
					"topic":   "test-topic",
				},
			},
		},
		Readers: map[string]reader_config.Config{
			"sub": {
				Protocol: "kafka",
				Settings: map[string]any{
					"brokers": []string{"localhost:9092"},
					"topic":   "test-topic",
					"group":   "test-group",
				},
			},
		},
	}

	overrides := map[string]string{
		"writer.pub.transactional_id":          "my-tx-id-12345",
		"writer.pub.linger":                    "50ms",
		"writer.pub.max_buffered_records":      "1000",
		"writer.pub.allow_auto_topic_creation": "true",
		"reader.sub.max_poll_records":          "500",
		"reader.sub.auto_commit_interval":      "5s",
	}

	result, err := internal_connectors.ApplyOverrides(baseConfig, overrides)
	if err != nil {
		t.Fatalf("ApplyOverrides() error = %v", err)
	}

	// Check writer overrides
	pubSettings := result.Writers["pub"].Settings.(map[string]any)
	if pubSettings["transactional_id"] != "my-tx-id-12345" {
		t.Errorf("ApplyOverrides() transactional_id = %v, want my-tx-id-12345", pubSettings["transactional_id"])
	}

	// Check that linger was converted to duration
	linger, ok := pubSettings["linger"].(time.Duration)
	if !ok {
		t.Errorf("ApplyOverrides() linger type = %T, want time.Duration", pubSettings["linger"])
	} else if linger != 50*time.Millisecond {
		t.Errorf("ApplyOverrides() linger = %v, want 50ms", linger)
	}

	// Check max_buffered_records was converted to int
	maxBuffered, ok := pubSettings["max_buffered_records"].(int)
	if !ok {
		t.Errorf("ApplyOverrides() max_buffered_records type = %T, want int", pubSettings["max_buffered_records"])
	} else if maxBuffered != 1000 {
		t.Errorf("ApplyOverrides() max_buffered_records = %v, want 1000", maxBuffered)
	}

	// Check allow_auto_topic_creation was converted to bool
	allowAuto, ok := pubSettings["allow_auto_topic_creation"].(bool)
	if !ok {
		t.Errorf("ApplyOverrides() allow_auto_topic_creation type = %T, want bool", pubSettings["allow_auto_topic_creation"])
	} else if allowAuto != true {
		t.Errorf("ApplyOverrides() allow_auto_topic_creation = %v, want true", allowAuto)
	}

	// Check reader overrides
	subSettings := result.Readers["sub"].Settings.(map[string]any)
	// Note: group is not in allowedReaderSettings, so it cannot be overridden
	// It should remain as the original value from baseConfig
	if subSettings["group"] != "test-group" {
		t.Errorf("ApplyOverrides() group = %v, want test-group (should not be overridden)", subSettings["group"])
	}

	// Check max_poll_records was converted to int
	maxPoll, ok := subSettings["max_poll_records"].(int)
	if !ok {
		t.Errorf("ApplyOverrides() max_poll_records type = %T, want int", subSettings["max_poll_records"])
	} else if maxPoll != 500 {
		t.Errorf("ApplyOverrides() max_poll_records = %v, want 500", maxPoll)
	}

	// Check auto_commit_interval was converted to duration
	autoCommitInterval, ok := subSettings["auto_commit_interval"].(time.Duration)
	if !ok {
		t.Errorf("ApplyOverrides() auto_commit_interval type = %T, want time.Duration", subSettings["auto_commit_interval"])
	} else if autoCommitInterval != 5*time.Second {
		t.Errorf("ApplyOverrides() auto_commit_interval = %v, want 5s", autoCommitInterval)
	}

	// Check that original config was not modified
	if baseConfig.Writers["pub"].Settings.(map[string]any)["transactional_id"] != nil {
		t.Error("ApplyOverrides() modified original config")
	}
}

func TestApplyOverrides_KafkaConverterValidation(t *testing.T) {
	baseConfig := public_connectors.Config{
		Writers: map[string]writer_config.Config{
			"pub": {
				Protocol: "kafka",
				Settings: map[string]any{
					"brokers": []string{"localhost:9092"},
					"topic":   "test-topic",
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
			path:    "writer.pub.linger",
			value:   "invalid-duration",
			wantErr: true,
			errMsg:  "invalid duration format",
		},
		{
			name:    "ping_timeout forbidden (negative value)",
			path:    "writer.pub.ping_timeout",
			value:   "-1s",
			wantErr: true,
			errMsg:  "cannot be overridden",
		},
		{
			name:    "invalid integer",
			path:    "writer.pub.max_buffered_records",
			value:   "not-a-number",
			wantErr: true,
			errMsg:  "invalid integer format",
		},
		{
			name:    "negative integer",
			path:    "writer.pub.max_buffered_records",
			value:   "-100",
			wantErr: true,
			errMsg:  "must be non-negative",
		},
		{
			name:    "invalid boolean",
			path:    "writer.pub.allow_auto_topic_creation",
			value:   "yes",
			wantErr: true,
			errMsg:  "invalid boolean format",
		},
		{
			name:    "empty transactional_id",
			path:    "writer.pub.transactional_id",
			value:   "",
			wantErr: true,
			errMsg:  "cannot be empty",
		},
		{
			name:    "brokers forbidden",
			path:    "writer.pub.brokers",
			value:   "localhost:9093",
			wantErr: true,
			errMsg:  "cannot be overridden",
		},
		{
			name:    "topic forbidden",
			path:    "writer.pub.topic",
			value:   "another-topic",
			wantErr: true,
			errMsg:  "cannot be overridden",
		},
		{
			name:    "tls forbidden",
			path:    "writer.pub.tls",
			value:   "enabled",
			wantErr: true,
			errMsg:  "cannot be overridden",
		},
		{
			name:    "ping_timeout forbidden",
			path:    "writer.pub.ping_timeout",
			value:   "10s",
			wantErr: true,
			errMsg:  "cannot be overridden",
		},
		{
			name:    "disable_idempotent_write forbidden",
			path:    "writer.pub.disable_idempotent_write",
			value:   "false",
			wantErr: true,
			errMsg:  "cannot be overridden",
		},
		{
			name:    "valid values",
			path:    "writer.pub.transactional_id",
			value:   "my-tx-id",
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
