//go:build kafka

package kafka

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
		wantType    string
		wantValue   any
		wantErr     bool
		errMsg      string
	}{
		{
			name:        "transactional_id valid",
			settingPath: "transactional_id",
			value:       "my-tx-id-12345",
			wantType:    "string",
			wantValue:   "my-tx-id-12345",
			wantErr:     false,
		},
		{
			name:        "transactional_id empty",
			settingPath: "transactional_id",
			value:       "",
			wantErr:     true,
			errMsg:      "cannot be empty",
		},
		{
			name:        "linger valid",
			settingPath: "linger",
			value:       "50ms",
			wantType:    "duration",
			wantValue:   50 * time.Millisecond,
			wantErr:     false,
		},
		{
			name:        "linger invalid format",
			settingPath: "linger",
			value:       "invalid-duration",
			wantErr:     true,
			errMsg:      "invalid duration format",
		},
		{
			name:        "max_buffered_records valid",
			settingPath: "max_buffered_records",
			value:       "1000",
			wantType:    "int",
			wantValue:   1000,
			wantErr:     false,
		},
		{
			name:        "max_buffered_records invalid",
			settingPath: "max_buffered_records",
			value:       "not-a-number",
			wantErr:     true,
			errMsg:      "invalid integer format",
		},
		{
			name:        "max_buffered_records negative",
			settingPath: "max_buffered_records",
			value:       "-100",
			wantErr:     true,
			errMsg:      "must be non-negative",
		},
		{
			name:        "allow_auto_topic_creation true",
			settingPath: "allow_auto_topic_creation",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "allow_auto_topic_creation invalid",
			settingPath: "allow_auto_topic_creation",
			value:       "yes",
			wantErr:     true,
			errMsg:      "invalid boolean format",
		},
		// Forbidden settings tests
		{
			name:        "brokers forbidden",
			settingPath: "brokers",
			value:       "localhost:9092",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "topic forbidden",
			settingPath: "topic",
			value:       "my-topic",
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
		{
			name:        "ping_timeout forbidden",
			settingPath: "ping_timeout",
			value:       "5s",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "disable_idempotent_write forbidden",
			settingPath: "disable_idempotent_write",
			value:       "false",
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

				// Check type and value
				switch tt.wantType {
				case "string":
					if s, ok := result.(string); !ok || s != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (string)", result, result, tt.wantValue)
					}
				case "int":
					if i, ok := result.(int); !ok || i != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (int)", result, result, tt.wantValue)
					}
				case "bool":
					if b, ok := result.(bool); !ok || b != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (bool)", result, result, tt.wantValue)
					}
				case "duration":
					if d, ok := result.(time.Duration); !ok || d != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (time.Duration)", result, result, tt.wantValue)
					}
				case "[]string":
					if arr, ok := result.([]string); !ok {
						t.Errorf("convertWriterConfigValue() = %v (%T), want []string", result, result)
					} else {
						wantArr := tt.wantValue.([]string)
						if len(arr) != len(wantArr) {
							t.Errorf("convertWriterConfigValue() []string length = %d, want %d", len(arr), len(wantArr))
						} else {
							for i, v := range arr {
								if v != wantArr[i] {
									t.Errorf("convertWriterConfigValue() []string[%d] = %v, want %v", i, v, wantArr[i])
								}
							}
						}
					}
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
			name:        "max_poll_records valid",
			settingPath: "max_poll_records",
			value:       "500",
			wantType:    "int",
			wantValue:   500,
			wantErr:     false,
		},
		{
			name:        "max_poll_records invalid",
			settingPath: "max_poll_records",
			value:       "not-a-number",
			wantErr:     true,
			errMsg:      "invalid integer format",
		},
		{
			name:        "max_poll_records zero",
			settingPath: "max_poll_records",
			value:       "0",
			wantErr:     true,
			errMsg:      "must be positive",
		},
		{
			name:        "auto_commit_interval valid",
			settingPath: "auto_commit_interval",
			value:       "5s",
			wantType:    "duration",
			wantValue:   5 * time.Second,
			wantErr:     false,
		},
		{
			name:        "auto_commit_interval invalid",
			settingPath: "auto_commit_interval",
			value:       "invalid",
			wantErr:     true,
			errMsg:      "invalid duration format",
		},
		{
			name:        "auto_commit_marks true",
			settingPath: "auto_commit_marks",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "balancers valid",
			settingPath: "balancers",
			value:       "sticky,round_robin",
			wantType:    "[]Balancer",
			wantValue:   []Balancer{BalancerSticky, BalancerRoundRobin},
			wantErr:     false,
		},
		{
			name:        "balancers invalid",
			settingPath: "balancers",
			value:       "invalid",
			wantErr:     true,
			errMsg:      "invalid balancer",
		},
		// Forbidden settings tests
		{
			name:        "brokers forbidden",
			settingPath: "brokers",
			value:       "localhost:9092",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "topic forbidden",
			settingPath: "topic",
			value:       "my-topic",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "group forbidden",
			settingPath: "group",
			value:       "my-group",
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
		{
			name:        "ping_timeout forbidden",
			settingPath: "ping_timeout",
			value:       "5s",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "fetch_isolation_level forbidden",
			settingPath: "fetch_isolation_level",
			value:       "read_commited",
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
				case "string":
					if s, ok := result.(string); !ok || s != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (string)", result, result, tt.wantValue)
					}
				case "int":
					if i, ok := result.(int); !ok || i != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (int)", result, result, tt.wantValue)
					}
				case "bool":
					if b, ok := result.(bool); !ok || b != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (bool)", result, result, tt.wantValue)
					}
				case "duration":
					if d, ok := result.(time.Duration); !ok || d != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (time.Duration)", result, result, tt.wantValue)
					}
				case "IsolationLevel":
					if level, ok := result.(IsolationLevel); !ok || level != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (IsolationLevel)", result, result, tt.wantValue)
					}
				case "[]Balancer":
					if arr, ok := result.([]Balancer); !ok {
						t.Errorf("convertReaderConfigValue() = %v (%T), want []Balancer", result, result)
					} else {
						wantArr := tt.wantValue.([]Balancer)
						if len(arr) != len(wantArr) {
							t.Errorf("convertReaderConfigValue() []Balancer length = %d, want %d", len(arr), len(wantArr))
						} else {
							for i, v := range arr {
								if v != wantArr[i] {
									t.Errorf("convertReaderConfigValue() []Balancer[%d] = %v, want %v", i, v, wantArr[i])
								}
							}
						}
					}
				}
			}
		})
	}
}
