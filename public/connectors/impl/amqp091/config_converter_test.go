//go:build amqp091

package amqp091

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
			name:        "conn.heartbeat valid",
			settingPath: "conn.heartbeat",
			value:       "30s",
			wantType:    "duration",
			wantValue:   30 * time.Second,
			wantErr:     false,
		},
		{
			name:        "conn.heartbeat invalid format",
			settingPath: "conn.heartbeat",
			value:       "invalid",
			wantErr:     true,
			errMsg:      "invalid duration format",
		},
		{
			name:        "conn.heartbeat negative",
			settingPath: "conn.heartbeat",
			value:       "-10s",
			wantErr:     true,
			errMsg:      "must be non-negative",
		},
		{
			name:        "conn.channel_max valid",
			settingPath: "conn.channel_max",
			value:       "100",
			wantType:    "uint16",
			wantValue:   uint16(100),
			wantErr:     false,
		},
		{
			name:        "conn.channel_max invalid format",
			settingPath: "conn.channel_max",
			value:       "not-a-number",
			wantErr:     true,
			errMsg:      "invalid uint16 format",
		},
		{
			name:        "conn.frame_size valid",
			settingPath: "conn.frame_size",
			value:       "4096",
			wantType:    "int",
			wantValue:   4096,
			wantErr:     false,
		},
		{
			name:        "conn.frame_size invalid format",
			settingPath: "conn.frame_size",
			value:       "not-a-number",
			wantErr:     true,
			errMsg:      "invalid integer format",
		},
		{
			name:        "conn.frame_size zero",
			settingPath: "conn.frame_size",
			value:       "0",
			wantErr:     true,
			errMsg:      "must be positive",
		},
		{
			name:        "exchange.durable true",
			settingPath: "exchange.durable",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "exchange.durable false",
			settingPath: "exchange.durable",
			value:       "false",
			wantType:    "bool",
			wantValue:   false,
			wantErr:     false,
		},
		{
			name:        "exchange.auto_delete true",
			settingPath: "exchange.auto_delete",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "exchange.internal true",
			settingPath: "exchange.internal",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "queue.durable true",
			settingPath: "queue.durable",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "queue.exclusive true",
			settingPath: "queue.exclusive",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "publish.mandatory true",
			settingPath: "publish.mandatory",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "publish.immediate true",
			settingPath: "publish.immediate",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "exchange boolean invalid",
			settingPath: "exchange.durable",
			value:       "yes",
			wantErr:     true,
			errMsg:      "invalid boolean format",
		},
		// Forbidden settings tests
		{
			name:        "url forbidden",
			settingPath: "url",
			value:       "amqp://localhost:5672",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "exchange.name forbidden",
			settingPath: "exchange.name",
			value:       "my-exchange",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "queue.name forbidden",
			settingPath: "queue.name",
			value:       "my-queue",
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
			name:        "conn.tls forbidden",
			settingPath: "conn.tls.cert_path",
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

				// Check type and value
				switch tt.wantType {
				case "int":
					if i, ok := result.(int); !ok || i != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (int)", result, result, tt.wantValue)
					}
				case "bool":
					if b, ok := result.(bool); !ok || b != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (bool)", result, result, tt.wantValue)
					}
				case "uint16":
					if u, ok := result.(uint16); !ok || u != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (uint16)", result, result, tt.wantValue)
					}
				case "duration":
					if d, ok := result.(time.Duration); !ok || d != tt.wantValue {
						t.Errorf("convertWriterConfigValue() = %v (%T), want %v (time.Duration)", result, result, tt.wantValue)
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
			name:        "conn.heartbeat valid",
			settingPath: "conn.heartbeat",
			value:       "30s",
			wantType:    "duration",
			wantValue:   30 * time.Second,
			wantErr:     false,
		},
		{
			name:        "conn.channel_max valid",
			settingPath: "conn.channel_max",
			value:       "100",
			wantType:    "uint16",
			wantValue:   uint16(100),
			wantErr:     false,
		},
		{
			name:        "conn.frame_size valid",
			settingPath: "conn.frame_size",
			value:       "4096",
			wantType:    "int",
			wantValue:   4096,
			wantErr:     false,
		},
		{
			name:        "exchange.durable true",
			settingPath: "exchange.durable",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "queue.durable true",
			settingPath: "queue.durable",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "queue.exclusive true",
			settingPath: "queue.exclusive",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "consume.exclusive true",
			settingPath: "consume.exclusive",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "consume.no_local true",
			settingPath: "consume.no_local",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "ack.multiple true",
			settingPath: "ack.multiple",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "nack.multiple true",
			settingPath: "nack.multiple",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "nack.requeue true",
			settingPath: "nack.requeue",
			value:       "true",
			wantType:    "bool",
			wantValue:   true,
			wantErr:     false,
		},
		{
			name:        "consume boolean invalid",
			settingPath: "consume.exclusive",
			value:       "yes",
			wantErr:     true,
			errMsg:      "invalid boolean format",
		},
		// Forbidden settings tests
		{
			name:        "url forbidden",
			settingPath: "url",
			value:       "amqp://localhost:5672",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "exchange.name forbidden",
			settingPath: "exchange.name",
			value:       "my-exchange",
			wantErr:     true,
			errMsg:      "cannot be overridden at runtime",
		},
		{
			name:        "queue.name forbidden",
			settingPath: "queue.name",
			value:       "my-queue",
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
			name:        "conn.tls forbidden",
			settingPath: "conn.tls.cert_path",
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

				// Check type and value
				switch tt.wantType {
				case "int":
					if i, ok := result.(int); !ok || i != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (int)", result, result, tt.wantValue)
					}
				case "bool":
					if b, ok := result.(bool); !ok || b != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (bool)", result, result, tt.wantValue)
					}
				case "uint16":
					if u, ok := result.(uint16); !ok || u != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (uint16)", result, result, tt.wantValue)
					}
				case "duration":
					if d, ok := result.(time.Duration); !ok || d != tt.wantValue {
						t.Errorf("convertReaderConfigValue() = %v (%T), want %v (time.Duration)", result, result, tt.wantValue)
					}
				}
			}
		})
	}
}
