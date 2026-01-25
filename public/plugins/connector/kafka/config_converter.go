package kafka

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// // allowedWriterSettings contains settings that can be overridden at runtime
// // Only settings listed here can be dynamically configured per session
// var allowedWriterSettings = map[string]bool{
// 	"transactional_id":          true, // Transactional ID for Kafka transactions
// 	"linger":                    true, // Batching delay
// 	"max_buffered_records":      true, // Maximum buffered records
// 	"allow_auto_topic_creation": true, // Allow automatic topic creation
// }

// // allowedReaderSettings contains settings that can be overridden at runtime
// // Only settings listed here can be dynamically configured per session
// var allowedReaderSettings = map[string]bool{
// 	"max_poll_records":          true, // Maximum records per poll
// 	"auto_commit_interval":      true, // Auto-commit interval
// 	"auto_commit_marks":         true, // Auto-commit marks
// 	"balancers":                 true, // Consumer group balancers
// 	"block_rebalance_on_poll":   true, // Block rebalance on poll
// 	"allow_auto_topic_creation": true, // Allow automatic topic creation
// }

// convertConfigValue converts and validates a configuration value for Kafka
func convertConfigValue(settingPath string, value string) (any, error) {
	settingName := lastSegment(settingPath)
	switch settingName {
	case "transactional_id":
		// String value, no conversion needed, but validate it's not empty if provided
		if value == "" {
			return nil, fmt.Errorf("transactional_id cannot be empty")
		}
		return value, nil

	case "linger":
		// Duration value
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for 'linger': %w (expected format: '10ms', '5s', '1h', etc.)", err)
		}
		if d < 0 {
			return nil, fmt.Errorf("linger must be non-negative, got: %v", d)
		}
		return d, nil

	case "max_buffered_records":
		// Integer value
		i, err := strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("invalid integer format for 'max_buffered_records': %w", err)
		}
		if i < 0 {
			return nil, fmt.Errorf("max_buffered_records must be non-negative, got: %d", i)
		}
		return i, nil

	case "allow_auto_topic_creation":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for 'allow_auto_topic_creation': %w (expected 'true' or 'false')", err)
		}
		return b, nil
	case "max_poll_records":
		// Integer value
		i, err := strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("invalid integer format for 'max_poll_records': %w", err)
		}
		if i <= 0 {
			return nil, fmt.Errorf("max_poll_records must be positive, got: %d", i)
		}
		return i, nil

	case "auto_commit_interval":
		// Duration value
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for 'auto_commit_interval': %w (expected format: '10ms', '5s', '1h', etc.)", err)
		}
		if d < 0 {
			return nil, fmt.Errorf("auto_commit_interval must be non-negative, got: %v", d)
		}
		return d, nil

	case "auto_commit_marks":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for 'auto_commit_marks': %w (expected 'true' or 'false')", err)
		}
		return b, nil

	case "balancers":
		// Balancer slice (comma-separated)
		if value == "" {
			return nil, fmt.Errorf("balancers cannot be empty")
		}
		parts := strings.Split(value, ",")
		result := make([]Balancer, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			balancer := Balancer(part)
			switch balancer {
			case BalancerSticky, BalancerCooperativeSticky, BalancerRange, BalancerRoundRobin:
				result = append(result, balancer)
			default:
				return nil, fmt.Errorf("invalid balancer '%s', expected: 'sticky', 'cooperative_sticky', 'range', or 'round_robin'", part)
			}
		}
		if len(result) == 0 {
			return nil, fmt.Errorf("balancers must contain at least one valid balancer")
		}
		return result, nil

	case "block_rebalance_on_poll":
		// Boolean value
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for 'block_rebalance_on_poll': %w (expected 'true' or 'false')", err)
		}
		return b, nil

	default:
		// This should not happen if allowedWriterSettings matches the switch cases
		// If it does, it indicates a programming error
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingName)
	}
}

func lastSegment(s string) string {
	idx := strings.LastIndexByte(s, '.')
	if idx == -1 {
		return s
	}
	return s[idx+1:]
}
