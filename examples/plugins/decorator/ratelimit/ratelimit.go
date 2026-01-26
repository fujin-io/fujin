// Package ratelimit provides an example rate limiting decorator.
// This demonstrates how to create a custom decorator plugin.
//
// To use this decorator:
// 1. Import this package in your main.go:
//
//	import _ "github.com/fujin-io/fujin/examples/plugins/decorator/ratelimit"
//
// 2. Add it to your connector config:
//
//	connectors:
//	  my_connector:
//	    protocol: kafka
//	    decorators:
//	      - name: ratelimit
//	        config:
//	          requests_per_second: 1000
package ratelimit

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/decorator"
)

// Config for rate limit decorator
type Config struct {
	RequestsPerSecond int `yaml:"requests_per_second"`
}

func init() {
	fmt.Println("INIT")
	_ = decorator.Register("ratelimit", func(config any, l *slog.Logger) (decorator.Decorator, error) {
		cfg := Config{
			RequestsPerSecond: 1000, // default
		}
		// Parse config if provided
		if m, ok := config.(map[string]any); ok {
			if rps, exists := m["requests_per_second"]; exists {
				if v, ok := rps.(int); ok {
					cfg.RequestsPerSecond = v
				}
			}
		}

		interval := time.Second / time.Duration(cfg.RequestsPerSecond)
		return &rateLimitDecorator{
			interval: interval,
			l:        l,
		}, nil
	})
}

type rateLimitDecorator struct {
	interval time.Duration
	l        *slog.Logger
}

func (d *rateLimitDecorator) WrapWriter(w connector.Writer, connectorName string) connector.Writer {
	return &rateLimitWriter{
		w:        w,
		interval: d.interval,
	}
}

func (d *rateLimitDecorator) WrapReader(r connector.Reader, connectorName string) connector.Reader {
	// Rate limiting on reader is typically not needed
	return r
}

type rateLimitWriter struct {
	w          connector.Writer
	interval   time.Duration
	lastAccess time.Time
}

func (w *rateLimitWriter) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	w.rateLimit()
	w.w.Produce(ctx, msg, callback)
}

func (w *rateLimitWriter) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	w.rateLimit()
	w.w.HProduce(ctx, msg, headers, callback)
}

func (w *rateLimitWriter) Flush(ctx context.Context) error {
	return w.w.Flush(ctx)
}

func (w *rateLimitWriter) BeginTx(ctx context.Context) error {
	return w.w.BeginTx(ctx)
}

func (w *rateLimitWriter) CommitTx(ctx context.Context) error {
	return w.w.CommitTx(ctx)
}

func (w *rateLimitWriter) RollbackTx(ctx context.Context) error {
	return w.w.RollbackTx(ctx)
}

func (w *rateLimitWriter) rateLimit() {
	elapsed := time.Since(w.lastAccess)
	if elapsed < w.interval {
		time.Sleep(w.interval - elapsed)
	}
	w.lastAccess = time.Now()
}
