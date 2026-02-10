// Package ratelimit provides an example rate limiting connector middleware.
// This demonstrates how to create a custom connector middleware plugin.
//
// To use this connector middleware:
// 1. Import this package in your main.go:
//
//	import _ "github.com/fujin-io/fujin/examples/plugins/middleware/connector/ratelimit"
//
// 2. Add it to your connector config:
//
//	connectors:
//	  my_connector:
//	    protocol: kafka
//	    connector_middlewares:
//	      - name: ratelimit
//	        config:
//	          requests_per_second: 1000
package ratelimit

import (
	"context"
	"log/slog"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
)

// Config for rate limit connector middleware
type Config struct {
	RequestsPerSecond int `yaml:"requests_per_second"`
}

func init() {
	_ = cmw.Register("ratelimit", func(config any, l *slog.Logger) (cmw.Middleware, error) {
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
		return &rateLimitMiddleware{
			interval: interval,
			l:        l,
		}, nil
	})
}

type rateLimitMiddleware struct {
	interval time.Duration
	l        *slog.Logger
}

func (d *rateLimitMiddleware) WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser {
	return &rateLimitWriter{
		w:        w,
		interval: d.interval,
	}
}

func (d *rateLimitMiddleware) WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser {
	// Rate limiting on reader is typically not needed
	return r
}

type rateLimitWriter struct {
	w          connector.WriteCloser
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

func (w *rateLimitWriter) Close() error {
	return nil
}

func (w *rateLimitWriter) rateLimit() {
	elapsed := time.Since(w.lastAccess)
	if elapsed < w.interval {
		time.Sleep(w.interval - elapsed)
	}
	w.lastAccess = time.Now()
}
