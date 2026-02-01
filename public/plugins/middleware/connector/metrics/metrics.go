// Package metrics provides a Prometheus metrics connector for connectors.
// Import this package to enable metrics collection:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/middleware/connector/metrics"
//
// Configure in YAML:
//
//	connector_middlewares:
//	  - name: metrics
//	    config:
//	      enabled: true
//	      addr: ":9090"      # HTTP server address for /metrics endpoint
//	      path: "/metrics"   # Metrics endpoint path
//
// Only one http server will be started to serve metrics.
package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/fujin-io/fujin/public/plugins/connector"
	cmv "github.com/fujin-io/fujin/public/plugins/middleware/connector"
)

var (
	once sync.Once

	opsTotal                 *prometheus.CounterVec
	errorsTotal              *prometheus.CounterVec
	connectorWriteLatencySec *prometheus.HistogramVec

	httpSrv     *http.Server
	httpSrvOnce sync.Once
)

// Config for metrics connector
type Config struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"` // HTTP server address (e.g., ":9090")
	Path    string `yaml:"path"` // Metrics endpoint path (default: "/metrics")
}

func init() {
	if err := cmv.Register("metrics", newMetricsMiddleware); err != nil {
		panic(fmt.Sprintf("register metrics connector: %v", err))
	}
}

func newMetricsMiddleware(config any, l *slog.Logger) (cmv.Middleware, error) {
	cfg := Config{
		Enabled: true,
		Addr:    ":9090",    // default address
		Path:    "/metrics", // default path
	}

	// Parse config if provided
	if m, ok := config.(map[string]any); ok {
		if enabled, exists := m["enabled"]; exists {
			if v, ok := enabled.(bool); ok {
				cfg.Enabled = v
			}
		}
		if addr, exists := m["addr"]; exists {
			if v, ok := addr.(string); ok {
				cfg.Addr = v
			}
		}
		if path, exists := m["path"]; exists {
			if v, ok := path.(string); ok {
				cfg.Path = v
			}
		}
	}

	if cfg.Enabled {
		initMetrics()
		initHTTPServer(cfg.Addr, cfg.Path, l)
	}

	return &metricsMiddleware{enabled: cfg.Enabled, l: l}, nil
}

func initMetrics() {
	once.Do(func() {
		opsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fujin_ops_total",
			Help: "Number of protocol operations",
		}, []string{"opcode", "connector"})

		errorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fujin_errors_total",
			Help: "Errors by stage and connector",
		}, []string{"stage", "connector"})

		connectorWriteLatencySec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fujin_connector_produce_latency_seconds",
			Help:    "Connector produce latency",
			Buckets: prometheus.DefBuckets,
		}, []string{"connector"})

		prometheus.MustRegister(opsTotal, errorsTotal, connectorWriteLatencySec)
	})
}

// initHTTPServer starts the HTTP server for metrics endpoint (globally, once)
func initHTTPServer(addr, path string, l *slog.Logger) {
	httpSrvOnce.Do(func() {
		if path == "" {
			path = "/metrics"
		}
		mux := http.NewServeMux()
		mux.Handle(path, promhttp.Handler())
		httpSrv = &http.Server{Addr: addr, Handler: mux}
		go func() {
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				l.Error("metrics http server", "err", err)
			}
		}()
		l.Info("metrics server started", "addr", addr, "path", path)
	})
}

// Init initializes the metrics HTTP server globally.
// This should be called once at server startup.
// Config can be passed from YAML observability.metrics section.
func Init(ctx context.Context, addr, path string, l *slog.Logger) error {
	if path == "" {
		path = "/metrics"
	}
	initMetrics()
	initHTTPServer(addr, path, l)
	return nil
}

// Shutdown gracefully shuts down the metrics HTTP server
func Shutdown(ctx context.Context) error {
	if httpSrv != nil {
		return httpSrv.Shutdown(ctx)
	}
	return nil
}

// IncOp increments the operations counter (exported for compatibility with v1 connectors)
func IncOp(opcode, connector string) {
	if opsTotal != nil {
		opsTotal.WithLabelValues(opcode, connector).Inc()
	}
}

// IncError increments the errors counter (exported for compatibility with v1 connectors)
func IncError(stage, connector string) {
	if errorsTotal != nil {
		errorsTotal.WithLabelValues(stage, connector).Inc()
	}
}

// ObserveProduceLatency observes the produce latency (exported for compatibility with v1 connectors)
func ObserveProduceLatency(connector string, d time.Duration) {
	if connectorWriteLatencySec != nil {
		connectorWriteLatencySec.WithLabelValues(connector).Observe(d.Seconds())
	}
}

// metricsMiddleware implements connector.Middleware
type metricsMiddleware struct {
	enabled bool
	l       *slog.Logger
}

func (d *metricsMiddleware) WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser {
	if !d.enabled {
		return w
	}
	return &metricsWriterWrapper{w: w, connectorName: connectorName}
}

func (d *metricsMiddleware) WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser {
	if !d.enabled {
		return r
	}
	return &metricsReaderWrapper{r: r, connectorName: connectorName}
}

// Writer wrapper

type metricsWriterWrapper struct {
	w             connector.WriteCloser
	connectorName string
}

func (d *metricsWriterWrapper) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	start := time.Now()
	d.w.Produce(ctx, msg, func(err error) {
		IncOp("produce", d.connectorName)
		if err != nil {
			IncError("produce", d.connectorName)
		}
		ObserveProduceLatency(d.connectorName, time.Since(start))
		if callback != nil {
			callback(err)
		}
	})
}

func (d *metricsWriterWrapper) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	start := time.Now()
	d.w.HProduce(ctx, msg, headers, func(err error) {
		IncOp("hproduce", d.connectorName)
		if err != nil {
			IncError("hproduce", d.connectorName)
		}
		ObserveProduceLatency(d.connectorName, time.Since(start))
		if callback != nil {
			callback(err)
		}
	})
}

func (d *metricsWriterWrapper) Flush(ctx context.Context) error {
	err := d.w.Flush(ctx)
	if err != nil {
		IncError("flush", d.connectorName)
	}
	return err
}

func (d *metricsWriterWrapper) BeginTx(ctx context.Context) error {
	IncOp("begin_tx", d.connectorName)
	err := d.w.BeginTx(ctx)
	if err != nil {
		IncError("begin_tx", d.connectorName)
	}
	return err
}

func (d *metricsWriterWrapper) CommitTx(ctx context.Context) error {
	IncOp("commit_tx", d.connectorName)
	err := d.w.CommitTx(ctx)
	if err != nil {
		IncError("commit_tx", d.connectorName)
	}
	return err
}

func (d *metricsWriterWrapper) RollbackTx(ctx context.Context) error {
	IncOp("rollback_tx", d.connectorName)
	err := d.w.RollbackTx(ctx)
	if err != nil {
		IncError("rollback_tx", d.connectorName)
	}
	return err
}

func (d *metricsWriterWrapper) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = Shutdown(ctx)
	return d.w.Close()
}

// Reader wrapper

type metricsReaderWrapper struct {
	r             connector.ReadCloser
	connectorName string
}

func (d *metricsReaderWrapper) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	IncOp("subscribe", d.connectorName)
	return d.r.Subscribe(
		ctx,
		func(message []byte, topic string, args ...any) {
			IncOp("msg", d.connectorName)
			h(message, topic, args...)
		},
	)
}

func (d *metricsReaderWrapper) HSubscribe(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	IncOp("hsubscribe", d.connectorName)
	return d.r.HSubscribe(
		ctx,
		func(message []byte, topic string, hs [][]byte, args ...any) {
			IncOp("hmsg", d.connectorName)
			h(message, topic, hs, args...)
		},
	)
}

func (d *metricsReaderWrapper) Fetch(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, args ...any)) {
	frh := func(n uint32, err error) {
		IncOp("fetch", d.connectorName)
		if err != nil {
			IncError("fetch", d.connectorName)
		}
		fetchResponseHandler(n, err)
	}
	d.r.Fetch(ctx, n, frh, msgHandler)
}

func (d *metricsReaderWrapper) HFetch(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, hs [][]byte, args ...any)) {
	frh := func(n uint32, err error) {
		IncOp("hfetch", d.connectorName)
		if err != nil {
			IncError("hfetch", d.connectorName)
		}
		fetchResponseHandler(n, err)
	}
	d.r.HFetch(ctx, n, frh, msgHandler)
}

func (d *metricsReaderWrapper) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	d.r.Ack(
		ctx, msgIDs,
		func(err error) {
			IncOp("ack", d.connectorName)
			if err != nil {
				IncError("ack", d.connectorName)
			}
			ackHandler(err)
		},
		func(b []byte, err error) {
			if err != nil {
				IncError("ack_msg", d.connectorName)
			}
			ackMsgHandler(b, err)
		},
	)
}

func (d *metricsReaderWrapper) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	d.r.Nack(
		ctx, msgIDs,
		func(err error) {
			IncOp("nack", d.connectorName)
			if err != nil {
				IncError("nack", d.connectorName)
			}
			nackHandler(err)
		},
		func(b []byte, err error) {
			if err != nil {
				IncError("nack_msg", d.connectorName)
			}
			nackMsgHandler(b, err)
		},
	)
}

func (d *metricsReaderWrapper) MsgIDStaticArgsLen() int {
	return d.r.MsgIDStaticArgsLen()
}

func (d *metricsReaderWrapper) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return d.r.EncodeMsgID(buf, topic, args...)
}

func (d *metricsReaderWrapper) IsAutoCommit() bool {
	return d.r.IsAutoCommit()
}

func (d *metricsReaderWrapper) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = Shutdown(ctx)
	return d.r.Close()
}
