// Package tracing provides an OpenTelemetry distributed tracing decorator for connectors.
// Import this package to enable distributed tracing:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/decorator/tracing"
//
// Configure in YAML:
//
//	decorators:
//	  - name: tracing
//	    config:
//	      enabled: true
package tracing

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/decorator"
)

var (
	messagingSystemFujin = semconv.MessagingSystemKey.String("fujin")
	tracerProvider       *sdktrace.TracerProvider
	tracerProviderOnce   sync.Once
)

// Config for tracing decorator
type Config struct {
	Enabled        bool    `yaml:"enabled"`
	OTLPEndpoint   string  `yaml:"otlp_endpoint"`
	Insecure       bool    `yaml:"insecure"`
	SampleRatio    float64 `yaml:"sample_ratio"`
	ServiceName    string  `yaml:"service_name"`
	ServiceVersion string  `yaml:"service_version"`
	Environment    string  `yaml:"environment"`
}

func init() {
	_ = decorator.Register("tracing", newTracingDecorator)
}

func newTracingDecorator(config any, l *slog.Logger) (decorator.Decorator, error) {
	cfg := Config{
		Enabled:        true,
		OTLPEndpoint:   "localhost:4317",
		Insecure:       true,
		SampleRatio:    0.1,
		ServiceName:    "fujin",
		ServiceVersion: "dev",
		Environment:    "dev",
	}

	// Parse config if provided
	if m, ok := config.(map[string]any); ok {
		if enabled, exists := m["enabled"]; exists {
			if v, ok := enabled.(bool); ok {
				cfg.Enabled = v
			}
		}
		if endpoint, exists := m["otlp_endpoint"]; exists {
			if v, ok := endpoint.(string); ok {
				cfg.OTLPEndpoint = v
			}
		}
		if insecure, exists := m["insecure"]; exists {
			if v, ok := insecure.(bool); ok {
				cfg.Insecure = v
			}
		}
		if ratio, exists := m["sample_ratio"]; exists {
			if v, ok := ratio.(float64); ok {
				cfg.SampleRatio = v
			}
		}
		if name, exists := m["service_name"]; exists {
			if v, ok := name.(string); ok {
				cfg.ServiceName = v
			}
		}
		if version, exists := m["service_version"]; exists {
			if v, ok := version.(string); ok {
				cfg.ServiceVersion = v
			}
		}
		if env, exists := m["environment"]; exists {
			if v, ok := env.(string); ok {
				cfg.Environment = v
			}
		}
	}

	// Tracer provider is initialized via Init() function at server startup
	// This allows using config from observability.tracing section

	return &tracingDecorator{enabled: cfg.Enabled, l: l}, nil
}

// initTracerProvider initializes the OpenTelemetry tracer provider (globally, once)
func initTracerProvider(ctx context.Context, cfg Config, l *slog.Logger) {
	tracerProviderOnce.Do(func() {
		var opts []otlptracegrpc.Option
		opts = append(opts, otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint))
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exp, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			l.Error("init otlp exporter", "err", err)
			return
		}

		sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))
		res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(
			"",
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.version", cfg.ServiceVersion),
			attribute.String("deployment.environment", cfg.Environment),
		))
		tracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithSampler(sampler),
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(tracerProvider)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
		l.Info("tracing initialized", "endpoint", cfg.OTLPEndpoint, "service", cfg.ServiceName)
	})
}

// Init initializes the OpenTelemetry tracer provider globally.
// This should be called once at server startup.
// Config can be passed from YAML observability.tracing section.
func Init(ctx context.Context, cfg Config, l *slog.Logger) error {
	if cfg.Enabled {
		initTracerProvider(ctx, cfg, l)
	}
	return nil
}

// Shutdown gracefully shuts down the tracing provider
func Shutdown(ctx context.Context) error {
	if tracerProvider != nil {
		return tracerProvider.Shutdown(ctx)
	}
	return nil
}

// tracingDecorator implements decorator.Decorator
type tracingDecorator struct {
	enabled bool
	l       *slog.Logger
}

func (d *tracingDecorator) WrapWriter(w connector.Writer, connectorName string) connector.Writer {
	if !d.enabled {
		return w
	}
	return &tracingWriterWrapper{w: w, connectorName: connectorName}
}

func (d *tracingDecorator) WrapReader(r connector.Reader, connectorName string) connector.Reader {
	if !d.enabled {
		return r
	}
	return &tracingReaderWrapper{r: r, connectorName: connectorName}
}

// Helper functions

func tracer() trace.Tracer {
	if tracerProvider != nil {
		return tracerProvider.Tracer("fujin")
	}
	return otel.Tracer("fujin")
}

func propagator() propagation.TextMapPropagator {
	return otel.GetTextMapPropagator()
}

// byteHeadersCarrier implements propagation.TextMapCarrier for [][]byte headers
type byteHeadersCarrier struct {
	hs *[][]byte
}

func newByteHeadersCarrier(hs *[][]byte) byteHeadersCarrier { return byteHeadersCarrier{hs: hs} }

func (c byteHeadersCarrier) Get(key string) string {
	if c.hs == nil {
		return ""
	}
	lower := strings.ToLower(key)
	headers := *c.hs
	for i := 0; i+1 < len(headers); i += 2 {
		if strings.ToLower(string(headers[i])) == lower {
			return string(headers[i+1])
		}
	}
	return ""
}

func (c byteHeadersCarrier) Set(key, value string) {
	if c.hs == nil {
		return
	}
	headers := *c.hs
	headers = append(headers, []byte(key), []byte(value))
	*c.hs = headers
}

func (c byteHeadersCarrier) Keys() []string {
	if c.hs == nil || *c.hs == nil {
		return nil
	}
	headers := *c.hs
	keys := make([]string, 0, len(headers)/2)
	for i := 0; i+1 < len(headers); i += 2 {
		keys = append(keys, string(headers[i]))
	}
	return keys
}

// Writer wrapper

type tracingWriterWrapper struct {
	w             connector.Writer
	connectorName string
}

func (d *tracingWriterWrapper) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	var span trace.Span
	ctx, span = tracer().Start(ctx, "writer.produce",
		trace.WithAttributes(
			messagingSystemFujin,
			attribute.String("connector", d.connectorName),
			attribute.Int("msg_size", len(msg)),
		))
	d.w.Produce(ctx, msg, func(err error) {
		if err != nil {
			span.RecordError(err)
		}
		span.End()
		if callback != nil {
			callback(err)
		}
	})
}

func (d *tracingWriterWrapper) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	var span trace.Span
	ctx, span = tracer().Start(ctx, "writer.hproduce",
		trace.WithAttributes(
			messagingSystemFujin,
			attribute.String("connector", d.connectorName),
			attribute.Int("msg_size", len(msg)),
			attribute.Int("header_count", len(headers)/2),
		),
	)
	carrier := newByteHeadersCarrier(&headers)
	propagator().Inject(ctx, carrier)
	d.w.HProduce(ctx, msg, headers, func(err error) {
		if err != nil {
			span.RecordError(err)
		}
		span.End()
		if callback != nil {
			callback(err)
		}
	})
}

func (d *tracingWriterWrapper) Flush(ctx context.Context) error {
	var span trace.Span
	ctx, span = tracer().Start(ctx, "writer.flush",
		trace.WithAttributes(
			messagingSystemFujin,
			attribute.String("connector", d.connectorName),
		),
	)
	defer span.End()
	err := d.w.Flush(ctx)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *tracingWriterWrapper) BeginTx(ctx context.Context) error {
	var span trace.Span
	ctx, span = tracer().Start(ctx, "writer.begin_tx",
		trace.WithAttributes(
			messagingSystemFujin,
			attribute.String("connector", d.connectorName),
		),
	)
	defer span.End()
	err := d.w.BeginTx(ctx)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *tracingWriterWrapper) CommitTx(ctx context.Context) error {
	var span trace.Span
	ctx, span = tracer().Start(ctx, "writer.commit_tx",
		trace.WithAttributes(
			messagingSystemFujin,
			attribute.String("connector", d.connectorName),
		),
	)
	defer span.End()
	err := d.w.CommitTx(ctx)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *tracingWriterWrapper) RollbackTx(ctx context.Context) error {
	var span trace.Span
	ctx, span = tracer().Start(ctx, "writer.rollback_tx",
		trace.WithAttributes(
			messagingSystemFujin,
			attribute.String("connector", d.connectorName),
		),
	)
	defer span.End()
	err := d.w.RollbackTx(ctx)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// Reader wrapper

type tracingReaderWrapper struct {
	r             connector.Reader
	connectorName string
}

func (d *tracingReaderWrapper) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	return d.r.Subscribe(
		ctx,
		func(message []byte, topic string, args ...any) {
			_, span := tracer().Start(ctx, "reader.subscribe.handle",
				trace.WithAttributes(
					messagingSystemFujin,
					attribute.String("connector", d.connectorName),
				),
			)
			h(message, topic, args...)
			span.End()
		},
	)
}

func (d *tracingReaderWrapper) HSubscribe(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	return d.r.HSubscribe(
		ctx,
		func(message []byte, topic string, hs [][]byte, args ...any) {
			carrier := newByteHeadersCarrier(&hs)
			ctx2 := propagator().Extract(ctx, carrier)
			_, span := tracer().Start(ctx2, "reader.hsubscribe.handle",
				trace.WithAttributes(
					messagingSystemFujin,
					attribute.String("connector", d.connectorName),
				),
			)
			h(message, topic, hs, args...)
			span.End()
		},
	)
}

func (d *tracingReaderWrapper) Fetch(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, args ...any)) {
	d.r.Fetch(ctx, n,
		func(n uint32, err error) {
			_, span := tracer().Start(ctx, "reader.fetch.handle",
				trace.WithAttributes(
					messagingSystemFujin,
					attribute.String("connector", d.connectorName),
				),
			)
			fetchResponseHandler(n, err)
			span.End()
		},
		func(message []byte, topic string, args ...any) {
			_, span := tracer().Start(ctx, "reader.fetch.handle_msg",
				trace.WithAttributes(
					messagingSystemFujin,
					attribute.String("connector", d.connectorName),
				),
			)
			msgHandler(message, topic, args...)
			span.End()
		},
	)
}

func (d *tracingReaderWrapper) HFetch(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, hs [][]byte, args ...any)) {
	d.r.HFetch(ctx, n,
		func(n uint32, err error) {
			_, span := tracer().Start(ctx, "reader.hfetch.handle",
				trace.WithAttributes(
					messagingSystemFujin,
					attribute.String("connector", d.connectorName),
				),
			)
			fetchResponseHandler(n, err)
			span.End()
		},
		func(message []byte, topic string, hs [][]byte, args ...any) {
			carrier := newByteHeadersCarrier(&hs)
			ctx2 := propagator().Extract(ctx, carrier)
			_, span := tracer().Start(ctx2, "reader.hfetch.handle_hmsg",
				trace.WithAttributes(
					messagingSystemFujin,
					attribute.String("connector", d.connectorName),
				),
			)
			msgHandler(message, topic, hs, args...)
			span.End()
		},
	)
}

func (d *tracingReaderWrapper) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	d.r.Ack(ctx, msgIDs,
		func(err error) {
			_, span := tracer().Start(ctx, "reader.ack.handle",
				trace.WithAttributes(
					messagingSystemFujin,
					attribute.String("connector", d.connectorName),
				),
			)
			ackHandler(err)
			span.End()
		},
		func(b []byte, err error) {
			_, span := tracer().Start(ctx, "reader.ack.handle_msg",
				trace.WithAttributes(
					messagingSystemFujin,
					attribute.String("connector", d.connectorName),
				),
			)
			ackMsgHandler(b, err)
			span.End()
		},
	)
}

func (d *tracingReaderWrapper) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	d.r.Nack(ctx, msgIDs,
		func(err error) {
			_, span := tracer().Start(ctx, "reader.nack.handle",
				trace.WithAttributes(
					messagingSystemFujin,
					attribute.String("connector", d.connectorName),
				),
			)
			nackHandler(err)
			span.End()
		},
		func(b []byte, err error) {
			_, span := tracer().Start(ctx, "reader.nack.handle_msg",
				trace.WithAttributes(
					messagingSystemFujin,
					attribute.String("connector", d.connectorName),
				),
			)
			nackMsgHandler(b, err)
			span.End()
		},
	)
}

func (d *tracingReaderWrapper) MsgIDStaticArgsLen() int {
	return d.r.MsgIDStaticArgsLen()
}

func (d *tracingReaderWrapper) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return d.r.EncodeMsgID(buf, topic, args...)
}

func (d *tracingReaderWrapper) IsAutoCommit() bool {
	return d.r.IsAutoCommit()
}
