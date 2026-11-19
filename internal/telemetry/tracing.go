package telemetry

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation scope for spans created by this service.
const TracerName = "github.com/iAscen/ledgerline"

// Tracer returns the service tracer.
func Tracer() trace.Tracer { return otel.Tracer(TracerName) }

// SetupTracing installs a global TracerProvider. Spans are exported over
// OTLP/HTTP only when OTEL_EXPORTER_OTLP_ENDPOINT (or the traces-specific
// variable) is set; otherwise spans are still created, so trace IDs appear in
// logs and propagate to webhooks, but nothing is exported.
func SetupTracing(ctx context.Context, service, version string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	res, err := sdkresource.Merge(sdkresource.Default(), sdkresource.NewSchemaless(
		semconv.ServiceName(service), semconv.ServiceVersion(version),
	))
	if err != nil {
		return nil, err
	}
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// TraceHandler decorates log records with trace_id/span_id from the context,
// so a log line can be joined to its trace.
type TraceHandler struct{ slog.Handler }

// Handle implements slog.Handler.
func (h TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs implements slog.Handler.
func (h TraceHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return TraceHandler{h.Handler.WithAttrs(a)}
}

// WithGroup implements slog.Handler.
func (h TraceHandler) WithGroup(n string) slog.Handler { return TraceHandler{h.Handler.WithGroup(n)} }

// TraceParent serialises the span context in ctx as a W3C traceparent value.
func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier["traceparent"]
}

// ContextWithTraceParent returns ctx carrying the remote span context encoded
// in tp, so asynchronous work can be linked to the request that created it.
func ContextWithTraceParent(ctx context.Context, tp string) context.Context {
	if tp == "" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": tp})
}
