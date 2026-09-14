package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var (
	tracer     trace.Tracer
	logger     *slog.Logger
	role       string
	downstream string
)

// httpClient bounds every downstream call. Without a timeout a slow/hung tier
// (this app deliberately injects multi-second "slow query" delays) blocks the
// request goroutine forever, piling up goroutines and connections until the pod
// OOMs instead of returning a clean 502.
var httpClient = &http.Client{Timeout: 5 * time.Second}

// traceHandler injects the active span's IDs into every log record so logs
// can be correlated to traces in the observability backend.
type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		sc := span.SpanContext()
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
			slog.String("service", role),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func initTracer(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	res, _ := resource.New(ctx, resource.WithFromEnv())
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	// TraceContext propagator = W3C traceparent header, the glue that lets
	// a child service continue the caller's trace.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return tp, nil
}

func downstreamName() string {
	switch role {
	case "frontend":
		return "middletier"
	case "middletier":
		return "backend"
	}
	return "none"
}

// callDownstream invokes the next tier over HTTP, injecting the current
// trace context so the remote span joins this trace.
func callDownstream(ctx context.Context, orderID string) (int, error) {
	ctx, span := tracer.Start(ctx, "call "+downstreamName())
	defer span.End()

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, downstream, nil)
	req.Header.Set("X-Order-Id", orderID)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := httpClient.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorContext(ctx, "downstream call failed",
			"order_id", orderID, "downstream", downstreamName(), "error", err)
		return http.StatusBadGateway, err
	}
	defer resp.Body.Close()
	span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))
	if resp.StatusCode >= 500 {
		e := fmt.Errorf("downstream %s returned %d", downstreamName(), resp.StatusCode)
		span.RecordError(e)
		span.SetStatus(codes.Error, e.Error())
		return resp.StatusCode, e
	}
	return resp.StatusCode, nil
}

// work simulates a local unit of work as its own child span.
func work(ctx context.Context, name string, minMs, maxMs int) {
	_, span := tracer.Start(ctx, name)
	defer span.End()
	time.Sleep(time.Duration(minMs+rand.Intn(maxMs)) * time.Millisecond)
}

// backendWork simulates a DB call with injected slow queries and errors.
func backendWork(ctx context.Context, orderID string) error {
	ctx, span := tracer.Start(ctx, "db-query orders")
	defer span.End()

	delay := 20 + rand.Intn(60)
	if rand.Intn(5) == 0 { // ~20% slow query
		delay = 800 + rand.Intn(700)
		logger.WarnContext(ctx, "slow query detected",
			"table", "orders", "query_ms", delay, "threshold_ms", 300, "order_id", orderID)
	}
	span.SetAttributes(
		attribute.String("db.system", "postgresql"),
		attribute.String("db.statement", "SELECT * FROM orders WHERE id = $1"),
		attribute.Int("db.query_ms", delay),
	)
	time.Sleep(time.Duration(delay) * time.Millisecond)

	if rand.Intn(7) == 0 { // ~14% hard failure
		err := fmt.Errorf("database connection pool exhausted: all 20 connections busy (order %s)", orderID)
		span.SetAttributes(
			attribute.Bool("error", true),
			attribute.String("error.type", "pool_exhausted"),
			attribute.Int("db.pool.max", 20),
			attribute.Int("db.pool.in_use", 20),
		)
		span.RecordError(err, trace.WithStackTrace(true))
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorContext(ctx, "backend db error",
			"order_id", orderID, "db", "postgresql",
			"http.status_code", 500, "error", err)
		return err
	}
	logger.InfoContext(ctx, "order persisted", "order_id", orderID, "query_ms", delay)
	return nil
}

func handleCheckout(w http.ResponseWriter, r *http.Request) {
	// Continue the caller's trace by extracting traceparent from headers.
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := tracer.Start(ctx, role+" /api/checkout")
	defer span.End()
	span.SetAttributes(httpReqAttrs(r)...)

	orderID := r.Header.Get("X-Order-Id")
	if orderID == "" {
		orderID = fmt.Sprintf("ord-%05d", rand.Intn(100000))
	}
	span.SetAttributes(
		attribute.String("order.id", orderID),
		attribute.String("tier", role),
		attribute.String("enduser.id", fmt.Sprintf("cust-%03d", rand.Intn(500))),
	)
	logger.InfoContext(ctx, "request received", "order_id", orderID, "tier", role)

	switch role {
	case "frontend":
		work(ctx, "validate-session", 5, 20)
		work(ctx, "render-cart", 5, 25)
		status, err := callDownstream(ctx, orderID)
		if err != nil {
			fail(span, http.StatusBadGateway, "downstream_unavailable", err)
			logger.ErrorContext(ctx, "checkout failed",
				"order_id", orderID, "downstream_status", status,
				"http.status_code", http.StatusBadGateway, "error", err)
			http.Error(w, "checkout unavailable", http.StatusBadGateway)
			return
		}
		logger.InfoContext(ctx, "checkout confirmed", "order_id", orderID)
	case "middletier":
		work(ctx, "cache-lookup", 3, 15)
		work(ctx, "apply-pricing-rules", 10, 40)
		status, err := callDownstream(ctx, orderID)
		if err != nil {
			fail(span, http.StatusBadGateway, "backend_error", err)
			logger.ErrorContext(ctx, "order enrichment failed",
				"order_id", orderID, "downstream_status", status,
				"http.status_code", http.StatusBadGateway, "error", err)
			http.Error(w, "backend error", http.StatusBadGateway)
			return
		}
	case "backend":
		if err := backendWork(ctx, orderID); err != nil {
			fail(span, http.StatusInternalServerError, "db_error", err)
			logger.ErrorContext(ctx, "checkout failed at backend",
				"order_id", orderID,
				"http.status_code", http.StatusInternalServerError, "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	span.SetAttributes(attribute.Int("http.status_code", http.StatusOK))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"order_id": orderID, "tier": role, "status": "ok"})
}

// httpReqAttrs captures full HTTP request context on the active span.
func httpReqAttrs(r *http.Request) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("http.method", r.Method),
		attribute.String("http.target", r.URL.RequestURI()),
		attribute.String("http.route", r.URL.Path),
		attribute.String("http.scheme", "http"),
		attribute.String("http.flavor", r.Proto),
		attribute.String("http.host", r.Host),
		attribute.String("http.url", "http://"+r.Host+r.URL.RequestURI()),
		attribute.String("http.user_agent", r.UserAgent()),
		attribute.String("http.client_ip", r.RemoteAddr),
		attribute.String("service.tier", role),
	}
}

// fail flags the span as errored with HTTP + error attributes and records
// the error (with stack trace) as an exception event.
func fail(span trace.Span, status int, errType string, err error) {
	span.SetAttributes(
		attribute.Int("http.status_code", status),
		attribute.Bool("error", true),
		attribute.String("error.type", errType),
		attribute.String("error.message", err.Error()),
	)
	span.RecordError(err, trace.WithStackTrace(true))
	span.SetStatus(codes.Error, err.Error())
}

func handleHealth(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") }

func main() {
	role = os.Getenv("ROLE")
	if role == "" {
		role = "backend"
	}
	downstream = os.Getenv("DOWNSTREAM_URL")

	ctx := context.Background()
	tp, err := initTracer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tracer init failed: %v\n", err)
		os.Exit(1)
	}
	defer tp.Shutdown(ctx)

	logger = slog.New(traceHandler{
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	})
	tracer = otel.Tracer("otel-" + role)

	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/api/checkout", handleCheckout)

	logger.InfoContext(ctx, "tier starting", "role", role, "downstream", downstream, "port", 8080)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		logger.ErrorContext(ctx, "server failed", "error", err)
		os.Exit(1)
	}
}
