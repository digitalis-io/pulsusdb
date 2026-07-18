// firehose is a synthetic telemetry generator that reproduces a realistic
// monthly observability envelope (see profile.go) rather than blasting the
// maximum the node can push. It emits logs, metrics, and traces straight to the
// OTel collector over OTLP/gRPC with three properties that make the data look
// real:
//
//   - Bounded cardinality: a fixed fleet (hosts/containers) and a fixed
//     custom-metric catalog. Series count is a function of the profile, NOT of
//     the pod/replica count -- every pod owns a deterministic stripe of the
//     same fleet, so the union across pods is the envelope exactly once
//     (run as a StatefulSet so pods get stable ordinals; see fleet.go).
//   - Realistic rate: monthly totals are divided into a per-second rate and
//     rate-limited, so logs/spans arrive at the true production pace.
//   - Time compression: SPEEDUP replays a whole month in month/SPEEDUP of
//     wall-clock, and emitted records carry back-dated timestamps advancing
//     across the simulated window (profile.simClock), so the data spreads over
//     a month instead of piling up at "now". SPEEDUP=1 is real-time steady
//     state.
package main

import (
	"context"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

func envStr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

var (
	metricsAddr = envStr("METRICS_ADDR", ":2112")

	// Fleet-wide uncompressed log-byte budget; the generator stops when the
	// per-pod share (maxLogBytes / replicas) is reached. Empty/0 = uncapped.
	maxLogBytes = envStr("MAX_LOG_BYTES", "")

	logSeverities = []otellog.Severity{
		otellog.SeverityDebug, otellog.SeverityInfo, otellog.SeverityInfo,
		otellog.SeverityInfo, otellog.SeverityWarn, otellog.SeverityError,
	}
	logStatusCodes = []int64{200, 200, 200, 201, 400, 404, 500, 503}
)

func randString(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

func initResource(ctx context.Context) (*resource.Resource, error) {
	return resource.New(ctx, resource.WithFromEnv())
}

// rateLoop calls emit at an average of perSec events per second, batched onto a
// coarse ticker (fractional remainder carried across ticks so the long-run rate
// is exact). Each emit is handed the current sim-clock instant. Returns when
// ctx is cancelled. perSec <= 0 is a no-op.
func rateLoop(ctx context.Context, perSec float64, p Profile, startWall time.Time, nowFn func() time.Time, emit func(t time.Time)) {
	if perSec <= 0 {
		return
	}
	const tick = 100 * time.Millisecond
	perTick := perSec * tick.Seconds()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	var acc float64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		acc += perTick
		n := int(acc)
		acc -= float64(n)
		for i := 0; i < n; i++ {
			emit(p.simClock(startWall, nowFn()))
		}
	}
}

// runLogs emits fleet-attributed log records at the profile's per-pod rate.
func runLogs(ctx context.Context, wg *sync.WaitGroup, provider *sdklog.LoggerProvider,
	fleet *Fleet, shard Shard, p Profile, startWall time.Time, body string,
) {
	hosts := fleet.ownedHosts(shard)
	if len(hosts) == 0 {
		return
	}
	perPod := p.logsPerSec() / float64(shard.Total)
	logger := provider.Logger("firehose")
	rng := rand.New(rand.NewSource(2))
	wg.Add(1)
	go func() {
		defer wg.Done()
		rateLoop(ctx, perPod, p, startWall, time.Now, func(t time.Time) {
			h := hosts[rng.Intn(len(hosts))]
			var rec otellog.Record
			rec.SetTimestamp(t)
			rec.SetObservedTimestamp(t)
			rec.SetSeverity(logSeverities[rng.Intn(len(logSeverities))])
			rec.SetBody(otellog.StringValue(body))
			rec.AddAttributes(
				otellog.String("service.name", h.Service),
				otellog.String("host.name", h.Name),
				otellog.String("region", h.Region),
				otellog.String("tier", h.Tier),
				otellog.Int64("status_code", logStatusCodes[rng.Intn(len(logStatusCodes))]),
				otellog.String("trace_id", randHex(rng, 32)),
			)
			logger.Emit(ctx, rec)
			emittedRecords.WithLabelValues("log").Inc()
		})
	}()
}

// runTraces emits whole traces (root + child spans) at the profile's per-pod
// trace rate, attributed to the fleet's APM hosts, with back-dated span times.
func runTraces(ctx context.Context, wg *sync.WaitGroup, tracer trace.Tracer,
	fleet *Fleet, shard Shard, p Profile, startWall time.Time,
) {
	hosts := fleet.ownedAPMHosts(shard)
	if len(hosts) == 0 {
		return
	}
	perPod := p.tracesPerSec() / float64(shard.Total)
	rng := rand.New(rand.NewSource(3))
	wg.Add(1)
	go func() {
		defer wg.Done()
		rateLoop(ctx, perPod, p, startWall, time.Now, func(t time.Time) {
			h := hosts[rng.Intn(len(hosts))]
			// each span gets a small synthetic duration; the trace spans a few ms.
			cur := t
			rootCtx, root := tracer.Start(ctx, h.Service+".request",
				trace.WithTimestamp(cur),
				trace.WithAttributes(
					attribute.String("host.name", h.Name),
					attribute.String("region", h.Region),
					attribute.String("tier", h.Tier),
				),
			)
			for s := 0; s < p.SpansPerTrace-1; s++ {
				stepStart := cur
				dur := time.Duration(1+rng.Intn(20)) * time.Millisecond
				_, span := tracer.Start(rootCtx, "step-"+strconv.Itoa(s),
					trace.WithTimestamp(stepStart),
					trace.WithAttributes(
						attribute.String("endpoint", customEndpoints[rng.Intn(len(customEndpoints))]),
						attribute.Int("payload_bytes", 128+rng.Intn(2048)),
					),
				)
				cur = stepStart.Add(dur)
				span.End(trace.WithTimestamp(cur))
			}
			root.End(trace.WithTimestamp(cur.Add(time.Millisecond)))
			emittedRecords.WithLabelValues("span").Add(float64(p.SpansPerTrace))
			emittedRecords.WithLabelValues("trace").Inc()
		})
	}()
}

func randHex(rng *rand.Rand, n int) string {
	const hexdigits = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = hexdigits[rng.Intn(16)]
	}
	return string(b)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startWall := time.Now()
	profile := loadProfile(startWall)
	shard := loadShard()

	capTotal, err := parseBytes(maxLogBytes)
	if err != nil {
		logger.Error("invalid MAX_LOG_BYTES", "value", maxLogBytes, "error", err)
		os.Exit(1)
	}
	var perPodCap int64
	if capTotal > 0 && shard.Total > 0 {
		perPodCap = capTotal / int64(shard.Total)
	}

	res, err := initResource(ctx)
	if err != nil {
		logger.Error("resource init failed", "error", err)
		os.Exit(1)
	}

	fleet := buildFleet(profile)
	catalog := buildCatalog(profile, fleet, shard)

	// Self-observability: expose emitted-volume metrics for capacity reports.
	go func() {
		if err := serveMetrics(metricsAddr); err != nil {
			logger.Error("metrics endpoint stopped", "error", err)
		}
	}()

	// Traces
	traceExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithDialOption(grpc.WithStatsHandler(byteStatsHandler{signal: "trace"})),
	)
	if err != nil {
		logger.Error("trace exporter init failed", "error", err)
		os.Exit(1)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp, sdktrace.WithMaxExportBatchSize(1024), sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	// Bound every provider flush on shutdown -- an unreachable collector at
	// SIGTERM would otherwise block the flush forever until k8s SIGKILLs the pod.
	defer shutdownWithTimeout(logger, "trace provider", tp.Shutdown)

	// Logs
	logExp, err := otlploggrpc.New(ctx,
		otlploggrpc.WithInsecure(),
		otlploggrpc.WithDialOption(grpc.WithStatsHandler(byteStatsHandler{signal: "log"})),
	)
	if err != nil {
		logger.Error("log exporter init failed", "error", err)
		os.Exit(1)
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp,
			sdklog.WithMaxQueueSize(8192),
			sdklog.WithExportMaxBatchSize(2048),
			sdklog.WithExportInterval(2*time.Second),
		)),
		sdklog.WithResource(res),
	)
	defer shutdownWithTimeout(logger, "log provider", lp.Shutdown)

	// Metrics: exported directly (not via a MeterProvider) so every datapoint
	// carries a back-dated sim timestamp and the catalog cardinality is exact.
	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithDialOption(grpc.WithStatsHandler(byteStatsHandler{signal: "metric"})),
	)
	if err != nil {
		logger.Error("metric exporter init failed", "error", err)
		os.Exit(1)
	}
	defer shutdownWithTimeout(logger, "metric exporter", metricExp.Shutdown)

	logger.Info("firehose starting",
		"speedup", profile.Speedup, "window_start", profile.WindowStart.Format(time.RFC3339),
		"shard_index", shard.Index, "shard_total", shard.Total,
		"fleet_hosts", len(fleet.Hosts), "fleet_containers", len(fleet.Containers),
		"metric_series_total", profile.metricSeriesTotal(), "metric_series_this_pod", catalog.seriesTotal,
		"custom_series_capped", catalog.capped,
		"logs_per_sec_fleet", profile.logsPerSec(), "spans_per_sec_fleet", profile.spansPerSec(),
		"metric_dps_per_sec_fleet", profile.metricDatapointsPerSec(),
		"max_log_bytes", maxLogBytes, "per_pod_cap_bytes", perPodCap,
	)

	// Workers run under a child context so the cap watcher can stop generation
	// without killing the process (which k8s would restart and re-emit).
	workersCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()

	body := randString(profile.LogBodyBytes)

	var wg sync.WaitGroup
	runLogs(workersCtx, &wg, lp, fleet, shard, profile, startWall, body)
	runTraces(workersCtx, &wg, tp.Tracer("firehose"), fleet, shard, profile, startWall)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runMetricCatalog(workersCtx, metricExp, catalog, res, profile, startWall, time.Now)
	}()

	go watchLogCap(workersCtx, perPodCap, stopWorkers, func(total int64) {
		logger.Info("log-byte cap reached; stopping generator, pod will idle until SIGTERM",
			"log_bytes", total, "per_pod_cap_bytes", perPodCap)
	})

	<-workersCtx.Done()
	wg.Wait()
	if ctx.Err() == nil {
		// Stopped by the cap, not a signal: idle alive so k8s won't restart.
		logger.Info("firehose idling after cap")
		<-ctx.Done()
	}
	logger.Info("firehose shutting down")
}

// shutdownWithTimeout flushes an OTel provider/exporter with a bounded context
// so a defer'd shutdown can never block the process forever on an unreachable
// collector.
func shutdownWithTimeout(logger *slog.Logger, name string, fn func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := fn(ctx); err != nil {
		logger.Warn("shutdown flush failed", "component", name, "error", err)
	}
}
