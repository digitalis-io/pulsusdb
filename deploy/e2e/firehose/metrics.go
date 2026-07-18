// Self-observability for the firehose load generator: how much telemetry it
// pushes and how fast. All counters are exposed on a Prometheus /metrics
// endpoint (METRICS_ADDR, default :2112) so the emitted data volume can be
// measured for capacity/cost reports independently of the receiving stack.
//
// Byte volume is measured EXACTLY on the wire via a gRPC stats.Handler
// attached to each OTLP exporter: WireLength is the post-compression bytes
// actually sent (so it reflects OTEL_EXPORTER_OTLP_COMPRESSION=gzip), and
// Length is the uncompressed serialized payload — the pair shows the gzip
// saving. Record counts are incremented app-side at emit time.
package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/stats"
)

var (
	// Exact bytes sent on the wire per signal, after compression.
	exportWireBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_export_wire_bytes_total",
		Help: "Compressed OTLP payload bytes sent on the wire, by signal.",
	}, []string{"signal"})

	// Uncompressed serialized payload bytes per signal (pre-compression).
	exportUncompressedBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_export_uncompressed_bytes_total",
		Help: "Uncompressed OTLP payload bytes serialized, by signal.",
	}, []string{"signal"})

	// OTLP export RPCs attempted per signal.
	exportRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_export_requests_total",
		Help: "OTLP export RPCs sent, by signal.",
	}, []string{"signal"})

	// OTLP export RPCs that returned an error, per signal.
	exportErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_export_errors_total",
		Help: "OTLP export RPCs that failed, by signal.",
	}, []string{"signal"})

	// Records emitted app-side: log records, spans, traces, metric datapoints.
	emittedRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "firehose_emitted_records_total",
		Help: "Telemetry records emitted by the generator, by kind (log|span|trace|metric_datapoint).",
	}, []string{"kind"})

	// Time the generator started, for run-duration calculations in the report.
	startTime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "firehose_start_time_seconds",
		Help: "Unix timestamp when the firehose generator started.",
	})

	// Per-pod uncompressed-log-bytes cap this pod enforces (0 = uncapped).
	logCapBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "firehose_log_cap_bytes",
		Help: "Per-pod uncompressed log-byte cap; the generator stops when reached (0 = uncapped).",
	})

	// 1 once this pod has hit the cap and stopped generating, else 0.
	logCapped = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "firehose_log_capped",
		Help: "1 if the pod has reached its log-byte cap and stopped, else 0.",
	})

	// Running total of successfully-sent uncompressed log bytes, read by the
	// cap watcher without going through the Prometheus counter.
	logUncompressedBytes atomic.Int64
)

// byteStatsHandler is a gRPC stats.Handler that accounts exact per-RPC payload
// bytes for one OTLP signal. One instance is attached per exporter so bytes are
// labelled by signal without inspecting message types.
type byteStatsHandler struct{ signal string }

func (h byteStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h byteStatsHandler) HandleRPC(_ context.Context, s stats.RPCStats) {
	switch v := s.(type) {
	case *stats.OutPayload:
		exportWireBytes.WithLabelValues(h.signal).Add(float64(v.WireLength))
		exportUncompressedBytes.WithLabelValues(h.signal).Add(float64(v.Length))
		if h.signal == "log" {
			logUncompressedBytes.Add(int64(v.Length))
		}
	case *stats.End:
		exportRequests.WithLabelValues(h.signal).Inc()
		if v.Error != nil {
			exportErrors.WithLabelValues(h.signal).Inc()
		}
	}
}

func (h byteStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h byteStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

// parseBytes turns a human byte size into bytes. Accepts a bare integer
// (bytes) or a decimal (K/M/G/T = 1e3..1e12) or binary (Ki/Mi/Gi/Ti = 2^10..)
// suffix, with an optional trailing "B" (e.g. "400G", "400GB", "22GiB").
// Empty string means 0 (uncapped).
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	u := strings.ToUpper(strings.TrimSuffix(s, "B"))
	mult := int64(1)
	switch {
	case strings.HasSuffix(u, "KI"):
		mult, u = 1<<10, strings.TrimSuffix(u, "KI")
	case strings.HasSuffix(u, "MI"):
		mult, u = 1<<20, strings.TrimSuffix(u, "MI")
	case strings.HasSuffix(u, "GI"):
		mult, u = 1<<30, strings.TrimSuffix(u, "GI")
	case strings.HasSuffix(u, "TI"):
		mult, u = 1<<40, strings.TrimSuffix(u, "TI")
	case strings.HasSuffix(u, "K"):
		mult, u = 1e3, strings.TrimSuffix(u, "K")
	case strings.HasSuffix(u, "M"):
		mult, u = 1e6, strings.TrimSuffix(u, "M")
	case strings.HasSuffix(u, "G"):
		mult, u = 1e9, strings.TrimSuffix(u, "G")
	case strings.HasSuffix(u, "T"):
		mult, u = 1e12, strings.TrimSuffix(u, "T")
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(u), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	if f < 0 {
		return 0, fmt.Errorf("byte size %q must not be negative", s)
	}
	return int64(f * float64(mult)), nil
}

// watchLogCap stops the generator once this pod's uncompressed log bytes reach
// perPodCap. It polls the atomic total and calls stop() (which cancels the
// worker context) exactly once. A perPodCap <= 0 disables the cap. Runs in a
// goroutine; returns when the cap fires or the context is done.
func watchLogCap(ctx context.Context, perPodCap int64, stop context.CancelFunc, onCap func(total int64)) {
	logCapBytes.Set(float64(perPodCap))
	logCapped.Set(0)
	if perPodCap <= 0 {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if total := logUncompressedBytes.Load(); total >= perPodCap {
				logCapped.Set(1)
				if onCap != nil {
					onCap(total)
				}
				stop()
				return
			}
		}
	}
}

// serveMetrics starts the Prometheus scrape endpoint. Blocks; run in a goroutine.
func serveMetrics(addr string) error {
	startTime.Set(float64(time.Now().Unix()))
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return srv.ListenAndServe()
}
