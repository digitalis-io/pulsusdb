package main

import (
	"os"
	"strconv"
	"time"
)

// Profile is the target monthly telemetry envelope the generator reproduces.
//
// The defaults approximate a real mid-size production estate taken from an
// observability capacity/cost model (infra hosts, containers, custom-metric
// cardinality, and monthly log/span volumes). They are intentionally generic
// and every field is env-overridable, so the same image can model a larger or
// smaller estate without a rebuild -- no customer name or raw usage export
// lives in the repo.
//
// Volumes are expressed per 30-day month. The generator turns each monthly
// total into a live emission RATE by dividing by the month length and
// multiplying by Speedup, so:
//
//	rate/sec = monthly_total / (MonthDays * 86400) * Speedup
//
// Speedup is a pure time-compression knob: Speedup=1 emits at the true
// real-world rate (a wall-clock month reproduces the envelope exactly);
// Speedup=720 replays the whole month in ~1 hour. Emitted records carry
// back-dated timestamps advancing from WindowStart at Speedup x wall-clock,
// so the data lands spread across the simulated month rather than all at "now".
type Profile struct {
	MonthDays   int           // length of the simulated month
	Speedup     float64       // time-compression factor (see above)
	WindowStart time.Time     // sim-clock origin; records advance from here toward now
	MonthDur    time.Duration // MonthDays as a duration (derived)

	// Fleet: the fixed set of emitting entities. Cardinality is a function of
	// these counts, NOT of the pod/replica count -- every pod draws from the
	// same deterministic fleet, so the union across pods is exactly this fleet.
	Hosts      int // infra hosts
	APMHosts   int // subset of hosts that also emit traces (APM)
	Containers int // containers spread across the hosts

	// Metrics.
	CustomMetricSeries int // custom-metric catalog cardinality (the DataDog "Custom Metrics" analogue)
	// Infra host metrics fan out over per-host resources, like a real
	// node-exporter / OTel hostmetrics receiver -- so a single host emits
	// hundreds of series (per-cpu, per-device, per-mount, per-interface), not a
	// flat handful. These knobs set the fan-out width; infraSeriesPerHost()
	// turns them into the per-host series count.
	CPUsPerHost         int
	DisksPerHost        int
	MountsPerHost       int
	NICsPerHost         int
	InfraMetricsPerCont int // standard metrics emitted per container (off by default)
	MetricIntervalSec   int // emit/scrape interval in SIM seconds

	// Logs.
	LogsPerMonth float64 // total log records per month
	LogBodyBytes int     // mean log body size (bytes)

	// Traces.
	SpansPerMonth float64 // total spans per month
	SpansPerTrace int     // spans per trace (root + children)
}

func envFloat(name string, def float64) float64 {
	if v := os.Getenv(name); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// loadProfile builds the Profile from env with the capacity-model defaults.
// WINDOW_START (RFC3339) pins the sim-clock origin; empty means "trailing
// MonthDays ending now", i.e. the generator back-fills the last month and then
// continues in real time.
func loadProfile(now time.Time) Profile {
	p := Profile{
		MonthDays: envInt("MONTH_DAYS", 30),
		Speedup:   envFloat("SPEEDUP", 1),

		Hosts:      envInt("HOSTS", 600),
		APMHosts:   envInt("APM_HOSTS", 30),
		Containers: envInt("CONTAINERS", 6000),

		CustomMetricSeries:  envInt("CUSTOM_METRIC_SERIES", 6100),
		CPUsPerHost:         envInt("CPUS_PER_HOST", 8),
		DisksPerHost:        envInt("DISKS_PER_HOST", 2),
		MountsPerHost:       envInt("MOUNTS_PER_HOST", 4),
		NICsPerHost:         envInt("NICS_PER_HOST", 2),
		InfraMetricsPerCont: envInt("INFRA_METRICS_PER_CONTAINER", 0),
		MetricIntervalSec:   envInt("METRIC_INTERVAL_SEC", 15),

		LogsPerMonth: envFloat("LOGS_PER_MONTH", 3.2e9),
		LogBodyBytes: envInt("LOG_BODY_BYTES", 1900),

		SpansPerMonth: envFloat("SPANS_PER_MONTH", 6.9e8),
		SpansPerTrace: envInt("SPANS_PER_TRACE", 5),
	}
	if p.MonthDays < 1 {
		p.MonthDays = 1
	}
	if p.Speedup <= 0 {
		p.Speedup = 1
	}
	if p.MetricIntervalSec < 1 {
		p.MetricIntervalSec = 1
	}
	if p.SpansPerTrace < 1 {
		p.SpansPerTrace = 1
	}
	p.MonthDur = time.Duration(p.MonthDays) * 24 * time.Hour

	if ws := os.Getenv("WINDOW_START"); ws != "" {
		if t, err := time.Parse(time.RFC3339, ws); err == nil {
			p.WindowStart = t
		}
	}
	if p.WindowStart.IsZero() {
		p.WindowStart = now.Add(-p.MonthDur)
	}
	return p
}

// secondsPerMonth is the real-world seconds in the simulated month.
func (p Profile) secondsPerMonth() float64 { return float64(p.MonthDays) * 86400 }

// logsPerSec is the fleet-wide log emission rate at the configured Speedup.
func (p Profile) logsPerSec() float64 { return p.LogsPerMonth / p.secondsPerMonth() * p.Speedup }

// spansPerSec is the fleet-wide span emission rate at the configured Speedup.
func (p Profile) spansPerSec() float64 { return p.SpansPerMonth / p.secondsPerMonth() * p.Speedup }

// tracesPerSec is spansPerSec folded into whole traces.
func (p Profile) tracesPerSec() float64 { return p.spansPerSec() / float64(p.SpansPerTrace) }

// metricSeriesTotal is the whole-fleet metric cardinality: custom catalog plus
// standard (fanned-out) infra metrics per host and per container.
func (p Profile) metricSeriesTotal() int {
	return p.CustomMetricSeries +
		p.Hosts*p.infraSeriesPerHost() +
		p.Containers*p.InfraMetricsPerCont
}

// metricDatapointsPerSec is the fleet-wide metric datapoint rate at Speedup:
// every series is emitted once per (sim) interval.
func (p Profile) metricDatapointsPerSec() float64 {
	return float64(p.metricSeriesTotal()) / float64(p.MetricIntervalSec) * p.Speedup
}

// simClock maps a wall-clock instant to the simulated timestamp: it advances
// from WindowStart at Speedup x wall-clock, clamped to now once it catches up
// (after which the generator emits in real time / steady state).
func (p Profile) simClock(startWall, nowWall time.Time) time.Time {
	elapsed := nowWall.Sub(startWall)
	sim := p.WindowStart.Add(time.Duration(float64(elapsed) * p.Speedup))
	if sim.After(nowWall) {
		return nowWall
	}
	return sim
}
