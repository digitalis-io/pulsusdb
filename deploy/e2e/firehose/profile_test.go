package main

import (
	"testing"
	"time"
)

func TestLoadProfileDefaults(t *testing.T) {
	// Env-free load: defaults are the capacity-model envelope.
	now := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	p := loadProfile(now)

	if p.MonthDays != 30 || p.Speedup != 1 {
		t.Fatalf("defaults: MonthDays=%d Speedup=%v", p.MonthDays, p.Speedup)
	}
	if p.CustomMetricSeries != 6100 {
		t.Errorf("CustomMetricSeries = %d, want 6100", p.CustomMetricSeries)
	}
	// WindowStart defaults to a trailing month.
	if got := now.Sub(p.WindowStart); got != 30*24*time.Hour {
		t.Errorf("WindowStart offset = %v, want 720h", got)
	}
}

func TestProfileRates(t *testing.T) {
	p := Profile{MonthDays: 30, Speedup: 1, LogsPerMonth: 3.2e9, SpansPerMonth: 6.9e8, SpansPerTrace: 5}
	sec := 30.0 * 86400
	if got, want := p.logsPerSec(), 3.2e9/sec; got != want {
		t.Errorf("logsPerSec = %v, want %v", got, want)
	}
	if got, want := p.spansPerSec(), 6.9e8/sec; got != want {
		t.Errorf("spansPerSec = %v, want %v", got, want)
	}
	// tracesPerSec = spansPerSec / spansPerTrace.
	if got, want := p.tracesPerSec(), (6.9e8/sec)/5; got != want {
		t.Errorf("tracesPerSec = %v, want %v", got, want)
	}
	// Speedup scales the rate linearly.
	p.Speedup = 720
	if got, want := p.logsPerSec(), 3.2e9/sec*720; got != want {
		t.Errorf("logsPerSec@720 = %v, want %v", got, want)
	}
}

func TestInfraSeriesPerHost(t *testing.T) {
	// Default fan-out (8 cpu, 2 disk, 4 mount, 2 nic) -> a realistic node-
	// exporter-scale ~200 series/host, not a flat handful.
	p := Profile{CPUsPerHost: 8, DisksPerHost: 2, MountsPerHost: 4, NICsPerHost: 2}
	if got := p.infraSeriesPerHost(); got != 210 {
		t.Fatalf("infraSeriesPerHost = %d, want 210", got)
	}
	// Fan-out scales with per-host resources: more cores -> more series.
	p.CPUsPerHost = 16
	if got := p.infraSeriesPerHost(); got <= 210 {
		t.Errorf("more CPUs must add series, got %d", got)
	}
}

func TestMetricSeriesTotal(t *testing.T) {
	p := Profile{
		CustomMetricSeries: 6100, Hosts: 600, Containers: 6000, InfraMetricsPerCont: 0,
		CPUsPerHost: 8, DisksPerHost: 2, MountsPerHost: 4, NICsPerHost: 2,
	}
	// 6100 custom + 600 hosts * 210 infra series/host.
	if got, want := p.metricSeriesTotal(), 6100+600*210; got != want {
		t.Errorf("metricSeriesTotal = %d, want %d", got, want)
	}
}

func TestSimClock(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	p := Profile{Speedup: 720, WindowStart: start.Add(-30 * 24 * time.Hour)}

	// At t=start, no elapsed => sim sits at WindowStart.
	if got := p.simClock(start, start); !got.Equal(p.WindowStart) {
		t.Errorf("simClock(0 elapsed) = %v, want %v", got, p.WindowStart)
	}
	// After 1 wall-minute at 720x => 720 sim-minutes = 12h past WindowStart.
	got := p.simClock(start, start.Add(time.Minute))
	if want := p.WindowStart.Add(12 * time.Hour); !got.Equal(want) {
		t.Errorf("simClock(1m@720x) = %v, want %v", got, want)
	}
	// Far in the future the sim clock clamps to now (steady state).
	now := start.Add(48 * time.Hour)
	if got := p.simClock(start, now); !got.Equal(now) {
		t.Errorf("simClock clamp = %v, want %v", got, now)
	}
}
