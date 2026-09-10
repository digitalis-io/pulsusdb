package main

import (
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestShardPartitioning(t *testing.T) {
	// Across N shards, every item is owned exactly once and counts sum to n.
	const n = 6100
	for _, total := range []int{1, 3, 6, 18} {
		owners := make([]int, n)
		sum := 0
		for idx := 0; idx < total; idx++ {
			s := Shard{Index: idx, Total: total}
			c := 0
			for i := 0; i < n; i++ {
				if s.owns(i) {
					owners[i]++
					c++
				}
			}
			if c != s.count(n) {
				t.Errorf("total=%d shard=%d: owned %d, count() said %d", total, idx, c, s.count(n))
			}
			sum += c
		}
		if sum != n {
			t.Errorf("total=%d: shards owned %d items, want %d", total, sum, n)
		}
		for i, o := range owners {
			if o != 1 {
				t.Fatalf("total=%d: item %d owned by %d shards, want 1", total, i, o)
			}
		}
	}
}

func TestBuildFleetCounts(t *testing.T) {
	p := Profile{Hosts: 600, APMHosts: 30, Containers: 6000}
	f := buildFleet(p)
	if len(f.Hosts) != 600 || len(f.Containers) != 6000 {
		t.Fatalf("fleet sizes hosts=%d containers=%d", len(f.Hosts), len(f.Containers))
	}
	apm := 0
	for _, h := range f.Hosts {
		if h.APM {
			apm++
		}
		if h.Name == "" {
			t.Fatal("host with empty name")
		}
	}
	if apm != 30 {
		t.Errorf("APM hosts = %d, want 30", apm)
	}
	// Every container is pinned to a real host.
	for _, c := range f.Containers {
		if c.Host == nil || c.Host.Name == "" {
			t.Fatal("container without a host")
		}
	}
}

func TestCatalogCardinalityShardedSumsToTotal(t *testing.T) {
	p := Profile{
		CustomMetricSeries: 6100, Hosts: 600, APMHosts: 30, Containers: 6000,
		CPUsPerHost: 8, DisksPerHost: 2, MountsPerHost: 4, NICsPerHost: 2,
	}
	f := buildFleet(p)
	want := p.metricSeriesTotal()

	// Single shard owns the whole envelope.
	full := buildCatalog(p, f, Shard{Index: 0, Total: 1})
	if full.seriesTotal != want {
		t.Errorf("single-shard seriesTotal = %d, want %d", full.seriesTotal, want)
	}

	// N shards partition it: the per-pod totals sum to the whole, so cardinality
	// does NOT scale with replica count.
	for _, total := range []int{3, 6, 18} {
		sum := 0
		for idx := 0; idx < total; idx++ {
			sum += buildCatalog(p, f, Shard{Index: idx, Total: total}).seriesTotal
		}
		if sum != want {
			t.Errorf("total=%d: summed seriesTotal = %d, want %d", total, sum, want)
		}
	}
}

// TestCatalogSeriesAreDistinct guards against the mixed-radix decode regressing
// into periodic collisions: the whole catalog must contain exactly seriesTotal
// DISTINCT (name, attribute-set) pairs, not duplicates padding the count.
func TestCatalogSeriesAreDistinct(t *testing.T) {
	p := Profile{
		CustomMetricSeries: 6100, Hosts: 600, APMHosts: 30,
		CPUsPerHost: 8, DisksPerHost: 2, MountsPerHost: 4, NICsPerHost: 2,
	}
	f := buildFleet(p)
	cat := buildCatalog(p, f, Shard{Index: 0, Total: 1})

	enc := attribute.DefaultEncoder()
	seen := make(map[string]struct{}, cat.seriesTotal)
	for _, g := range cat.groups {
		for _, set := range g.series {
			key := g.name + "|" + set.Encoded(enc)
			if _, dup := seen[key]; dup {
				t.Fatalf("duplicate series: %s", key)
			}
			seen[key] = struct{}{}
		}
	}
	if len(seen) != cat.seriesTotal {
		t.Fatalf("distinct series = %d, seriesTotal = %d", len(seen), cat.seriesTotal)
	}
	if cat.capped != 0 {
		t.Errorf("default profile should not cap custom series, capped=%d", cat.capped)
	}
}
