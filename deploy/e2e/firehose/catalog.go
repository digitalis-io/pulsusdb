package main

import (
	"context"
	"math/rand"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// metricGroup is one metric name with a fixed set of attribute combinations
// (its series). Grouping series by name mirrors how a real scrape lands: one
// metric, many labelled datapoints.
type metricGroup struct {
	name string
	unit string
	// series holds the attribute set for each timeseries under this metric.
	series []attribute.Set
}

// Catalog is the fixed, bounded set of metric series this pod emits. Its
// cardinality is deterministic (a function of the profile, not the replica
// count) and is split into this pod's shard at build time, so summing all pods
// reproduces the whole envelope exactly once per interval.
type Catalog struct {
	groups      []metricGroup
	seriesTotal int
	// capped is how many requested custom series exceeded the catalog's
	// distinct capacity and were dropped (0 in the normal case). Logged at
	// startup so a too-large CUSTOM_METRIC_SERIES override is never silent.
	capped int
}

// custom-metric building blocks. The cross product (names x combos) is far
// larger than any realistic CustomMetricSeries target, so we take a
// deterministic prefix of it to hit the exact requested cardinality with
// realistic-looking name/label shapes.
var (
	customMeasures = []struct {
		suffix string
		unit   string
	}{
		{"request.count", "{request}"},
		{"request.duration.ms", "ms"},
		{"request.errors", "{error}"},
		{"queue.depth", "{item}"},
		{"cache.hit.ratio", "1"},
		{"db.query.duration.ms", "ms"},
		{"saga.step.count", "{step}"},
		{"business.value.gbp", "GBP"},
	}
	customEndpoints = []string{
		"/checkout", "/pay", "/orders", "/inventory", "/accounts",
		"/auth", "/search", "/pricing", "/notify", "/ledger",
	}
	customStatus = []string{"2xx", "4xx", "5xx"}

	// Infra host metrics modelled like a real node-exporter / OTel hostmetrics
	// receiver: most metrics fan out over per-host resources (cpu cores, block
	// devices, mount points, network interfaces) x a direction/state set, so one
	// host emits hundreds of series rather than a flat handful. axis.dim names a
	// per-host dimension whose width comes from the profile (CPUs/Disks/Mounts/
	// NICsPerHost); axis.static is a fixed label value set.
	infraHostMetrics = []infraTemplate{
		{"system.cpu.time", "s", []infraAxis{{key: "cpu", dim: "cpu"}, {key: "state", static: cpuStates}}},
		{"system.cpu.utilization", "1", []infraAxis{{key: "cpu", dim: "cpu"}, {key: "state", static: cpuStates}}},
		{"system.memory.usage", "By", []infraAxis{{key: "state", static: memStates}}},
		{"system.memory.utilization", "1", []infraAxis{{key: "state", static: memStates}}},
		{"system.paging.operations", "{operation}", []infraAxis{{key: "direction", static: directions}, {key: "type", static: pagingTypes}}},
		{"system.disk.io", "By", []infraAxis{{key: "device", dim: "disk"}, {key: "direction", static: directions}}},
		{"system.disk.operations", "{operation}", []infraAxis{{key: "device", dim: "disk"}, {key: "direction", static: directions}}},
		{"system.disk.io_time", "s", []infraAxis{{key: "device", dim: "disk"}}},
		{"system.disk.operation_time", "s", []infraAxis{{key: "device", dim: "disk"}, {key: "direction", static: directions}}},
		{"system.filesystem.usage", "By", []infraAxis{{key: "mountpoint", dim: "mount"}, {key: "state", static: fsStates}}},
		{"system.filesystem.utilization", "1", []infraAxis{{key: "mountpoint", dim: "mount"}}},
		{"system.filesystem.inodes.usage", "{inode}", []infraAxis{{key: "mountpoint", dim: "mount"}, {key: "state", static: fsInodeStates}}},
		{"system.network.io", "By", []infraAxis{{key: "device", dim: "nic"}, {key: "direction", static: directions}}},
		{"system.network.packets", "{packet}", []infraAxis{{key: "device", dim: "nic"}, {key: "direction", static: directions}}},
		{"system.network.errors", "{error}", []infraAxis{{key: "device", dim: "nic"}, {key: "direction", static: directions}}},
		{"system.network.dropped", "{packet}", []infraAxis{{key: "device", dim: "nic"}, {key: "direction", static: directions}}},
		{"system.network.connections", "{connection}", []infraAxis{{key: "state", static: tcpStates}}},
		{"system.load.1m", "1", nil},
		{"system.load.5m", "1", nil},
		{"system.load.15m", "1", nil},
		{"system.processes.count", "{process}", []infraAxis{{key: "status", static: procStates}}},
		{"system.processes.created", "{process}", nil},
		{"system.uptime", "s", nil},
		{"system.context_switches", "{switch}", nil},
	}

	cpuStates             = []string{"user", "system", "idle", "iowait", "nice", "irq", "softirq", "steal"}
	memStates             = []string{"used", "free", "cached", "buffered", "slab_reclaimable"}
	fsStates              = []string{"used", "free", "reserved"}
	fsInodeStates         = []string{"used", "free"}
	directions            = []string{"read", "write"}
	pagingTypes           = []string{"major", "minor"}
	tcpStates             = []string{"ESTABLISHED", "TIME_WAIT", "CLOSE_WAIT", "LISTEN", "SYN_SENT"}
	procStates            = []string{"running", "sleeping", "zombie"}
	infraContainerMetrics = []struct {
		name string
		unit string
	}{
		{"container.cpu.utilization", "1"},
		{"container.memory.usage", "By"},
		{"container.memory.utilization", "1"},
		{"container.disk.io", "By"},
		{"container.network.io", "By"},
		{"container.restarts", "{restart}"},
		{"container.threads", "{thread}"},
		{"container.fs.usage", "By"},
		{"container.uptime", "s"},
		{"container.oom.events", "{event}"},
	}

	catalogScope = instrumentation.Scope{Name: "firehose/catalog"}
)

// buildCatalog constructs this shard's owned metric series. Series are assigned
// a global round-robin index across the whole fleet (custom first, then infra)
// and the shard keeps the ones it owns, grouped by metric name for emit.
func buildCatalog(p Profile, fleet *Fleet, shard Shard) *Catalog {
	c := &Catalog{}
	byName := map[string]int{} // metric name -> index into c.groups
	idx := 0                   // global series index for sharding

	add := func(name, unit string, attrs ...attribute.KeyValue) {
		own := shard.owns(idx)
		idx++
		if !own {
			return
		}
		gi, ok := byName[name]
		if !ok {
			gi = len(c.groups)
			c.groups = append(c.groups, metricGroup{name: name, unit: unit})
			byName[name] = gi
		}
		c.groups[gi].series = append(c.groups[gi].series, attribute.NewSet(attrs...))
		c.seriesTotal++
	}

	// Custom metrics: enumerate the cartesian product
	// measure x service x endpoint x status x tier x region as a mixed-radix
	// counter and take the first CustomMetricSeries tuples. Decoding n across
	// distinct radices makes every n map to a UNIQUE tuple (hence a unique
	// name+attrs series) for n < the product, so the catalog holds exactly the
	// requested distinct cardinality -- no periodic collisions.
	nm, ns := len(customMeasures), len(fleetServices)
	ne, nst := len(customEndpoints), len(customStatus)
	nt, nr := len(fleetTiers), len(fleetRegions)
	maxSeries := nm * ns * ne * nst * nt * nr
	target := p.CustomMetricSeries
	if target > maxSeries {
		c.capped = target - maxSeries
		target = maxSeries
	}
	for n := 0; n < target; n++ {
		x := n
		ri := x % nr
		x /= nr
		ti := x % nt
		x /= nt
		sti := x % nst
		x /= nst
		ei := x % ne
		x /= ne
		si := x % ns
		x /= ns
		mi := x % nm
		m := customMeasures[mi]
		svc := fleetServices[si]
		add("app."+svc+"."+m.suffix, m.unit,
			attribute.String("service", svc),
			attribute.String("endpoint", customEndpoints[ei]),
			attribute.String("status_class", customStatus[sti]),
			attribute.String("tier", fleetTiers[ti]),
			attribute.String("region", fleetRegions[ri].region),
		)
	}

	// Infra host metrics: a fanned-out node-exporter-like set per host.
	for _, h := range fleet.Hosts {
		p.forEachHostSeries(h, add)
	}
	// Infra container metrics: standard set per container (off by default).
	for _, ct := range fleet.Containers {
		for i := 0; i < p.InfraMetricsPerCont && i < len(infraContainerMetrics); i++ {
			cm := infraContainerMetrics[i]
			add(cm.name, cm.unit,
				attribute.String("container.name", ct.Name),
				attribute.String("host.name", ct.Host.Name),
				attribute.String("k8s.namespace.name", ct.Namespace),
			)
		}
	}
	return c
}

// resourceMetrics renders the whole catalog as a single ResourceMetrics with
// every owned series carrying a datapoint stamped at sim time t. Values are
// synthetic (a bounded random level); the point of the generator is volume and
// cardinality, not signal shape.
func (c *Catalog) resourceMetrics(res *resource.Resource, t time.Time, rng *rand.Rand) *metricdata.ResourceMetrics {
	metrics := make([]metricdata.Metrics, 0, len(c.groups))
	for _, g := range c.groups {
		dps := make([]metricdata.DataPoint[float64], 0, len(g.series))
		for _, set := range g.series {
			dps = append(dps, metricdata.DataPoint[float64]{
				Attributes: set,
				Time:       t,
				Value:      rng.Float64() * 100,
			})
		}
		metrics = append(metrics, metricdata.Metrics{
			Name: g.name,
			Unit: g.unit,
			Data: metricdata.Gauge[float64]{DataPoints: dps},
		})
	}
	return &metricdata.ResourceMetrics{
		Resource:     res,
		ScopeMetrics: []metricdata.ScopeMetrics{{Scope: catalogScope, Metrics: metrics}},
	}
}

// runMetricCatalog emits the owned catalog once per SIM interval, stamping each
// batch at the current sim-clock instant, until ctx is cancelled. The wall
// gap between emits is the sim interval compressed by Speedup.
func runMetricCatalog(ctx context.Context, exp sdkmetric.Exporter, cat *Catalog, res *resource.Resource,
	p Profile, startWall time.Time, nowFn func() time.Time,
) {
	if cat.seriesTotal == 0 {
		return
	}
	wallGap := time.Duration(float64(p.MetricIntervalSec) * float64(time.Second) / p.Speedup)
	if wallGap < time.Millisecond {
		wallGap = time.Millisecond
	}
	rng := rand.New(rand.NewSource(1))
	ticker := time.NewTicker(wallGap)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		rm := cat.resourceMetrics(res, p.simClock(startWall, nowFn()), rng)
		if err := exp.Export(ctx, rm); err != nil {
			exportErrors.WithLabelValues("metric").Inc()
			continue
		}
		emittedRecords.WithLabelValues("metric_datapoint").Add(float64(cat.seriesTotal))
	}
}
