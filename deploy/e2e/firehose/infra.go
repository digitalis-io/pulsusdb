package main

import (
	"strconv"

	"go.opentelemetry.io/otel/attribute"
)

// infraAxis is one label dimension of an infra metric. Exactly one of static
// (a fixed value set) or dim (a per-host resource kind: "cpu"|"disk"|"mount"|
// "nic", whose width comes from the profile) is set.
type infraAxis struct {
	key    string
	static []string
	dim    string
}

// infraTemplate is one infra metric name whose series are the cartesian product
// of its axes -- mirroring how a hostmetrics receiver reports, e.g.,
// system.cpu.time once per (cpu, state).
type infraTemplate struct {
	name string
	unit string
	axes []infraAxis
}

// dimValues returns the per-host resource labels for a dynamic dimension,
// scaled to the profile's fan-out width.
func (p Profile) dimValues(dim string) []string {
	n := 0
	switch dim {
	case "cpu":
		n = p.CPUsPerHost
	case "disk":
		n = p.DisksPerHost
	case "mount":
		n = p.MountsPerHost
	case "nic":
		n = p.NICsPerHost
	}
	if n < 1 {
		n = 1
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		switch dim {
		case "cpu":
			out[i] = "cpu" + strconv.Itoa(i)
		case "disk":
			out[i] = "sd" + string(rune('a'+i%26))
		case "mount":
			if i == 0 {
				out[i] = "/"
			} else {
				out[i] = "/mnt/vol" + strconv.Itoa(i)
			}
		case "nic":
			out[i] = "eth" + strconv.Itoa(i)
		}
	}
	return out
}

// values returns the label values for one axis under this profile.
func (p Profile) axisValues(a infraAxis) []string {
	if a.dim != "" {
		return p.dimValues(a.dim)
	}
	return a.static
}

// infraSeriesPerHost is the fanned-out series count a single host emits: the
// sum over templates of the product of their axis widths.
func (p Profile) infraSeriesPerHost() int {
	total := 0
	for _, t := range infraHostMetrics {
		n := 1
		for _, a := range t.axes {
			n *= len(p.axisValues(a))
		}
		total += n
	}
	return total
}

// forEachHostSeries invokes add(name, unit, attrs...) once per infra series for
// one host: base host identity labels plus one value from every axis (cartesian
// product across axes).
func (p Profile) forEachHostSeries(h *Host, add func(name, unit string, attrs ...attribute.KeyValue)) {
	base := []attribute.KeyValue{
		attribute.String("host.name", h.Name),
		attribute.String("region", h.Region),
		attribute.String("zone", h.Zone),
		attribute.String("tier", h.Tier),
	}
	for _, t := range infraHostMetrics {
		if len(t.axes) == 0 {
			add(t.name, t.unit, base...)
			continue
		}
		// Iterate the cartesian product of axis value indices.
		sizes := make([]int, len(t.axes))
		vals := make([][]string, len(t.axes))
		combos := 1
		for i, a := range t.axes {
			vals[i] = p.axisValues(a)
			sizes[i] = len(vals[i])
			combos *= sizes[i]
		}
		idx := make([]int, len(t.axes))
		for c := 0; c < combos; c++ {
			attrs := make([]attribute.KeyValue, len(base), len(base)+len(t.axes))
			copy(attrs, base)
			for i, a := range t.axes {
				attrs = append(attrs, attribute.String(a.key, vals[i][idx[i]]))
			}
			add(t.name, t.unit, attrs...)
			// increment mixed-radix counter
			for i := len(idx) - 1; i >= 0; i-- {
				idx[i]++
				if idx[i] < sizes[i] {
					break
				}
				idx[i] = 0
			}
		}
	}
}
