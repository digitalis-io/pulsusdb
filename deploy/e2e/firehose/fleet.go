package main

import (
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
)

// Shard identifies this pod's slice of the fleet/catalog. When the generator
// runs as a StatefulSet, every pod owns a fixed, non-overlapping stripe so the
// union across pods reproduces the whole envelope exactly once -- cardinality
// and rate are independent of the replica count. Index/Total default to 0/1
// (single-pod) when no ordinal is discoverable.
type Shard struct {
	Index int
	Total int
}

// loadShard derives the pod's stripe. POD_INDEX/REPLICAS win if set; otherwise
// the ordinal is parsed from the StatefulSet pod name (HOSTNAME =
// "<name>-<ordinal>") and REPLICAS gives the total. A Deployment (random pod
// suffix) has no stable ordinal, so it collapses to 0/1 -- run the generator as
// a StatefulSet to shard across pods.
func loadShard() Shard {
	total := envInt("REPLICAS", 1)
	if total < 1 {
		total = 1
	}
	if idx := os.Getenv("POD_INDEX"); idx != "" {
		if n, err := strconv.Atoi(idx); err == nil && n >= 0 {
			return Shard{Index: n % total, Total: total}
		}
	}
	if host := os.Getenv("HOSTNAME"); host != "" {
		if i := strings.LastIndex(host, "-"); i >= 0 {
			if n, err := strconv.Atoi(host[i+1:]); err == nil && n >= 0 {
				return Shard{Index: n % total, Total: total}
			}
		}
	}
	return Shard{Index: 0, Total: total}
}

// owns reports whether item i belongs to this shard (round-robin stripe).
func (s Shard) owns(i int) bool { return i%s.Total == s.Index }

// count returns how many of n round-robin items this shard owns.
func (s Shard) count(n int) int {
	c := n / s.Total
	if s.Index < n%s.Total {
		c++
	}
	return c
}

// Host is one infra host in the fleet with stable resource identity.
type Host struct {
	Name    string
	Region  string
	Zone    string
	Tier    string
	Service string
	Env     string
	APM     bool // also emits traces
}

// Container is one container pinned to a host.
type Container struct {
	Name      string
	Image     string
	Namespace string
	Host      *Host
}

// Fleet is the deterministic set of emitting entities. It is built identically
// in every pod (same seed data), so a given host/container has the same
// identity everywhere; a pod simply emits for the subset it owns.
type Fleet struct {
	Hosts      []*Host
	Containers []*Container
}

var (
	fleetRegions = []struct {
		region string
		zones  []string
	}{
		{"europe-west2", []string{"europe-west2-a", "europe-west2-b", "europe-west2-c"}},
		{"europe-west1", []string{"europe-west1-b", "europe-west1-c", "europe-west1-d"}},
		{"us-east1", []string{"us-east1-b", "us-east1-c", "us-east1-d"}},
	}
	fleetTiers    = []string{"frontend", "middletier", "backend", "batch", "search", "ingest"}
	fleetServices = []string{
		"checkout", "payments", "orders", "inventory", "accounts", "auth",
		"notifications", "search", "pricing", "fraud", "ledger", "gateway",
	}
	fleetImages = []string{
		"app:1.42.0", "worker:2.7.1", "api:3.14.0", "sidecar:0.9.4", "batch:1.0.8",
	}
)

// buildFleet deterministically constructs the fleet from the profile counts.
func buildFleet(p Profile) *Fleet {
	f := &Fleet{
		Hosts:      make([]*Host, 0, p.Hosts),
		Containers: make([]*Container, 0, p.Containers),
	}
	for i := 0; i < p.Hosts; i++ {
		rz := fleetRegions[i%len(fleetRegions)]
		h := &Host{
			Name:    fmt.Sprintf("host-%05d", i),
			Region:  rz.region,
			Zone:    rz.zones[i%len(rz.zones)],
			Tier:    fleetTiers[i%len(fleetTiers)],
			Service: fleetServices[i%len(fleetServices)],
			APM:     i < p.APMHosts,
			Env:     "poc",
		}
		f.Hosts = append(f.Hosts, h)
	}
	if len(f.Hosts) == 0 { // guard: never build containers without a host
		return f
	}
	for i := 0; i < p.Containers; i++ {
		host := f.Hosts[i%len(f.Hosts)]
		f.Containers = append(f.Containers, &Container{
			Name:      fmt.Sprintf("%s-%06d", host.Service, i),
			Image:     fleetImages[i%len(fleetImages)],
			Namespace: host.Tier,
			Host:      host,
		})
	}
	return f
}

// ownedHosts returns the hosts this shard emits for.
func (f *Fleet) ownedHosts(s Shard) []*Host {
	if s.Total <= 1 {
		return f.Hosts
	}
	out := make([]*Host, 0, s.count(len(f.Hosts)))
	for i, h := range f.Hosts {
		if s.owns(i) {
			out = append(out, h)
		}
	}
	return out
}

// ownedAPMHosts returns the trace-emitting hosts this shard owns. When a shard
// owns none (few APM hosts, many shards), it falls back to a fleet APM host
// chosen by shard index -- so different shards spread onto different hosts
// rather than all attributing traces to the same one.
func (f *Fleet) ownedAPMHosts(s Shard) []*Host {
	all := f.ownedHosts(s)
	out := make([]*Host, 0, len(all))
	for _, h := range all {
		if h.APM {
			out = append(out, h)
		}
	}
	if len(out) > 0 {
		return out
	}
	var apm []*Host
	for _, h := range f.Hosts {
		if h.APM {
			apm = append(apm, h)
		}
	}
	if len(apm) > 0 {
		return []*Host{apm[hashMod("apm-"+strconv.Itoa(s.Index), len(apm))]}
	}
	if len(f.Hosts) > 0 {
		return []*Host{f.Hosts[hashMod("h-"+strconv.Itoa(s.Index), len(f.Hosts))]}
	}
	return nil
}

// hashMod gives a stable pseudo-random-but-deterministic index in [0,n) for a
// string key -- used to spread synthetic values without a RNG.
func hashMod(key string, n int) int {
	if n <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()) % n
}
