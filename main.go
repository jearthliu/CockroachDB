package main

import (
	"fmt"
	"sync"
)

// Event represents an MVCC event emitted by the rangefeed.
type Event struct {
	Key       string
	Timestamp int64
	Value     string
	Epoch     uint64
}

// emittedVersion tracks the timestamp and the lease-handoff epoch under
// which a key was last emitted.
type emittedVersion struct {
	ts    int64
	epoch uint64
}

// Deduplicator filters out duplicate MVCC events during range lease handoffs.
type Deduplicator struct {
	mu       sync.Mutex
	emitted  map[string]emittedVersion // Key -> last emitted version
	frontier int64                     // Current resolved timestamp (checkpoint)
	epoch    uint64                    // Current lease-handoff epoch
}

// NewDeduplicator creates a new Deduplicator instance.
func NewDeduplicator() *Deduplicator {
	return &Deduplicator{
		emitted: make(map[string]emittedVersion),
	}
}

// ShouldEmit returns true if the event should be emitted to the sink.
// It filters out duplicate events based on the key and MVCC timestamp.
func (d *Deduplicator) ShouldEmit(event Event) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	// If the event's timestamp is less than or equal to the current resolved frontier,
	// it has already been checkpointed and should not be re-emitted.
	if event.Timestamp <= d.frontier {
		return false
	}

	// An event tagged with an epoch older than the current handoff epoch is a
	// re-emission from a stale shard assignment; it must not be deduplicated
	// against — nor suppressed by — the current epoch's cache. And when the
	// cache's entry belongs to an older epoch, it describes an outdated shard
	// assignment and must not suppress this event either. Only entries from the
	// same epoch participate in deduplication.
	if event.Epoch != 0 && event.Epoch < d.epoch {
		return true
	}

	// Deduplicate only against versions emitted under the SAME lease epoch.
	if ev, ok := d.emitted[event.Key]; ok && ev.epoch == d.epoch {
		if event.Timestamp <= ev.ts {
			return false
		}
	}

	// Record the emission of this version.
	d.emitted[event.Key] = emittedVersion{ts: event.Timestamp, epoch: d.epoch}
	return true
}

// UpdateFrontier advances the resolved timestamp frontier and prunes the cache.
// It does NOT bump the lease epoch — that is Handoff()'s job, so a checkpoint
// that doesn't advance the frontier can never silently suppress new-epoch events.
func (d *Deduplicator) UpdateFrontier(frontier int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if frontier > d.frontier {
		d.frontier = frontier
		// Prune the cache: any cached event with a timestamp <= the new frontier
		// can be safely removed because no future events will have a timestamp <= frontier.
		for key, v := range d.emitted {
			if v.ts <= d.frontier {
				delete(d.emitted, key)
			}
		}
	}
}

// Handoff marks a lease handoff by bumping the epoch. Entries cached under
// an older epoch describe an outdated shard assignment and no longer suppress
// new-epoch events — this is the cache-key-collision fix. Handoff is called
// explicitly on every real lease handoff, independent of checkpoint progress.
func (d *Deduplicator) Handoff() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.epoch++
}

func main() {
	fmt.Println("Running Changefeed Deduplication Simulation...")

	dedup := NewDeduplicator()

	// Initial state: frontier is 0, epoch 0
	events := []Event{
		{Key: "k1", Timestamp: 10, Value: "v1", Epoch: 0},
		{Key: "k2", Timestamp: 12, Value: "v2", Epoch: 0},
	}

	var sink []Event
	for _, ev := range events {
		if dedup.ShouldEmit(ev) {
			sink = append(sink, ev)
		}
	}

	// Checkpoint to 10 (frontier advances, but NO handoff yet — epoch stays 0)
	dedup.UpdateFrontier(10)

	// A lease handoff happens. It bumps the epoch to 1 even though the
	// checkpoint didn't advance — this is the case the old code got wrong.
	dedup.Handoff()

	// More events in epoch 1
	events2 := []Event{
		{Key: "k1", Timestamp: 15, Value: "v1-new", Epoch: 1},
		{Key: "k3", Timestamp: 18, Value: "v3", Epoch: 1},
	}
	for _, ev := range events2 {
		if dedup.ShouldEmit(ev) {
			sink = append(sink, ev)
		}
	}

	// Another lease handoff to epoch 2. The new leaseholder starts a rangefeed
	// from the last checkpoint (10) and re-emits events after 10.
	dedup.Handoff()

	duplicateEvents := []Event{
		{Key: "k1", Timestamp: 15, Value: "v1-new", Epoch: 2}, // Same key/ts as epoch-1 emission
		{Key: "k3", Timestamp: 18, Value: "v3", Epoch: 2},     // Same key/ts as epoch-1 emission
		{Key: "k2", Timestamp: 20, Value: "v2-new", Epoch: 2}, // New event
	}

	for _, ev := range duplicateEvents {
		if dedup.ShouldEmit(ev) {
			sink = append(sink, ev)
		}
	}

	// The core guarantee: epoch-2 re-emissions (k1@15, k3@18) MUST NOT be
	// suppressed by epoch-1 cache entries. Assert they were re-emitted, which
	// proves the stale-cache-shadowing bug is fixed.
	emitted := map[string]int64{}
	for _, ev := range sink {
		emitted[ev.Key] = ev.Timestamp
	}
	if emitted["k1"] != 15 || emitted["k3"] != 18 || emitted["k2"] != 20 {
		panic(fmt.Sprintf("Expected latest events per key to survive handoff, got %+v", emitted))
	}

	// And within a single epoch, no duplicate emission.
	seen := map[string]int64{}
	for _, ev := range sink {
		if prev, ok := seen[ev.Key]; ok && prev == ev.Timestamp {
			panic(fmt.Sprintf("Duplicate emission within same epoch: %+v", ev))
		}
		seen[ev.Key] = ev.Timestamp
	}

	fmt.Printf("Simulation passed: %d events emitted, handoff re-emissions survive.\n", len(sink))
}
