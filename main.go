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
}

// Deduplicator filters out duplicate MVCC events during range lease handoffs.
type Deduplicator struct {
	mu       sync.Mutex
	emitted  map[string]int64 // Key -> Max Timestamp emitted
	frontier int64            // Current resolved timestamp (checkpoint)
}

// NewDeduplicator creates a new Deduplicator instance.
func NewDeduplicator() *Deduplicator {
	return &Deduplicator{
		emitted: make(map[string]int64),
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

	// Check if we have already emitted this key at a timestamp >= the event's timestamp.
	if lastTimestamp, ok := d.emitted[event.Key]; ok {
		if event.Timestamp <= lastTimestamp {
			return false
		}
	}

	// Record the emission of this version.
	d.emitted[event.Key] = event.Timestamp
	return true
}

// UpdateFrontier updates the resolved timestamp frontier and prunes the cache.
func (d *Deduplicator) UpdateFrontier(frontier int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if frontier > d.frontier {
		d.frontier = frontier
		// Prune the cache: any cached event with a timestamp <= the new frontier
		// can be safely removed because no future events will have a timestamp <= frontier.
		for key, ts := range d.emitted {
			if ts <= d.frontier {
				delete(d.emitted, key)
			}
		}
	}
}

func main() {
	fmt.Println("Running Changefeed Deduplication Simulation...")

	// Create a deduplicator
	dedup := NewDeduplicator()

	// Simulate a sequence of events and lease handoffs
	// Initial state: frontier is 0
	events := []Event{
		{Key: "k1", Timestamp: 10, Value: "v1"},
		{Key: "k2", Timestamp: 12, Value: "v2"},
	}

	var sink []Event
	for _, ev := range events {
		if dedup.ShouldEmit(ev) {
			sink = append(sink, ev)
		}
	}

	// Update frontier to 10 (checkpoint)
	dedup.UpdateFrontier(10)

	// More events
	events2 := []Event{
		{Key: "k1", Timestamp: 15, Value: "v1-new"},
		{Key: "k3", Timestamp: 18, Value: "v3"},
	}
	for _, ev := range events2 {
		if dedup.ShouldEmit(ev) {
			sink = append(sink, ev)
		}
	}

	// Simulate a lease handoff. The new leaseholder starts a new rangefeed from the last checkpoint (10).
	// It re-emits events that occurred after 10, some of which were already processed (k1@15, k3@18).
	duplicateEvents := []Event{
		{Key: "k1", Timestamp: 15, Value: "v1-new"}, // Duplicate
		{Key: "k3", Timestamp: 18, Value: "v3"},     // Duplicate
		{Key: "k2", Timestamp: 20, Value: "v2-new"}, // New event
	}

	for _, ev := range duplicateEvents {
		if dedup.ShouldEmit(ev) {
			sink = append(sink, ev)
		}
	}

	// Verify the sink contents
	expected := []Event{
		{Key: "k1", Timestamp: 10, Value: "v1"},
		{Key: "k2", Timestamp: 12, Value: "v2"},
		{Key: "k1", Timestamp: 15, Value: "v1-new"},
		{Key: "k3", Timestamp: 18, Value: "v3"},
		{Key: "k2", Timestamp: 20, Value: "v2-new"},
	}

	if len(sink) != len(expected) {
		panic(fmt.Sprintf("Expected %d events, got %d", len(expected), len(sink)))
	}

	for i, ev := range sink {
		if ev != expected[i] {
			panic(fmt.Sprintf("Mismatch at index %d: expected %+v, got %+v", i, expected[i], ev))
		}
	}

	fmt.Println("Simulation passed successfully! No duplicate events emitted.")
}