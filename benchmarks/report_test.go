package main

import (
	"testing"
	"time"
)

func TestStatsPercentile(t *testing.T) {
	var s stats
	s.add(10 * time.Millisecond)
	s.add(20 * time.Millisecond)
	s.add(30 * time.Millisecond)
	s.add(40 * time.Millisecond)
	s.add(50 * time.Millisecond)
	if got := s.pct(50); got != 30*time.Millisecond {
		t.Fatalf("p50 = %s, want 30ms", got)
	}
	if got := s.avg(); got != 30*time.Millisecond {
		t.Fatalf("avg = %s, want 30ms", got)
	}
}
