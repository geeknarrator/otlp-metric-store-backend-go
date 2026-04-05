package main

import "testing"

// filterNewSeries only requires a zero-value ClickHouseMetricsStore — no
// ClickHouse connection is needed for these tests.

func TestFilterNewSeries_FirstCallIsMiss(t *testing.T) {
	s := &ClickHouseMetricsStore{}
	newIDs, hits, misses := s.filterNewSeries([]uint64{1, 2, 3})
	if hits != 0 {
		t.Errorf("expected 0 hits on first call, got %d", hits)
	}
	if misses != 3 {
		t.Errorf("expected 3 misses on first call, got %d", misses)
	}
	if len(newIDs) != 3 {
		t.Errorf("expected 3 new IDs, got %d", len(newIDs))
	}
}

func TestFilterNewSeries_SecondCallWithSameIDsIsHit(t *testing.T) {
	s := &ClickHouseMetricsStore{}
	s.filterNewSeries([]uint64{1, 2, 3}) // populate cache

	newIDs, hits, misses := s.filterNewSeries([]uint64{1, 2, 3})
	if hits != 3 {
		t.Errorf("expected 3 hits on second call, got %d", hits)
	}
	if misses != 0 {
		t.Errorf("expected 0 misses on second call, got %d", misses)
	}
	if len(newIDs) != 0 {
		t.Errorf("expected 0 new IDs on second call, got %d", len(newIDs))
	}
}

func TestFilterNewSeries_MixedNewAndSeen(t *testing.T) {
	s := &ClickHouseMetricsStore{}
	s.filterNewSeries([]uint64{1, 2}) // seed 1 and 2 into cache

	newIDs, hits, misses := s.filterNewSeries([]uint64{1, 2, 3, 4})
	if hits != 2 {
		t.Errorf("expected 2 hits, got %d", hits)
	}
	if misses != 2 {
		t.Errorf("expected 2 misses, got %d", misses)
	}
	if len(newIDs) != 2 {
		t.Errorf("expected 2 new IDs, got %d", len(newIDs))
	}
	for _, id := range newIDs {
		if id != 3 && id != 4 {
			t.Errorf("unexpected new ID: %d", id)
		}
	}
}

func TestFilterNewSeries_EmptyInput(t *testing.T) {
	s := &ClickHouseMetricsStore{}
	newIDs, hits, misses := s.filterNewSeries(nil)
	if hits != 0 || misses != 0 || len(newIDs) != 0 {
		t.Errorf("expected all zeros for nil input, got hits=%d misses=%d newIDs=%d", hits, misses, len(newIDs))
	}
}

func TestFilterNewSeries_CacheIsPopulatedAfterMiss(t *testing.T) {
	s := &ClickHouseMetricsStore{}
	s.filterNewSeries([]uint64{42})

	// 42 should now be in the cache — a second call must report a hit.
	_, hits, _ := s.filterNewSeries([]uint64{42})
	if hits != 1 {
		t.Errorf("expected ID to be cached after first miss, got %d hits", hits)
	}
}
