package main

import (
	"testing"
)

func TestComputeSeriesID_Stable(t *testing.T) {
	// Same inputs must always produce the same ID.
	id1 := computeSeriesID(
		"my-service", "cpu.utilization",
		map[string]string{"host": "a"},
		map[string]string{"scope": "lib"},
		map[string]string{"cpu": "0"},
	)
	id2 := computeSeriesID(
		"my-service", "cpu.utilization",
		map[string]string{"host": "a"},
		map[string]string{"scope": "lib"},
		map[string]string{"cpu": "0"},
	)
	if id1 != id2 {
		t.Errorf("expected stable SeriesID, got %d and %d", id1, id2)
	}
}

func TestComputeSeriesID_MapKeyOrderIndependent(t *testing.T) {
	// Attribute maps with the same key/value pairs but different insertion
	// order must produce the same ID.
	id1 := computeSeriesID(
		"svc", "metric",
		map[string]string{"a": "1", "b": "2"},
		map[string]string{},
		map[string]string{},
	)
	id2 := computeSeriesID(
		"svc", "metric",
		map[string]string{"b": "2", "a": "1"},
		map[string]string{},
		map[string]string{},
	)
	if id1 != id2 {
		t.Errorf("expected map-order-independent SeriesID, got %d and %d", id1, id2)
	}
}

func TestComputeSeriesID_DifferentServicesProduceDifferentIDs(t *testing.T) {
	id1 := computeSeriesID("service-a", "metric", map[string]string{}, map[string]string{}, map[string]string{})
	id2 := computeSeriesID("service-b", "metric", map[string]string{}, map[string]string{}, map[string]string{})
	if id1 == id2 {
		t.Error("expected different SeriesIDs for different service names")
	}
}

func TestComputeSeriesID_DifferentMetricNamesProduceDifferentIDs(t *testing.T) {
	id1 := computeSeriesID("svc", "cpu.utilization", map[string]string{}, map[string]string{}, map[string]string{})
	id2 := computeSeriesID("svc", "memory.usage", map[string]string{}, map[string]string{}, map[string]string{})
	if id1 == id2 {
		t.Error("expected different SeriesIDs for different metric names")
	}
}

func TestComputeSeriesID_DifferentAttributeValuesProduceDifferentIDs(t *testing.T) {
	id1 := computeSeriesID("svc", "metric", map[string]string{}, map[string]string{}, map[string]string{"cpu": "0"})
	id2 := computeSeriesID("svc", "metric", map[string]string{}, map[string]string{}, map[string]string{"cpu": "1"})
	if id1 == id2 {
		t.Error("expected different SeriesIDs for different data point attribute values")
	}
}

func TestComputeSeriesID_FieldBoundaryCollision(t *testing.T) {
	// Without null-byte separators, ("a", "bc") and ("ab", "c") would hash
	// to the same value. Verify they don't.
	id1 := computeSeriesID("a", "bc", map[string]string{}, map[string]string{}, map[string]string{})
	id2 := computeSeriesID("ab", "c", map[string]string{}, map[string]string{}, map[string]string{})
	if id1 == id2 {
		t.Error("SeriesID field boundary collision: different inputs produced the same ID")
	}
}

func TestComputeSeriesID_DifferentAttributeMapsBoundaryCollision(t *testing.T) {
	// Attribute values from different maps (resource vs scope vs dp) with the
	// same concatenated bytes must not collide.
	id1 := computeSeriesID("svc", "metric",
		map[string]string{"k": "v"},
		map[string]string{},
		map[string]string{},
	)
	id2 := computeSeriesID("svc", "metric",
		map[string]string{},
		map[string]string{"k": "v"},
		map[string]string{},
	)
	if id1 == id2 {
		t.Error("SeriesID attribute map boundary collision: resource and scope attrs produced the same ID")
	}
}
