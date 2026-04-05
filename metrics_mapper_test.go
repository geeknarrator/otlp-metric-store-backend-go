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

// -- GaugeSeriesRowsFrom --

func gaugeRow(seriesID uint64, svc, metric string) GaugeRow {
	return GaugeRow{
		SeriesID:    seriesID,
		ServiceName: svc,
		MetricName:  metric,
		ResourceAttributes: map[string]string{"host": "a"},
		ScopeAttributes:    map[string]string{},
		Attributes:         map[string]string{},
	}
}

func TestGaugeSeriesRowsFrom_DeduplicatesSameSeriesID(t *testing.T) {
	rows := []GaugeRow{
		gaugeRow(42, "svc", "cpu"),
		gaugeRow(42, "svc", "cpu"),
		gaugeRow(42, "svc", "cpu"),
	}
	series := GaugeSeriesRowsFrom(rows)
	if len(series) != 1 {
		t.Errorf("expected 1 unique series row, got %d", len(series))
	}
}

func TestGaugeSeriesRowsFrom_PreservesDistinctSeriesIDs(t *testing.T) {
	rows := []GaugeRow{
		gaugeRow(1, "svc", "cpu"),
		gaugeRow(2, "svc", "memory"),
		gaugeRow(3, "svc", "disk"),
	}
	series := GaugeSeriesRowsFrom(rows)
	if len(series) != 3 {
		t.Errorf("expected 3 series rows, got %d", len(series))
	}
}

func TestGaugeSeriesRowsFrom_EmptyInput(t *testing.T) {
	series := GaugeSeriesRowsFrom(nil)
	if len(series) != 0 {
		t.Errorf("expected 0 series rows for nil input, got %d", len(series))
	}
}

func TestGaugeSeriesRowsFrom_MetadataIsPreserved(t *testing.T) {
	row := GaugeRow{
		SeriesID:              99,
		ServiceName:           "my-svc",
		MetricName:            "latency",
		MetricDescription:     "p99 latency",
		MetricUnit:            "ms",
		ResourceAttributes:    map[string]string{"host": "h1"},
		ResourceSchemaUrl:     "https://schema.example.com",
		ScopeName:             "my-scope",
		ScopeVersion:          "1.0",
		ScopeAttributes:       map[string]string{"env": "prod"},
		ScopeDroppedAttrCount: 2,
		ScopeSchemaUrl:        "https://scope.example.com",
		Attributes:            map[string]string{"region": "eu"},
	}
	series := GaugeSeriesRowsFrom([]GaugeRow{row})
	if len(series) != 1 {
		t.Fatalf("expected 1 series row, got %d", len(series))
	}
	s := series[0]
	if s.SeriesID != row.SeriesID { t.Errorf("SeriesID mismatch") }
	if s.ServiceName != row.ServiceName { t.Errorf("ServiceName mismatch") }
	if s.MetricName != row.MetricName { t.Errorf("MetricName mismatch") }
	if s.MetricDescription != row.MetricDescription { t.Errorf("MetricDescription mismatch") }
	if s.MetricUnit != row.MetricUnit { t.Errorf("MetricUnit mismatch") }
	if s.ScopeName != row.ScopeName { t.Errorf("ScopeName mismatch") }
	if s.ScopeDroppedAttrCount != row.ScopeDroppedAttrCount { t.Errorf("ScopeDroppedAttrCount mismatch") }
}

// -- SumSeriesRowsFrom --

func sumRow(seriesID uint64, svc, metric string, temporality int32, monotonic bool) SumRow {
	return SumRow{
		GaugeRow:               gaugeRow(seriesID, svc, metric),
		AggregationTemporality: temporality,
		IsMonotonic:            monotonic,
	}
}

func TestSumSeriesRowsFrom_DeduplicatesSameSeriesID(t *testing.T) {
	rows := []SumRow{
		sumRow(42, "svc", "requests", 2, true),
		sumRow(42, "svc", "requests", 2, true),
	}
	series := SumSeriesRowsFrom(rows)
	if len(series) != 1 {
		t.Errorf("expected 1 unique series row, got %d", len(series))
	}
}

func TestSumSeriesRowsFrom_SumSpecificFieldsPreserved(t *testing.T) {
	rows := []SumRow{
		sumRow(1, "svc", "requests", 2, true),
	}
	series := SumSeriesRowsFrom(rows)
	if len(series) != 1 {
		t.Fatalf("expected 1 series row, got %d", len(series))
	}
	if series[0].AggregationTemporality != 2 {
		t.Errorf("expected AggregationTemporality=2, got %d", series[0].AggregationTemporality)
	}
	if !series[0].IsMonotonic {
		t.Error("expected IsMonotonic=true")
	}
}
