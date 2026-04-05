package main

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
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

// -- MapGaugeSeriesRows --

func gaugeResourceMetrics(svc, metricName string, dpAttrs map[string]string, numDataPoints int) []*metricspb.ResourceMetrics {
	dps := make([]*metricspb.NumberDataPoint, numDataPoints)
	for i := range dps {
		kv := make([]*commonpb.KeyValue, 0, len(dpAttrs))
		for k, v := range dpAttrs {
			kv = append(kv, &commonpb.KeyValue{
				Key:   k,
				Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
			})
		}
		dps[i] = &metricspb.NumberDataPoint{
			Attributes: kv,
			Value:      &metricspb.NumberDataPoint_AsDouble{AsDouble: float64(i)},
		}
	}
	return []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: svc}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "test-scope"},
					Metrics: []*metricspb.Metric{
						{
							Name:        metricName,
							Description: metricName + " description",
							Unit:        "1",
							Data:        &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: dps}},
						},
					},
				},
			},
		},
	}
}

func TestMapGaugeSeriesRows_DeduplicatesWithinBatch(t *testing.T) {
	// 3 data points for the same series — only 1 series row expected.
	rm := gaugeResourceMetrics("svc", "cpu", map[string]string{"cpu": "0"}, 3)
	series := MapGaugeSeriesRows(rm)
	if len(series) != 1 {
		t.Errorf("expected 1 series row, got %d", len(series))
	}
}

func TestMapGaugeSeriesRows_PreservesDistinctSeries(t *testing.T) {
	// Two metrics = two distinct series.
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "scope"},
					Metrics: []*metricspb.Metric{
						{Name: "cpu", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{}}}}},
						{Name: "memory", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{}}}}},
					},
				},
			},
		},
	}
	series := MapGaugeSeriesRows(rm)
	if len(series) != 2 {
		t.Errorf("expected 2 series rows, got %d", len(series))
	}
}

func TestMapGaugeSeriesRows_MetadataIsPreserved(t *testing.T) {
	rm := gaugeResourceMetrics("my-svc", "latency", map[string]string{"region": "eu"}, 1)
	rm[0].ScopeMetrics[0].Metrics[0].Description = "p99 latency"
	rm[0].ScopeMetrics[0].Metrics[0].Unit = "ms"

	series := MapGaugeSeriesRows(rm)
	if len(series) != 1 {
		t.Fatalf("expected 1 series row, got %d", len(series))
	}
	s := series[0]
	if s.ServiceName != "my-svc" {
		t.Errorf("ServiceName: want my-svc, got %s", s.ServiceName)
	}
	if s.MetricName != "latency" {
		t.Errorf("MetricName: want latency, got %s", s.MetricName)
	}
	if s.MetricDescription != "p99 latency" {
		t.Errorf("MetricDescription: want 'p99 latency', got %s", s.MetricDescription)
	}
	if s.MetricUnit != "ms" {
		t.Errorf("MetricUnit: want ms, got %s", s.MetricUnit)
	}
	if s.Attributes["region"] != "eu" {
		t.Errorf("Attributes[region]: want eu, got %s", s.Attributes["region"])
	}
}

func TestMapGaugeSeriesRows_EmptyInput(t *testing.T) {
	series := MapGaugeSeriesRows(nil)
	if len(series) != 0 {
		t.Errorf("expected 0 series rows for nil input, got %d", len(series))
	}
}

// -- MapSumSeriesRows --

func TestMapSumSeriesRows_DeduplicatesWithinBatch(t *testing.T) {
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "requests",
							Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
								AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
								IsMonotonic:            true,
								DataPoints: []*metricspb.NumberDataPoint{
									{Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1}},
									{Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 2}},
								},
							}},
						},
					},
				},
			},
		},
	}
	series := MapSumSeriesRows(rm)
	if len(series) != 1 {
		t.Errorf("expected 1 series row, got %d", len(series))
	}
}

func TestMapSumSeriesRows_SumSpecificFieldsPreserved(t *testing.T) {
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "requests",
							Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
								AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
								IsMonotonic:            true,
								DataPoints:             []*metricspb.NumberDataPoint{{}},
							}},
						},
					},
				},
			},
		},
	}
	series := MapSumSeriesRows(rm)
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
