package main

import (
	"fmt"
	"sort"
	"time"

	xxhash "github.com/cespare/xxhash/v2"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// serviceName extracts the service.name from resource attributes, returning "" if not found.
func serviceName(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	for _, attr := range resource.GetAttributes() {
		if attr.GetKey() == "service.name" {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

// kvToMap converts a slice of OTLP KeyValue pairs to a Go map.
func kvToMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to its string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.GetIntValue())
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", v.GetDoubleValue())
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", v.GetBoolValue())
	default:
		return fmt.Sprintf("%v", v)
	}
}

// computeSeriesID returns a stable uint64 hash that uniquely identifies a
// metric series by its identifying dimensions: service name, metric name,
// resource attributes, scope attributes, and data point attributes.
//
// Map keys are sorted before hashing to ensure the result is independent of
// Go's non-deterministic map iteration order.
//
// Fields are separated by null bytes to prevent collisions between inputs
// like ("a", "bc") and ("ab", "c").
func computeSeriesID(serviceName, metricName string, resourceAttrs, scopeAttrs, dpAttrs map[string]string) uint64 {
	h := xxhash.New()
	writeField := func(s string) {
		h.Write([]byte(s))
		h.Write([]byte{0x00})
	}
	writeMap := func(m map[string]string) {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			writeField(k)
			writeField(m[k])
		}
		h.Write([]byte{0xff}) // separator between attribute maps
	}

	writeField(serviceName)
	writeField(metricName)
	writeMap(resourceAttrs)
	writeMap(scopeAttrs)
	writeMap(dpAttrs)
	return h.Sum64()
}

// nanosToTime converts a uint64 nanoseconds-since-epoch to time.Time.
func nanosToTime(nanos uint64) time.Time {
	return time.Unix(0, int64(nanos))
}

// numberDataPointValue extracts the float64 value from a NumberDataPoint.
func numberDataPointValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}

// MapGaugeRows converts an ExportMetricsServiceRequest into GaugeRows
// for all Gauge metrics found in the request.
func MapGaugeRows(resourceMetrics []*metricspb.ResourceMetrics) []GaugeRow {
	var rows []GaugeRow
	for _, rm := range resourceMetrics {
		svcName := serviceName(rm.GetResource())
		resAttrs := kvToMap(rm.GetResource().GetAttributes())
		resSchemaUrl := rm.GetSchemaUrl()

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())

			for _, metric := range sm.GetMetrics() {
				gauge := metric.GetGauge()
				if gauge == nil {
					continue
				}
				for _, dp := range gauge.GetDataPoints() {
					dpAttrs := kvToMap(dp.GetAttributes())
					rows = append(rows, GaugeRow{
						SeriesID:              computeSeriesID(svcName, metric.GetName(), resAttrs, scopeAttrs, dpAttrs),
						ResourceAttributes:    resAttrs,
						ResourceSchemaUrl:     resSchemaUrl,
						ScopeName:             scope.GetName(),
						ScopeVersion:          scope.GetVersion(),
						ScopeAttributes:       scopeAttrs,
						ScopeDroppedAttrCount: scope.GetDroppedAttributesCount(),
						ScopeSchemaUrl:        sm.GetSchemaUrl(),
						ServiceName:           svcName,
						MetricName:            metric.GetName(),
						MetricDescription:     metric.GetDescription(),
						MetricUnit:            metric.GetUnit(),
						Attributes:            dpAttrs,
						StartTimeUnix:         nanosToTime(dp.GetStartTimeUnixNano()),
						TimeUnix:              nanosToTime(dp.GetTimeUnixNano()),
						Value:                 numberDataPointValue(dp),
						Flags:                 dp.GetFlags(),
					})
				}
			}
		}
	}
	return rows
}

// MapSumRows converts an ExportMetricsServiceRequest into SumRows
// for all Sum metrics found in the request.
func MapSumRows(resourceMetrics []*metricspb.ResourceMetrics) []SumRow {
	var rows []SumRow
	for _, rm := range resourceMetrics {
		svcName := serviceName(rm.GetResource())
		resAttrs := kvToMap(rm.GetResource().GetAttributes())
		resSchemaUrl := rm.GetSchemaUrl()

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())

			for _, metric := range sm.GetMetrics() {
				sum := metric.GetSum()
				if sum == nil {
					continue
				}
				for _, dp := range sum.GetDataPoints() {
					dpAttrs := kvToMap(dp.GetAttributes())
					rows = append(rows, SumRow{
						GaugeRow: GaugeRow{
							SeriesID:              computeSeriesID(svcName, metric.GetName(), resAttrs, scopeAttrs, dpAttrs),
							ResourceAttributes:    resAttrs,
							ResourceSchemaUrl:     resSchemaUrl,
							ScopeName:             scope.GetName(),
							ScopeVersion:          scope.GetVersion(),
							ScopeAttributes:       scopeAttrs,
							ScopeDroppedAttrCount: scope.GetDroppedAttributesCount(),
							ScopeSchemaUrl:        sm.GetSchemaUrl(),
							ServiceName:           svcName,
							MetricName:            metric.GetName(),
							MetricDescription:     metric.GetDescription(),
							MetricUnit:            metric.GetUnit(),
							Attributes:            dpAttrs,
							StartTimeUnix:         nanosToTime(dp.GetStartTimeUnixNano()),
							TimeUnix:              nanosToTime(dp.GetTimeUnixNano()),
							Value:                 numberDataPointValue(dp),
							Flags:                 dp.GetFlags(),
						},
						AggregationTemporality: int32(sum.GetAggregationTemporality()),
						IsMonotonic:            sum.GetIsMonotonic(),
					})
				}
			}
		}
	}
	return rows
}
