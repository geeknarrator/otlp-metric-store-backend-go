//go:build integration

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func setupClickHouse(t *testing.T) (*ClickHouseMetricsStore, func()) {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, "clickhouse/clickhouse-server:26.2",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_USER":     "default",
			"CLICKHOUSE_PASSWORD": "test",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("starting clickhouse container: %v", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("getting container host: %v", err)
	}
	mappedPort, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("getting mapped port: %v", err)
	}

	addr := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	store, err := NewClickHouseMetricsStore(ctx, addr, "default", "default", "test")
	if err != nil {
		t.Fatalf("creating clickhouse metrics store: %v", err)
	}

	cleanup := func() {
		store.Close()
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("terminating clickhouse container: %v", err)
		}
	}

	return store, cleanup
}

func TestCreateTables(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	expectedTables := []string{
		"otel_metrics_gauge",
		"otel_metrics_gauge_series",
		"otel_metrics_sum",
		"otel_metrics_sum_series",
		"otel_metrics_histogram",
		"otel_metrics_exponential_histogram",
		"otel_metrics_summary",
	}

	for _, table := range expectedTables {
		var count uint64
		err := store.conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = 'default' AND name = $1", table,
		).Scan(&count)
		if err != nil {
			t.Fatalf("querying system.tables for %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("expected table %s to exist, got count=%d", table, count)
		}
	}
}

func TestInsertGauge(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	startTime := now - uint64(time.Minute)
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
					{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
				},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{
						Name:    "test-scope",
						Version: "1.0.0",
					},
					Metrics: []*metricspb.Metric{
						{
							Name:        "cpu.utilization",
							Description: "CPU utilization percentage",
							Unit:        "%",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes:        []*commonpb.KeyValue{{Key: "cpu", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "0"}}}},
											StartTimeUnixNano: startTime,
											TimeUnixNano:      now,
											Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	gaugeRows := MapGaugeRows(resourceMetrics)
	if err := store.InsertGauge(ctx, gaugeRows); err != nil {
		t.Fatalf("inserting gauge rows: %v", err)
	}

	var (
		seriesID uint64
		value    float64
	)
	err := store.conn.QueryRow(ctx,
		"SELECT SeriesID, Value FROM otel_metrics_gauge",
	).Scan(&seriesID, &value)
	if err != nil {
		t.Fatalf("querying gauge: %v", err)
	}
	if seriesID != gaugeRows[0].SeriesID {
		t.Errorf("expected SeriesID=%d, got %d", gaugeRows[0].SeriesID, seriesID)
	}
	if value != 42.5 {
		t.Errorf("expected Value=42.5, got %f", value)
	}
}

func TestInsertSum(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	startTime := now - uint64(time.Minute)
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
					{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
				},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{
						Name:    "test-scope",
						Version: "1.0.0",
					},
					Metrics: []*metricspb.Metric{
						{
							Name:        "http.requests.total",
							Description: "Total HTTP requests",
							Unit:        "{request}",
							Data: &metricspb.Metric_Sum{
								Sum: &metricspb.Sum{
									AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes: []*commonpb.KeyValue{
												{Key: "method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
												{Key: "status", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "200"}}},
											},
											StartTimeUnixNano: startTime,
											TimeUnixNano:      now,
											Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 1234},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	sumRows := MapSumRows(resourceMetrics)
	if err := store.InsertSum(ctx, sumRows); err != nil {
		t.Fatalf("inserting sum rows: %v", err)
	}

	var (
		seriesID uint64
		value    float64
	)
	err := store.conn.QueryRow(ctx,
		"SELECT SeriesID, Value FROM otel_metrics_sum",
	).Scan(&seriesID, &value)
	if err != nil {
		t.Fatalf("querying sum: %v", err)
	}
	if seriesID != sumRows[0].SeriesID {
		t.Errorf("expected SeriesID=%d, got %d", sumRows[0].SeriesID, seriesID)
	}
	if value != 1234 {
		t.Errorf("expected Value=1234, got %f", value)
	}
}

func TestInsertGaugeSeries(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "test-scope", Version: "1.0.0"},
					Metrics: []*metricspb.Metric{
						{
							Name:        "cpu.utilization",
							Description: "CPU utilization percentage",
							Unit:        "%",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{
											TimeUnixNano: now,
											Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
										},
										// Second data point for the same series — only one series row should be written.
										{
											TimeUnixNano: now + 1000,
											Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 43.0},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	seriesRows := MapGaugeSeriesRows(resourceMetrics)

	if len(seriesRows) != 1 {
		t.Fatalf("expected 1 deduplicated series row before insert, got %d", len(seriesRows))
	}

	if err := store.InsertGaugeSeries(ctx, seriesRows); err != nil {
		t.Fatalf("inserting gauge series rows: %v", err)
	}

	var (
		seriesID    uint64
		serviceName string
		metricName  string
		metricUnit  string
	)
	err := store.conn.QueryRow(ctx,
		"SELECT SeriesID, ServiceName, MetricName, MetricUnit FROM otel_metrics_gauge_series WHERE MetricName = 'cpu.utilization'",
	).Scan(&seriesID, &serviceName, &metricName, &metricUnit)
	if err != nil {
		t.Fatalf("querying gauge series: %v", err)
	}
	if seriesID != seriesRows[0].SeriesID {
		t.Errorf("expected SeriesID=%d, got %d", seriesRows[0].SeriesID, seriesID)
	}
	if serviceName != "test-service" {
		t.Errorf("expected ServiceName=test-service, got %s", serviceName)
	}
	if metricName != "cpu.utilization" {
		t.Errorf("expected MetricName=cpu.utilization, got %s", metricName)
	}
	if metricUnit != "%" {
		t.Errorf("expected MetricUnit=%%, got %s", metricUnit)
	}
}

func TestInsertSumSeries(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "test-scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "http.requests.total",
							Unit: "{request}",
							Data: &metricspb.Metric_Sum{
								Sum: &metricspb.Sum{
									AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*metricspb.NumberDataPoint{
										{
											TimeUnixNano: now,
											Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 100},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	seriesRows := MapSumSeriesRows(resourceMetrics)

	if err := store.InsertSumSeries(ctx, seriesRows); err != nil {
		t.Fatalf("inserting sum series rows: %v", err)
	}

	var (
		seriesID               uint64
		serviceName            string
		metricName             string
		aggregationTemporality int32
		isMonotonic            bool
	)
	err := store.conn.QueryRow(ctx,
		"SELECT SeriesID, ServiceName, MetricName, AggregationTemporality, IsMonotonic FROM otel_metrics_sum_series WHERE MetricName = 'http.requests.total'",
	).Scan(&seriesID, &serviceName, &metricName, &aggregationTemporality, &isMonotonic)
	if err != nil {
		t.Fatalf("querying sum series: %v", err)
	}
	if seriesID != seriesRows[0].SeriesID {
		t.Errorf("expected SeriesID=%d, got %d", seriesRows[0].SeriesID, seriesID)
	}
	if serviceName != "test-service" {
		t.Errorf("expected ServiceName=test-service, got %s", serviceName)
	}
	if aggregationTemporality != 2 {
		t.Errorf("expected AggregationTemporality=2, got %d", aggregationTemporality)
	}
	if !isMonotonic {
		t.Error("expected IsMonotonic=true")
	}
}

func TestGRPCToClickHouse(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	// Start gRPC server wired to the ClickHouse store.
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer("bufconn", store))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connecting to grpc server: %v", err)
	}
	defer conn.Close()

	client := colmetricspb.NewMetricsServiceClient(conn)

	// Send a gauge metric via gRPC.
	now := uint64(time.Now().UnixNano())
	_, err = client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "e2e-service"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "e2e-scope"},
						Metrics: []*metricspb.Metric{
							{
								Name: "e2e.gauge",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: now,
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 99.9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("exporting metrics via grpc: %v", err)
	}

	// Verify the data point landed in ClickHouse.
	var value float64
	err = store.conn.QueryRow(ctx,
		"SELECT Value FROM otel_metrics_gauge",
	).Scan(&value)
	if err != nil {
		t.Fatalf("querying gauge data: %v", err)
	}
	if value != 99.9 {
		t.Errorf("expected Value=99.9, got %f", value)
	}

	// Verify the series row landed and the SeriesID matches between data and series tables.
	var (
		seriesIDFromData   uint64
		seriesIDFromSeries uint64
		seriesServiceName  string
	)
	if err := store.conn.QueryRow(ctx,
		"SELECT SeriesID FROM otel_metrics_gauge",
	).Scan(&seriesIDFromData); err != nil {
		t.Fatalf("querying SeriesID from gauge data: %v", err)
	}
	if err := store.conn.QueryRow(ctx,
		"SELECT SeriesID, ServiceName FROM otel_metrics_gauge_series WHERE MetricName = 'e2e.gauge'",
	).Scan(&seriesIDFromSeries, &seriesServiceName); err != nil {
		t.Fatalf("querying gauge series: %v", err)
	}
	if seriesIDFromData != seriesIDFromSeries {
		t.Errorf("SeriesID mismatch: data table has %d, series table has %d", seriesIDFromData, seriesIDFromSeries)
	}
	if seriesServiceName != "e2e-service" {
		t.Errorf("expected ServiceName=e2e-service in series table, got %s", seriesServiceName)
	}
}
