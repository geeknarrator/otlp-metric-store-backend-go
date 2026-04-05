package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// GaugeRow represents a single gauge data point for ClickHouse insertion.
// Metadata identifying the series is stored separately in otel_metrics_gauge_series.
type GaugeRow struct {
	SeriesID      uint64
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}

// SumRow represents a single sum data point for ClickHouse insertion.
// AggregationTemporality and IsMonotonic are series-level properties stored
// in otel_metrics_sum_series, not repeated per data point.
type SumRow struct {
	GaugeRow
}

// GaugeSeriesRow holds the metadata that identifies a unique gauge series.
// It is inserted into otel_metrics_gauge_series, which uses ReplacingMergeTree
// to deduplicate rows with the same SeriesID on background merges.
type GaugeSeriesRow struct {
	SeriesID              uint64
	ResourceAttributes    map[string]string
	ResourceSchemaUrl     string
	ScopeName             string
	ScopeVersion          string
	ScopeAttributes       map[string]string
	ScopeDroppedAttrCount uint32
	ScopeSchemaUrl        string
	ServiceName           string
	MetricName            string
	MetricDescription     string
	MetricUnit            string
	Attributes            map[string]string
}

// SumSeriesRow holds the metadata that identifies a unique sum series.
// It extends GaugeSeriesRow with sum-specific fields.
type SumSeriesRow struct {
	GaugeSeriesRow
	AggregationTemporality int32
	IsMonotonic            bool
}

// MetricsStore defines the interface for storing metrics in ClickHouse.
type MetricsStore interface {
	CreateTables(ctx context.Context) error
	InsertGauge(ctx context.Context, rows []GaugeRow) error
	InsertGaugeSeries(ctx context.Context, rows []GaugeSeriesRow) error
	InsertSum(ctx context.Context, rows []SumRow) error
	InsertSumSeries(ctx context.Context, rows []SumSeriesRow) error
	Close() error
}

// ClickHouseMetricsStore implements MetricsStore using a ClickHouse connection.
type ClickHouseMetricsStore struct {
	conn driver.Conn
}

// NewClickHouseMetricsStore creates a new ClickHouseMetricsStore connected to the given address.
func NewClickHouseMetricsStore(ctx context.Context, addr string, database string, username string, password string) (*ClickHouseMetricsStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return &ClickHouseMetricsStore{conn: conn}, nil
}

// CreateTables executes DDL for all 5 metric tables.
func (s *ClickHouseMetricsStore) CreateTables(ctx context.Context) error {
	ddls := []string{
		createGaugeTableSQL,
		createGaugeSeriesTableSQL,
		createSumTableSQL,
		createSumSeriesTableSQL,
		createHistogramTableSQL,
		createExponentialHistogramTableSQL,
		createSummaryTableSQL,
	}
	for _, ddl := range ddls {
		if err := s.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("creating table: %w", err)
		}
	}
	return nil
}

// InsertGauge batch-inserts gauge rows into otel_metrics_gauge.
func (s *ClickHouseMetricsStore) InsertGauge(ctx context.Context, rows []GaugeRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_gauge")
	if err != nil {
		return fmt.Errorf("preparing gauge batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesID,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending gauge row: %w", err)
		}
	}
	return batch.Send()
}

// InsertGaugeSeries batch-inserts gauge series rows into otel_metrics_gauge_series.
// Duplicate rows for the same SeriesID are expected and harmless — ReplacingMergeTree
// deduplicates them asynchronously on background merges.
func (s *ClickHouseMetricsStore) InsertGaugeSeries(ctx context.Context, rows []GaugeSeriesRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_gauge_series")
	if err != nil {
		return fmt.Errorf("preparing gauge series batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesID,
			r.ResourceAttributes,
			r.ResourceSchemaUrl,
			r.ScopeName,
			r.ScopeVersion,
			r.ScopeAttributes,
			r.ScopeDroppedAttrCount,
			r.ScopeSchemaUrl,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			r.Attributes,
		); err != nil {
			return fmt.Errorf("appending gauge series row: %w", err)
		}
	}
	return batch.Send()
}

// InsertSum batch-inserts sum rows into otel_metrics_sum.
func (s *ClickHouseMetricsStore) InsertSum(ctx context.Context, rows []SumRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_sum")
	if err != nil {
		return fmt.Errorf("preparing sum batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesID,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending sum row: %w", err)
		}
	}
	return batch.Send()
}

// InsertSumSeries batch-inserts sum series rows into otel_metrics_sum_series.
// Duplicate rows for the same SeriesID are expected and harmless — ReplacingMergeTree
// deduplicates them asynchronously on background merges.
func (s *ClickHouseMetricsStore) InsertSumSeries(ctx context.Context, rows []SumSeriesRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_sum_series")
	if err != nil {
		return fmt.Errorf("preparing sum series batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesID,
			r.ResourceAttributes,
			r.ResourceSchemaUrl,
			r.ScopeName,
			r.ScopeVersion,
			r.ScopeAttributes,
			r.ScopeDroppedAttrCount,
			r.ScopeSchemaUrl,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			r.Attributes,
			r.AggregationTemporality,
			r.IsMonotonic,
		); err != nil {
			return fmt.Errorf("appending sum series row: %w", err)
		}
	}
	return batch.Send()
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.conn.Close()
}
