package main

import (
	"context"
	"fmt"
	"sync"
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
//
// seenSeries is an in-process cache of SeriesIDs that have already been written
// to ClickHouse. On a cache hit, the series insert is skipped entirely, reducing
// duplicate writes to the ReplacingMergeTree series tables.
//
// The cache is intentionally unbounded — series cardinality is assumed to be low.
// On process restart the cache is cold and series rows will be re-inserted once,
// which is safe because ReplacingMergeTree handles deduplication.
// In a multi-replica deployment each replica maintains its own cache; the
// ReplacingMergeTree acts as the cross-replica correctness backstop.
type ClickHouseMetricsStore struct {
	conn        driver.Conn
	seenSeries  sync.Map // SeriesID uint64 → struct{}
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

// CreateTables executes DDL for all metric data and series tables.
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

// filterNewSeries returns only the IDs that have not yet been written to ClickHouse
// (i.e. are absent from the in-process cache), counting hits and misses.
// It does NOT update the cache — call markSeriesSeen after a successful write
// so that a failed batch.Send() does not permanently suppress the series.
func (s *ClickHouseMetricsStore) filterNewSeries(ids []uint64) (newIDs []uint64, hits, misses int) {
	for _, id := range ids {
		if _, ok := s.seenSeries.Load(id); ok {
			hits++
		} else {
			misses++
			newIDs = append(newIDs, id)
		}
	}
	return
}

// markSeriesSeen records the given IDs as successfully written to ClickHouse.
// Must be called only after a successful batch.Send() to avoid cache poisoning
// on write failures.
func (s *ClickHouseMetricsStore) markSeriesSeen(ids []uint64) {
	for _, id := range ids {
		s.seenSeries.Store(id, struct{}{})
	}
}

// InsertGaugeSeries batch-inserts gauge series rows into otel_metrics_gauge_series,
// skipping any series whose SeriesID is already in the in-process cache.
// Rows that do reach ClickHouse may still be duplicates across replicas or after
// a restart — ReplacingMergeTree deduplicates them on background merges.
func (s *ClickHouseMetricsStore) InsertGaugeSeries(ctx context.Context, rows []GaugeSeriesRow) error {
	ids := make([]uint64, len(rows))
	for i, r := range rows {
		ids[i] = r.SeriesID
	}
	newIDs, hits, misses := s.filterNewSeries(ids)
	seriesCacheHitCounter.Add(ctx, int64(hits))
	seriesCacheMissCounter.Add(ctx, int64(misses))

	if len(newIDs) == 0 {
		return nil
	}

	// Build a lookup set so we only append rows for new series.
	newSet := make(map[uint64]struct{}, len(newIDs))
	for _, id := range newIDs {
		newSet[id] = struct{}{}
	}

	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_gauge_series")
	if err != nil {
		return fmt.Errorf("preparing gauge series batch: %w", err)
	}
	for _, r := range rows {
		if _, ok := newSet[r.SeriesID]; !ok {
			continue
		}
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
	if err := batch.Send(); err != nil {
		return err
	}
	s.markSeriesSeen(newIDs)
	gaugeSeriesWrittenCounter.Add(ctx, int64(len(newIDs)))
	return nil
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

// InsertSumSeries batch-inserts sum series rows into otel_metrics_sum_series,
// skipping any series whose SeriesID is already in the in-process cache.
// Rows that do reach ClickHouse may still be duplicates across replicas or after
// a restart — ReplacingMergeTree deduplicates them on background merges.
func (s *ClickHouseMetricsStore) InsertSumSeries(ctx context.Context, rows []SumSeriesRow) error {
	ids := make([]uint64, len(rows))
	for i, r := range rows {
		ids[i] = r.SeriesID
	}
	newIDs, hits, misses := s.filterNewSeries(ids)
	seriesCacheHitCounter.Add(ctx, int64(hits))
	seriesCacheMissCounter.Add(ctx, int64(misses))

	if len(newIDs) == 0 {
		return nil
	}

	newSet := make(map[uint64]struct{}, len(newIDs))
	for _, id := range newIDs {
		newSet[id] = struct{}{}
	}

	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_sum_series")
	if err != nil {
		return fmt.Errorf("preparing sum series batch: %w", err)
	}
	for _, r := range rows {
		if _, ok := newSet[r.SeriesID]; !ok {
			continue
		}
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
	if err := batch.Send(); err != nil {
		return err
	}
	s.markSeriesSeen(newIDs)
	sumSeriesWrittenCounter.Add(ctx, int64(len(newIDs)))
	return nil
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.conn.Close()
}
