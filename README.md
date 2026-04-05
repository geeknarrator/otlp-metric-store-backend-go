# OTLP Metric Store — Go Backend

A gRPC backend that receives OpenTelemetry metrics via the OTLP protocol and stores them in ClickHouse.

Gauge and Sum metric types are supported. Each metric type is stored across two tables:

- **Data table** (`otel_metrics_gauge`, `otel_metrics_sum`) — one row per data point, containing only `SeriesID`, timestamps, value, and flags. Partitioned by day on `TimeUnix`, ordered by `(SeriesID, TimeUnix)` for efficient time-range queries.
- **Series table** (`otel_metrics_gauge_series`, `otel_metrics_sum_series`) — one row per unique metric series, containing all metadata (service name, resource/scope/data-point attributes, metric name, description, unit). Uses `ReplacingMergeTree` for background deduplication.

`SeriesID` is a stable `uint64` hash computed deterministically from the series' identifying dimensions (service name, metric name, and all attribute maps), requiring no database round-trip.

### Series deduplication

Series metadata is written only once per unique series using a two-layer strategy:

1. **In-process cache** — each instance maintains a `sync.Map` of seen `SeriesID`s. On a cache hit the series insert is skipped entirely, so steady-state throughput produces zero redundant series writes per replica.
2. **`ReplacingMergeTree`** — any duplicates that do reach ClickHouse (on startup before the cache warms, or across replicas) are deduplicated asynchronously on background merges.

Cache hits and misses are tracked via OTel counters. On process restart the cache is cold and each series is re-inserted once, which is safe.

## Known limitations

### Histogram, ExponentialHistogram, and Summary metrics

The ClickHouse tables for these metric types are created at startup but no Go types, mappers, or insert methods exist for them. Any incoming histogram or summary data points are silently discarded. These tables also use the pre-refactor schema (all metadata columns inline, no `SeriesID`), which is inconsistent with the gauge/sum design. Implementing these types is straightforward but was out of scope for the current work.

### Unbounded in-process series cache

The in-process `sync.Map` that deduplicates series inserts has no eviction policy. This is safe when series cardinality is bounded (as is typical for well-labelled infrastructure metrics), but if label values contain high-entropy data (e.g. request IDs, user IDs) the cache will grow without bound and cause unbounded heap growth. In that case the cache should be replaced with an LRU of a fixed capacity.

### No cross-replica cache coordination

Each replica maintains its own in-process series cache. After a deployment or failover, the new instance starts cold and re-inserts every series once. `ReplacingMergeTree` deduplicates these on background merges, so correctness is preserved, but write amplification at startup is proportional to total series cardinality.

### No transactional series + data-point writes

ClickHouse does not support multi-table transactions. Series rows are written before data-point rows to minimise the window where a data point exists without its series metadata, but a partial failure (series written, data-point write fails) leaves an orphaned series row. The inverse (data point written before series) cannot happen because the series insert is always attempted first.

## Prerequisites

- Go 1.26+
- A running ClickHouse instance (v23+ recommended)
- Docker (for running integration tests via testcontainers)

## Build

```shell
go build ./...
```

## Run

```shell
go run ./... \
  -listenAddr=localhost:4317 \
  -clickhouseAddr=localhost:9000 \
  -clickhouseDatabase=default \
  -clickhouseUsername=default \
  -clickhousePassword=yourpassword
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-listenAddr` | `localhost:4317` | gRPC listen address |
| `-maxReceiveMessageSize` | `16777216` | Max gRPC message size in bytes (16 MiB) |
| `-clickhouseAddr` | `localhost:9000` | ClickHouse native protocol address |
| `-clickhouseDatabase` | `default` | ClickHouse database name |
| `-clickhouseUsername` | `default` | ClickHouse username |
| `-clickhousePassword` | _(empty)_ | ClickHouse password |

On startup the application:
1. Connects to ClickHouse and verifies the connection
2. Creates all required tables if they don't exist (`CREATE TABLE IF NOT EXISTS`)
3. Starts listening for OTLP metric exports on the gRPC endpoint

## Run tests

Unit and integration tests run together. Integration tests spin up a real ClickHouse instance via Docker (testcontainers) automatically — Docker must be running.

```shell
go test -count=1 ./...
```

For verbose integration test output:

```shell
go test -count=1 -v ./...
```

## Observability

The application instruments itself with OpenTelemetry (exported to stdout by default) and emits the following signals:

### Metrics

| Metric | Description |
|--------|-------------|
| `com.dash0.otlp_metric_store.export_requests` | Total OTLP export requests received |
| `com.dash0.otlp_metric_store.gauge_data_points` | Gauge data points written to ClickHouse |
| `com.dash0.otlp_metric_store.sum_data_points` | Sum data points written to ClickHouse |
| `com.dash0.otlp_metric_store.gauge_series_written` | Gauge series rows written to ClickHouse (after cache filtering) |
| `com.dash0.otlp_metric_store.sum_series_written` | Sum series rows written to ClickHouse (after cache filtering) |
| `com.dash0.otlp_metric_store.series_cache_hits` | Series insert operations skipped due to in-process cache hit |
| `com.dash0.otlp_metric_store.series_cache_misses` | Series insert operations that missed the cache and were written to ClickHouse |

### Logs

Structured logs are emitted via `log/slog` at the following points:

- Application startup and ClickHouse readiness
- Per-request debug log on every received export
- Error logs with full context on any ClickHouse insert failure

### Traces

All inbound gRPC requests are traced via `otelgrpc`. Trace context is propagated to log records and metric exemplars.
