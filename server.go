package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	listenAddr            = flag.String("listenAddr", "localhost:4317", "The gRPC listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "The max message size in bytes the server can receive")
	clickhouseAddr        = flag.String("clickhouseAddr", "localhost:9000", "The ClickHouse server address")
	clickhouseDatabase    = flag.String("clickhouseDatabase", "default", "The ClickHouse database name")
	clickhouseUsername    = flag.String("clickhouseUsername", "default", "The ClickHouse username")
	clickhousePassword    = flag.String("clickhousePassword", "", "The ClickHouse password")
)

const name = "dash0.com/otlp-metric-store-backend"

var (
	meter  = otel.Meter(name)
	logger = otelslog.NewLogger(name)

	metricsReceivedCounter    metric.Int64Counter
	gaugeDataPointsCounter    metric.Int64Counter
	sumDataPointsCounter      metric.Int64Counter
	gaugeSeriesWrittenCounter metric.Int64Counter
	sumSeriesWrittenCounter   metric.Int64Counter
	seriesCacheHitCounter     metric.Int64Counter
	seriesCacheMissCounter    metric.Int64Counter
)

func init() {
	var err error

	metricsReceivedCounter, err = meter.Int64Counter(
		"com.dash0.otlp_metric_store.export_requests",
		metric.WithDescription("Number of ExportMetricsServiceRequests received"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		panic(err)
	}

	gaugeDataPointsCounter, err = meter.Int64Counter(
		"com.dash0.otlp_metric_store.gauge_data_points",
		metric.WithDescription("Number of gauge data points written to ClickHouse"),
		metric.WithUnit("{datapoint}"),
	)
	if err != nil {
		panic(err)
	}

	sumDataPointsCounter, err = meter.Int64Counter(
		"com.dash0.otlp_metric_store.sum_data_points",
		metric.WithDescription("Number of sum data points written to ClickHouse"),
		metric.WithUnit("{datapoint}"),
	)
	if err != nil {
		panic(err)
	}

	gaugeSeriesWrittenCounter, err = meter.Int64Counter(
		"com.dash0.otlp_metric_store.gauge_series_written",
		metric.WithDescription("Number of gauge series rows written to ClickHouse (duplicates expected)"),
		metric.WithUnit("{series}"),
	)
	if err != nil {
		panic(err)
	}

	sumSeriesWrittenCounter, err = meter.Int64Counter(
		"com.dash0.otlp_metric_store.sum_series_written",
		metric.WithDescription("Number of sum series rows written to ClickHouse (duplicates expected)"),
		metric.WithUnit("{series}"),
	)
	if err != nil {
		panic(err)
	}

	seriesCacheHitCounter, err = meter.Int64Counter(
		"com.dash0.otlp_metric_store.series_cache_hits",
		metric.WithDescription("Number of series insert operations skipped due to in-process cache hit"),
		metric.WithUnit("{series}"),
	)
	if err != nil {
		panic(err)
	}

	seriesCacheMissCounter, err = meter.Int64Counter(
		"com.dash0.otlp_metric_store.series_cache_misses",
		metric.WithDescription("Number of series insert operations that missed the in-process cache and were written to ClickHouse"),
		metric.WithUnit("{series}"),
	)
	if err != nil {
		panic(err)
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	flag.Parse()

	slog.SetDefault(logger)
	logger.Info("Starting application")

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(context.Background())
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	ctx := context.Background()

	store, err := NewClickHouseMetricsStore(ctx, *clickhouseAddr, *clickhouseDatabase, *clickhouseUsername, *clickhousePassword)
	if err != nil {
		return fmt.Errorf("connecting to ClickHouse: %w", err)
	}
	defer func() {
		err = errors.Join(err, store.Close())
	}()

	if err := store.CreateTables(ctx); err != nil {
		return fmt.Errorf("creating ClickHouse tables: %w", err)
	}
	logger.Info("ClickHouse tables ready")

	slog.Debug("Starting listener", slog.String("listenAddr", *listenAddr))
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(*maxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer(store))

	slog.Info("gRPC server listening", slog.String("addr", *listenAddr))

	return grpcServer.Serve(listener)
}
