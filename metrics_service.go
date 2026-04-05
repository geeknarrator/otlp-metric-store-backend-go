package main

import (
	"context"
	"log/slog"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

type dash0MetricsServiceServer struct {
	store MetricsStore

	colmetricspb.UnimplementedMetricsServiceServer
}

func newServer(store MetricsStore) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{store: store}
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	if m.store != nil {
		rm := request.GetResourceMetrics()

		if gaugeRows := MapGaugeRows(rm); len(gaugeRows) > 0 {
			gaugeSeriesRows := MapGaugeSeriesRows(rm)
			if err := m.store.InsertGaugeSeries(ctx, gaugeSeriesRows); err != nil {
				slog.ErrorContext(ctx, "Failed to insert gauge series", slog.Any("error", err))
				return nil, err
			}
			gaugeSeriesWrittenCounter.Add(ctx, int64(len(gaugeSeriesRows)))

			if err := m.store.InsertGauge(ctx, gaugeRows); err != nil {
				slog.ErrorContext(ctx, "Failed to insert gauge data points", slog.Any("error", err))
				return nil, err
			}
			gaugeDataPointsCounter.Add(ctx, int64(len(gaugeRows)))
		}

		if sumRows := MapSumRows(rm); len(sumRows) > 0 {
			sumSeriesRows := MapSumSeriesRows(rm)
			if err := m.store.InsertSumSeries(ctx, sumSeriesRows); err != nil {
				slog.ErrorContext(ctx, "Failed to insert sum series", slog.Any("error", err))
				return nil, err
			}
			sumSeriesWrittenCounter.Add(ctx, int64(len(sumSeriesRows)))

			if err := m.store.InsertSum(ctx, sumRows); err != nil {
				slog.ErrorContext(ctx, "Failed to insert sum data points", slog.Any("error", err))
				return nil, err
			}
			sumDataPointsCounter.Add(ctx, int64(len(sumRows)))
		}
	}

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
