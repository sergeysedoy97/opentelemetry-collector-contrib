// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter

import (
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func createBenchExporter(b *testing.B) exporter.Metrics {
	b.Helper()

	srv := httptest.NewServer(testHandler{})
	set := exportertest.NewNopSettings(metadata.Type)
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = srv.URL

	storageID := component.MustNewID("file_storage")
	cfg.QueueBatchConfig.StorageID = &storageID
	cfg.QueueBatchConfig.Batch.Get().FlushTimeout = 0
	cfg.QueueBatchConfig.Batch.Get().MinSize = 0

	exp, err := NewFactory().CreateMetrics(b.Context(), set, cfg)
	if err != nil {
		b.Error(err)
	}

	err = exp.Start(b.Context(), &testHost{ext: map[component.ID]component.Component{storageID: &storageExtension{}}})
	if err != nil {
		b.Error(err)
	}

	b.Cleanup(func() {
		err := exp.Shutdown(b.Context())
		if err != nil {
			b.Error(err)
		}
		srv.Close()
	})

	return exp
}

func generateBenchmarkMetrics(numMetrics, dpsPerMetric int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	resAttrs := rm.Resource().Attributes()
	resAttrs.PutStr("service.name", "service-01")
	resAttrs.PutStr("host.name", "node-01")
	resAttrs.PutStr("data_stream.dataset", "benchmark")
	resAttrs.PutStr("data_stream.namespace", "production")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/example/bench")
	sm.Scope().SetVersion("v1.2.3")
	now := pcommon.NewTimestampFromTime(time.Now())
	for i := range numMetrics {
		m := sm.Metrics().AppendEmpty()
		switch i % 3 {
		case 0:
			g := m.SetEmptyGauge()
			for j := range dpsPerMetric {
				dp := g.DataPoints().AppendEmpty()
				dp.SetTimestamp(now)
				dp.SetDoubleValue(float64(i*100 + j))
				dp.Attributes().PutStr("container.id", strconv.Itoa(j))
				dp.Attributes().PutStr("service.namespace", "a")
			}
		case 1:
			sum := m.SetEmptySum()
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			sum.SetIsMonotonic(true)
			for j := range dpsPerMetric {
				dp := sum.DataPoints().AppendEmpty()
				dp.SetTimestamp(now)
				dp.SetIntValue(int64(i*100 + j))
				dp.Attributes().PutStr("container.id", strconv.Itoa(j))
				dp.Attributes().PutStr("service.namespace", "a")
			}
		case 2:
			h := m.SetEmptyHistogram()
			h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			for j := range dpsPerMetric {
				dp := h.DataPoints().AppendEmpty()
				dp.SetTimestamp(now)
				dp.ExplicitBounds().FromRaw([]float64{0.1, 0.5, 1.0, 5.0})
				dp.BucketCounts().FromRaw([]uint64{10, 25, 5, 2, 0})
				dp.Attributes().PutStr("container.id", strconv.Itoa(j))
				dp.Attributes().PutStr("service.namespace", "a")
			}
		}
	}

	return md
}

func BenchmarkRequestConverter(b *testing.B) {
	exp := createBenchExporter(b)
	md := generateBenchmarkMetrics(50, 20)
	b.ReportAllocs()
	for b.Loop() {
		err := exp.ConsumeMetrics(b.Context(), md)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkRequestConverter_Parallel(b *testing.B) {
	exp := createBenchExporter(b)
	md := generateBenchmarkMetrics(50, 20)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := exp.ConsumeMetrics(b.Context(), md)
			if err != nil {
				b.Error(err)
			}
		}
	})
}
