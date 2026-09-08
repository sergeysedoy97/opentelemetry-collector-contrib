// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// func parseNDJSON(t *testing.T, raw string) []map[string]any {
// 	var results []map[string]any
// 	for line := range strings.SplitSeq(raw, "\n") {
// 		if line == "" {
// 			continue
// 		}
// 		var parsed map[string]any
// 		require.NoError(t, json.Unmarshal([]byte(line), &parsed), line)
// 		results = append(results, parsed)
// 	}
// 	return results
// }

func setupTestExporter(t *testing.T, handler http.HandlerFunc) exporter.Metrics {
	t.Helper()

	srv := httptest.NewServer(handler)
	set := exportertest.NewNopSettings(metadata.Type)
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = srv.URL

	storageID := component.MustNewID("file_storage")
	cfg.InitialInterval = 0
	cfg.MaxElapsedTime = 0
	cfg.QueueBatchConfig.StorageID = &storageID
	cfg.QueueBatchConfig.Batch.Get().FlushTimeout = 0
	cfg.QueueBatchConfig.Batch.Get().MinSize = 0

	exp, err := NewFactory().CreateMetrics(t.Context(), set, cfg)
	require.NoError(t, err)

	require.NoError(t, exp.Start(t.Context(), &testHost{ext: map[component.ID]component.Component{storageID: &storageExtension{}}}))

	t.Cleanup(func() {
		require.NoError(t, exp.Shutdown(t.Context()))
		srv.Close()
	})

	return exp
}

func TestPusher_GaugeMetrics(t *testing.T) {
	var capturedPayload string
	var wg sync.WaitGroup
	var capturedError error
	wg.Add(1)

	exp := setupTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		defer r.Body.Close()
		g, e := gzip.NewReader(r.Body)
		if e != nil {
			capturedError = e
			return
		}
		defer g.Close()
		b, e := io.ReadAll(g)
		if e != nil {
			capturedError = e
			return
		}
		capturedPayload = string(b)
		w.WriteHeader(http.StatusOK)
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	mDouble := sm.Metrics().AppendEmpty()
	mDouble.SetName("double")
	mDouble.SetEmptyGauge()
	dpDouble := mDouble.Gauge().DataPoints().AppendEmpty()
	dpDouble.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dpDouble.SetDoubleValue(1)

	mInt := sm.Metrics().AppendEmpty()
	mInt.SetName("int")
	mInt.SetEmptyGauge()
	dpInt := mInt.Gauge().DataPoints().AppendEmpty()
	dpInt.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dpInt.SetIntValue(1)

	require.NoError(t, exp.ConsumeMetrics(t.Context(), md))

	wg.Wait()

	require.NoError(t, capturedError)

	assert.Equal(t, "{\"create\":{\"_index\":\"metrics-otelcol.v1-garbage\",\"dynamic_templates\":{\"metrics.double\":\"gauge_double\",\"metrics.int\":\"gauge_long\"}}}\n{\"@timestamp\":1700000000000,\"data_stream\":{\"type\":\"metrics\",\"dataset\":\"otelcol.v1\",\"namespace\":\"garbage\"},\"attributes\":{},\"resource\":{\"attributes\":{}},\"scope\":{\"attributes\":{},\"name\":\"\",\"version\":\"\"},\"metrics\":{\"double\":1.0,\"int\":1},\"_metric_names_hash\":\"9347a6685c7d8590\"}\n", capturedPayload)
}

func TestPusher_SumMetrics(t *testing.T) {
	var capturedPayload string
	var wg sync.WaitGroup
	var capturedError error
	wg.Add(1)

	exp := setupTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		defer r.Body.Close()
		g, e := gzip.NewReader(r.Body)
		if e != nil {
			capturedError = e
			return
		}
		defer g.Close()
		b, e := io.ReadAll(g)
		if e != nil {
			capturedError = e
			return
		}
		capturedPayload = string(b)
		w.WriteHeader(http.StatusOK)
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	mDouble := sm.Metrics().AppendEmpty()
	mDouble.SetName("double")
	sumDouble := mDouble.SetEmptySum()
	sumDouble.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dpDouble := sumDouble.DataPoints().AppendEmpty()
	dpDouble.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dpDouble.SetDoubleValue(1)

	mInt := sm.Metrics().AppendEmpty()
	mInt.SetName("int")
	sumInt := mInt.SetEmptySum()
	sumInt.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dpInt := sumInt.DataPoints().AppendEmpty()
	dpInt.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dpInt.SetIntValue(1)

	require.NoError(t, exp.ConsumeMetrics(t.Context(), md))

	wg.Wait()

	require.NoError(t, capturedError)

	assert.Equal(t, "{\"create\":{\"_index\":\"metrics-otelcol.v1-garbage\",\"dynamic_templates\":{\"metrics.double\":\"gauge_double\",\"metrics.int\":\"gauge_long\"}}}\n{\"@timestamp\":1700000000000,\"data_stream\":{\"type\":\"metrics\",\"dataset\":\"otelcol.v1\",\"namespace\":\"garbage\"},\"attributes\":{},\"resource\":{\"attributes\":{}},\"scope\":{\"attributes\":{},\"name\":\"\",\"version\":\"\"},\"metrics\":{\"double\":1.0,\"int\":1},\"_metric_names_hash\":\"9347a6685c7d8590\"}\n", capturedPayload)
}

func TestPusher_HistogramMetrics(t *testing.T) {
	var capturedPayload string
	var wg sync.WaitGroup
	var capturedError error
	wg.Add(1)

	exp := setupTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		defer r.Body.Close()
		g, e := gzip.NewReader(r.Body)
		if e != nil {
			capturedError = e
			return
		}
		defer g.Close()
		b, e := io.ReadAll(g)
		if e != nil {
			capturedError = e
			return
		}
		capturedPayload = string(b)
		w.WriteHeader(http.StatusOK)
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	mHis := sm.Metrics().AppendEmpty()
	mHis.SetName("histogram")
	his := mHis.SetEmptyHistogram()
	his.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dpDis := his.DataPoints().AppendEmpty()
	dpDis.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dpDis.ExplicitBounds().FromRaw([]float64{1, 2, 3})
	dpDis.BucketCounts().FromRaw([]uint64{1, 0, 1, 10}) // 4 buckets for 3 bounds

	require.NoError(t, exp.ConsumeMetrics(t.Context(), md))

	wg.Wait()

	require.NoError(t, capturedError)

	assert.Equal(t, "{\"create\":{\"_index\":\"metrics-otelcol.v1-garbage\",\"dynamic_templates\":{\"metrics.histogram\":\"histogram\"}}}\n{\"@timestamp\":1700000000000,\"data_stream\":{\"type\":\"metrics\",\"dataset\":\"otelcol.v1\",\"namespace\":\"garbage\"},\"attributes\":{},\"resource\":{\"attributes\":{}},\"scope\":{\"attributes\":{},\"name\":\"\",\"version\":\"\"},\"metrics\":{\"histogram\":{\"values\":[1.0,3.0],\"counts\":[1,1]}},\"_metric_names_hash\":\"1b2c60c8eb9775c6\"}\n", capturedPayload)
}

func TestPusher_ExponentialHistogramMetrics(t *testing.T) {
	var capturedPayload string
	var wg sync.WaitGroup
	var capturedError error
	wg.Add(1)

	exp := setupTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		defer r.Body.Close()
		g, e := gzip.NewReader(r.Body)
		if e != nil {
			capturedError = e
			return
		}
		defer g.Close()
		b, e := io.ReadAll(g)
		if e != nil {
			capturedError = e
			return
		}
		capturedPayload = string(b)
		w.WriteHeader(http.StatusOK)
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	mExpHis := sm.Metrics().AppendEmpty()
	mExpHis.SetName("exponential_histogram")
	expHis := mExpHis.SetEmptyExponentialHistogram()
	expHis.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dpExpHis := expHis.DataPoints().AppendEmpty()
	dpExpHis.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dpExpHis.SetScale(1)
	dpExpHis.SetMin(1)
	dpExpHis.SetMax(1)
	dpExpHis.SetZeroThreshold(1)
	dpExpHis.SetZeroCount(1)
	dpExpHis.Positive().SetOffset(1)
	dpExpHis.Positive().BucketCounts().FromRaw([]uint64{1, 0, 1})
	dpExpHis.Negative().SetOffset(1)
	dpExpHis.Negative().BucketCounts().FromRaw([]uint64{1, 0, 1})

	require.NoError(t, exp.ConsumeMetrics(t.Context(), md))

	wg.Wait()

	require.NoError(t, capturedError)

	assert.Equal(t, "{\"create\":{\"_index\":\"metrics-otelcol.v1-garbage\",\"dynamic_templates\":{\"metrics.exponential_histogram\":\"exponential_histogram\"}}}\n{\"@timestamp\":1700000000000,\"data_stream\":{\"type\":\"metrics\",\"dataset\":\"otelcol.v1\",\"namespace\":\"garbage\"},\"attributes\":{},\"resource\":{\"attributes\":{}},\"scope\":{\"attributes\":{},\"name\":\"\",\"version\":\"\"},\"metrics\":{\"exponential_histogram\":{\"scale\":1,\"min\":1.0,\"max\":1.0,\"zero\":{\"threshold\":1.0,\"count\":1},\"positive\":{\"indices\":[1,3],\"counts\":[1,1]},\"negative\":{\"indices\":[1,3],\"counts\":[1,1]}}},\"_metric_names_hash\":\"56746b849da7c877\"}\n", capturedPayload)
}

func TestPusher_RequestRetry(t *testing.T) {
	var capturedPayload string
	var wg sync.WaitGroup
	var capturedError error
	var retry bool
	wg.Add(2)

	exp := setupTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		defer r.Body.Close()
		if !retry {
			w.WriteHeader(http.StatusTooManyRequests)
			retry = true
			return
		}
		g, e := gzip.NewReader(r.Body)
		if e != nil {
			capturedError = e
			return
		}
		defer g.Close()
		b, e := io.ReadAll(g)
		if e != nil {
			capturedError = e
			return
		}
		capturedPayload = string(b)
		w.WriteHeader(http.StatusOK)
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	mInt := sm.Metrics().AppendEmpty()
	mInt.SetName("int")
	mInt.SetEmptyGauge()
	dpInt := mInt.Gauge().DataPoints().AppendEmpty()
	dpInt.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dpInt.SetIntValue(1)

	require.NoError(t, exp.ConsumeMetrics(t.Context(), md))

	wg.Wait()

	require.NoError(t, capturedError)

	assert.Equal(t, "{\"create\":{\"_index\":\"metrics-otelcol.v1-garbage\",\"dynamic_templates\":{\"metrics.int\":\"gauge_long\"}}}\n{\"@timestamp\":1700000000000,\"data_stream\":{\"type\":\"metrics\",\"dataset\":\"otelcol.v1\",\"namespace\":\"garbage\"},\"attributes\":{},\"resource\":{\"attributes\":{}},\"scope\":{\"attributes\":{},\"name\":\"\",\"version\":\"\"},\"metrics\":{\"int\":1},\"_metric_names_hash\":\"60f24396f77057c3\"}\n", capturedPayload)
}

func TestPusher_DocumentRetry(t *testing.T) {
	var capturedPayload string
	var wg sync.WaitGroup
	var capturedError error
	var retry bool
	wg.Add(2)

	exp := setupTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		defer r.Body.Close()
		if !retry {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"items":[{"create":{"status":429}}]}`))
			retry = true
			return
		}
		g, e := gzip.NewReader(r.Body)
		if e != nil {
			capturedError = e
			return
		}
		defer g.Close()
		b, e := io.ReadAll(g)
		if e != nil {
			capturedError = e
			return
		}
		capturedPayload = string(b)
		w.WriteHeader(http.StatusOK)
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	mInt := sm.Metrics().AppendEmpty()
	mInt.SetName("int")
	mInt.SetEmptyGauge()
	dpInt := mInt.Gauge().DataPoints().AppendEmpty()
	dpInt.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dpInt.SetIntValue(1)

	require.NoError(t, exp.ConsumeMetrics(t.Context(), md))

	wg.Wait()

	require.NoError(t, capturedError)

	assert.Equal(t, "{\"create\":{\"_index\":\"metrics-otelcol.v1-garbage\",\"dynamic_templates\":{\"metrics.int\":\"gauge_long\"}}}\n{\"@timestamp\":1700000000000,\"data_stream\":{\"type\":\"metrics\",\"dataset\":\"otelcol.v1\",\"namespace\":\"garbage\"},\"attributes\":{},\"resource\":{\"attributes\":{}},\"scope\":{\"attributes\":{},\"name\":\"\",\"version\":\"\"},\"metrics\":{\"int\":1},\"_metric_names_hash\":\"60f24396f77057c3\"}\n", capturedPayload)
}
