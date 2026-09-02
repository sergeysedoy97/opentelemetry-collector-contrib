// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func parseNDJSON(t *testing.T, raw string) []map[string]any {
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	var results []map[string]any
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var parsed map[string]any
		err := json.Unmarshal([]byte(line), &parsed)
		require.NoError(t, err, "Failed to parse JSON line: %s", line)
		results = append(results, parsed)
	}
	return results
}

func setupTestExporter(t *testing.T, handler http.HandlerFunc) *ElasticsearchExporter {
	server := httptest.NewServer(handler)

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoints = []string{server.URL}
	cfg.Timeout = 5 * time.Second

	set := exportertest.NewNopSettings(exportertest.NopType)
	exp, err := NewExporter(cfg, set)
	require.NoError(t, err)

	err = exp.Start(t.Context(), componenttest.NewNopHost())
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = exp.Shutdown(t.Context())
		server.Close()
	})

	return exp
}

func TestPusher_GaugeMetrics(t *testing.T) {
	var capturedPayload string

	exp := setupTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		capturedPayload = string(body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("data_stream.dataset", "cpu_stats")
	rm.Resource().Attributes().PutStr("data_stream.namespace", "test")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("system_scope")
	sm.Scope().SetVersion("1.0.0")

	// 1. Double Gauge
	mDouble := sm.Metrics().AppendEmpty()
	mDouble.SetName("system.cpu.usage")
	mDouble.SetEmptyGauge()
	dpDouble := mDouble.Gauge().DataPoints().AppendEmpty()
	dpDouble.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dpDouble.SetDoubleValue(0.75)
	dpDouble.Attributes().PutStr("cpu", "0")

	// 2. Int Gauge
	mInt := sm.Metrics().AppendEmpty()
	mInt.SetName("system.memory.used")
	mInt.SetEmptyGauge()
	dpInt := mInt.Gauge().DataPoints().AppendEmpty()
	dpInt.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dpInt.SetIntValue(10244096)
	dpInt.Attributes().PutStr("cpu", "0") // Same DP hash grouping

	err := exp.pusher(t.Context(), md)
	require.NoError(t, err)

	records := parseNDJSON(t, capturedPayload)
	require.Len(t, records, 2, "Expected 1 bulk metadata line and 1 document line")

	// Verify Action Line
	createOp := records[0]["create"].(map[string]any)
	assert.Equal(t, "metrics-cpu_stats-test", createOp["_index"])
	templates := createOp["dynamic_templates"].(map[string]any)
	assert.Equal(t, "gauge_double", templates["metrics.system.cpu.usage"])
	assert.Equal(t, "gauge_long", templates["metrics.system.memory.used"])

	// Verify Document Line
	doc := records[1]
	assert.Equal(t, float64(1700000000000), doc["@timestamp"])

	metricsObj := doc["metrics"].(map[string]any)
	assert.Equal(t, 0.75, metricsObj["system.cpu.usage"])
	assert.Equal(t, float64(10244096), metricsObj["system.memory.used"])
	assert.NotEmpty(t, doc["_metric_names_hash"])
}

func TestPusher_SumMetrics(t *testing.T) {
	var capturedPayload string

	exp := setupTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		capturedPayload = string(body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	// Monotonic Cumulative Sum -> Counter
	mCounter := sm.Metrics().AppendEmpty()
	mCounter.SetName("http.requests.total")
	sum := mCounter.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := sum.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dp.SetIntValue(500)

	err := exp.pusher(t.Context(), md)
	require.NoError(t, err)

	records := parseNDJSON(t, capturedPayload)
	createOp := records[0]["create"].(map[string]any)
	templates := createOp["dynamic_templates"].(map[string]any)
	assert.Equal(t, "counter_long", templates["metrics.http.requests.total"])
}

func TestPusher_HistogramMetrics(t *testing.T) {
	var capturedPayload string

	exp := setupTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		capturedPayload = string(body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	mHist := sm.Metrics().AppendEmpty()
	mHist.SetName("http.server.latency")
	hist := mHist.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta) // Must be Delta

	dp := hist.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dp.ExplicitBounds().FromRaw([]float64{5.0, 10.0, 25.0})
	dp.BucketCounts().FromRaw([]uint64{2, 5, 3, 1}) // 4 buckets for 3 bounds

	err := exp.pusher(t.Context(), md)
	require.NoError(t, err)

	records := parseNDJSON(t, capturedPayload)
	doc := records[1]
	metricsObj := doc["metrics"].(map[string]any)
	histVal := metricsObj["http.server.latency"].(map[string]any)

	// Check buckets & values mapped correctly
	counts := histVal["counts"].([]any)
	values := histVal["values"].([]any)

	assert.Equal(t, []any{float64(2), float64(5), float64(4)}, counts) // 3 + 1 merged overflow
	assert.Equal(t, []any{5.0, 10.0, 25.0}, values)
}

func TestPusher_ExponentialHistogramMetrics(t *testing.T) {
	var capturedPayload string

	exp := setupTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		capturedPayload = string(body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	mExp := sm.Metrics().AppendEmpty()
	mExp.SetName("db.query.time")
	expHist := mExp.SetEmptyExponentialHistogram()
	expHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	dp := expHist.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dp.SetScale(1)
	dp.SetZeroThreshold(0.001)
	dp.SetZeroCount(2)

	dp.Positive().SetOffset(1)
	dp.Positive().BucketCounts().FromRaw([]uint64{3, 0, 4})

	err := exp.pusher(t.Context(), md)
	require.NoError(t, err)

	records := parseNDJSON(t, capturedPayload)
	doc := records[1]
	metricsObj := doc["metrics"].(map[string]any)
	expVal := metricsObj["db.query.time"].(map[string]any)

	assert.Equal(t, float64(1), expVal["scale"])
	zero := expVal["zero"].(map[string]any)
	assert.Equal(t, float64(2), zero["count"])

	pos := expVal["positive"].(map[string]any)
	assert.Equal(t, []any{float64(1), float64(3)}, pos["indices"]) // skips 0-count index
	assert.Equal(t, []any{float64(3), float64(4)}, pos["counts"])
}

func TestPusher_DataStreamOverrideAndNoRecordedValue(t *testing.T) {
	var capturedPayload string

	exp := setupTestExporter(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		capturedPayload = string(body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("data_stream.dataset", "default_dataset")
	rm.Resource().Attributes().PutStr("data_stream.namespace", "default_namespace")

	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("custom.metric")
	g := m.SetEmptyGauge()

	// DP 1: Standard

	// DP 2: Overrides dataset + namespace at DataPoint level
	dp2 := g.DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dp2.SetDoubleValue(20.0)
	dp2.Attributes().PutStr("data_stream.dataset", "custom_dataset")
	dp2.Attributes().PutStr("data_stream.namespace", "custom_namespace")

	dp1 := g.DataPoints().AppendEmpty()
	dp1.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dp1.SetDoubleValue(10.0)

	// DP 3: Marked as NoRecordedValue -> Should be ignored
	dp3 := g.DataPoints().AppendEmpty()
	dp3.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	dp3.SetFlags(dp3.Flags().WithNoRecordedValue(true))

	err := exp.pusher(t.Context(), md)
	require.NoError(t, err)

	records := parseNDJSON(t, capturedPayload)
	// Should produce 2 action lines and 2 doc lines due to split data streams
	require.Len(t, records, 4)

	indices := []string{
		records[0]["create"].(map[string]any)["_index"].(string),
		records[2]["create"].(map[string]any)["_index"].(string),
	}
	assert.Contains(t, indices, "metrics-default_dataset-default_namespace")
	assert.Contains(t, indices, "metrics-custom_dataset-custom_namespace")
}
