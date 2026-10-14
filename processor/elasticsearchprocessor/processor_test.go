// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchprocessor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zaptest"
)

func createTestProcessor() *elasticsearchProcessor {
	return newProcessor(&Config{
		HistogramMetricAttribute:            "le",
		HistogramMetricSuffix:               "_bucket",
		ExponentialHistogramMetricAttribute: "midpoint",
		ExponentialHistogramMetricSuffix:    "_bucket",
		SummaryMetricAttribute:              "quantile",
		SummaryMetricSuffix:                 "_quantile",
	}, zaptest.NewLogger(&testing.T{}))
}

func TestProcessHistogram(t *testing.T) {
	proc := createTestProcessor()

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("test")

	h := m.SetEmptyHistogram()
	dp := h.DataPoints().AppendEmpty()

	dp.ExplicitBounds().FromRaw([]float64{0.1, 0.5, 1.0})
	dp.BucketCounts().FromRaw([]uint64{2, 3, 5, 1}) // cumulative sums: 2, 5, 10, 11

	result, err := proc.processMetrics(t.Context(), md)
	require.NoError(t, err)

	metrics := result.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.Equal(t, 2, metrics.Len())

	gaugeMetric := metrics.At(1)
	assert.Equal(t, "test_bucket", gaugeMetric.Name())

	dps := gaugeMetric.Gauge().DataPoints()
	require.Equal(t, 4, dps.Len())

	expectedBounds := []string{"0.1", "0.5", "1", "+Inf"}
	expectedCounts := []int64{2, 5, 10, 11}

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		assert.Equal(t, expectedCounts[i], dp.IntValue())
		val, exists := dp.Attributes().Get("le")
		assert.True(t, exists)
		assert.Equal(t, expectedBounds[i], val.Str())
	}
}

func TestProcessExponentialHistogramWithNegativeScale(t *testing.T) {
	proc := createTestProcessor()

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("test")

	eh := m.SetEmptyExponentialHistogram()
	dp := eh.DataPoints().AppendEmpty()
	dp.SetZeroCount(1)
	dp.SetScale(-1)

	dp.Positive().SetOffset(0)
	dp.Positive().BucketCounts().FromRaw([]uint64{4, 16})

	dp.Negative().SetOffset(0)
	dp.Negative().BucketCounts().FromRaw([]uint64{1})

	result, err := proc.processMetrics(t.Context(), md)
	require.NoError(t, err)

	metrics := result.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.Equal(t, 2, metrics.Len())

	gaugeMetric := metrics.At(1)
	assert.Equal(t, "test_bucket", gaugeMetric.Name())

	dps := gaugeMetric.Gauge().DataPoints()
	require.Equal(t, 4, dps.Len())

	assert.Equal(t, int64(1), dps.At(0).IntValue())
	zVal, _ := dps.At(0).Attributes().Get("midpoint")
	assert.Equal(t, "0", zVal.Str())

	assert.Equal(t, int64(4), dps.At(1).IntValue())
	pVal1, _ := dps.At(1).Attributes().Get("midpoint")
	assert.Equal(t, "2.5", pVal1.Str())

	assert.Equal(t, int64(16), dps.At(2).IntValue())
	pVal2, _ := dps.At(2).Attributes().Get("midpoint")
	assert.Equal(t, "10", pVal2.Str())

	assert.Equal(t, int64(1), dps.At(3).IntValue())
	nVal, _ := dps.At(3).Attributes().Get("midpoint")
	assert.Equal(t, "-2.5", nVal.Str())
}

func TestProcessExponentialHistogramWithPositiveScale(t *testing.T) {
	proc := createTestProcessor()

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("test")

	eh := m.SetEmptyExponentialHistogram()
	dp := eh.DataPoints().AppendEmpty()
	dp.SetZeroCount(1)
	dp.SetScale(0)

	dp.Positive().SetOffset(0)
	dp.Positive().BucketCounts().FromRaw([]uint64{4, 16})

	dp.Negative().SetOffset(0)
	dp.Negative().BucketCounts().FromRaw([]uint64{1})

	result, err := proc.processMetrics(t.Context(), md)
	require.NoError(t, err)

	metrics := result.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.Equal(t, 2, metrics.Len())

	gaugeMetric := metrics.At(1)
	assert.Equal(t, "test_bucket", gaugeMetric.Name())

	dps := gaugeMetric.Gauge().DataPoints()
	require.Equal(t, 4, dps.Len())

	assert.Equal(t, int64(1), dps.At(0).IntValue())
	zVal, _ := dps.At(0).Attributes().Get("midpoint")
	assert.Equal(t, "0", zVal.Str())

	assert.Equal(t, int64(4), dps.At(1).IntValue())
	pVal1, _ := dps.At(1).Attributes().Get("midpoint")
	assert.Equal(t, "1.5", pVal1.Str())

	assert.Equal(t, int64(16), dps.At(2).IntValue())
	pVal2, _ := dps.At(2).Attributes().Get("midpoint")
	assert.Equal(t, "3", pVal2.Str())

	assert.Equal(t, int64(1), dps.At(3).IntValue())
	nVal, _ := dps.At(3).Attributes().Get("midpoint")
	assert.Equal(t, "-1.5", nVal.Str())
}

func TestSafeUint64ToInt64(t *testing.T) {
	assert.Equal(t, int64(100), safeUint64ToInt64(100))
	assert.Equal(t, int64(math.MaxInt64), safeUint64ToInt64(math.MaxInt64))
	assert.Equal(t, int64(math.MaxInt64), safeUint64ToInt64(uint64(math.MaxInt64)+100))
}

func TestProcessSummary(t *testing.T) {
	proc := createTestProcessor()

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("test")

	s := m.SetEmptySummary()
	dp := s.DataPoints().AppendEmpty()

	q1 := dp.QuantileValues().AppendEmpty()
	q1.SetQuantile(0.5)
	q1.SetValue(12.5)

	q2 := dp.QuantileValues().AppendEmpty()
	q2.SetQuantile(0.99)
	q2.SetValue(45.2)

	result, err := proc.processMetrics(t.Context(), md)
	require.NoError(t, err)

	metrics := result.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.Equal(t, 2, metrics.Len())

	gaugeMetric := metrics.At(1)
	assert.Equal(t, "test_quantile", gaugeMetric.Name())

	dps := gaugeMetric.Gauge().DataPoints()
	require.Equal(t, 2, dps.Len())

	assert.InDelta(t, 12.5, dps.At(0).DoubleValue(), 0.001)
	val1, _ := dps.At(0).Attributes().Get("quantile")
	assert.Equal(t, "0.5", val1.Str())

	assert.InDelta(t, 45.2, dps.At(1).DoubleValue(), 0.001)
	val2, _ := dps.At(1).Attributes().Get("quantile")
	assert.Equal(t, "0.99", val2.Str())
}
