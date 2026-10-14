// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchprocessor // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/processor/elasticsearchprocessor"

import (
	"context"
	"math"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/expohisto/mapping"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/expohisto/mapping/exponent"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/expohisto/mapping/logarithm"
)

type elasticsearchProcessor struct {
	logger *zap.Logger
	config *Config
}

func newProcessor(config *Config, logger *zap.Logger) *elasticsearchProcessor {
	return &elasticsearchProcessor{
		logger: logger,
		config: config,
	}
}

func (p *elasticsearchProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	for i := range md.ResourceMetrics().Len() {
		rm := md.ResourceMetrics().At(i)
		for j := range rm.ScopeMetrics().Len() {
			ilm := rm.ScopeMetrics().At(j)
			metrics := ilm.Metrics()
			for k := range metrics.Len() {
				metric := metrics.At(k)
				switch metric.Type() {
				case pmetric.MetricTypeHistogram:
					p.processHistogram(&metric, ilm)
				case pmetric.MetricTypeExponentialHistogram:
					p.processExponentialHistogram(&metric, ilm)
				case pmetric.MetricTypeSummary:
					p.processSummary(&metric, ilm)
				}
			}
		}
	}
	return md, nil
}

func (p *elasticsearchProcessor) processSummary(metric *pmetric.Metric, ilm pmetric.ScopeMetrics) {
	oldMetric := metric.Summary()
	oldDataPoints := oldMetric.DataPoints()
	newMetric := createMetric(metric, p.config.SummaryMetricSuffix)
	newDataPoints := newMetric.SetEmptyGauge().DataPoints()

	for i := range oldDataPoints.Len() {
		oldDataPoint := oldDataPoints.At(i)
		oldAttributes := oldDataPoint.Attributes()
		oldStartTimestamp := oldDataPoint.StartTimestamp()
		oldTimestamp := oldDataPoint.Timestamp()

		quantileValues := oldDataPoint.QuantileValues()

		for j := range quantileValues.Len() {
			quantileValue := quantileValues.At(j)

			newDataPoint := createNumberDataPoint(newDataPoints, oldAttributes, oldStartTimestamp, oldTimestamp)
			newDataPoint.SetDoubleValue(quantileValue.Value())
			newDataPoint.Attributes().PutStr(p.config.SummaryMetricAttribute, strconv.FormatFloat(quantileValue.Quantile(), 'g', -1, 64))
		}
	}

	if newDataPoints.Len() > 0 {
		newMetric.MoveTo(ilm.Metrics().AppendEmpty())
	}
}

func (p *elasticsearchProcessor) processHistogram(metric *pmetric.Metric, ilm pmetric.ScopeMetrics) {
	oldMetric := metric.Histogram()
	oldDataPoints := oldMetric.DataPoints()
	newMetric := createMetric(metric, p.config.HistogramMetricSuffix)
	newDataPoints := newMetric.SetEmptyGauge().DataPoints()

	for i := range oldDataPoints.Len() {
		oldDataPoint := oldDataPoints.At(i)
		oldAttributes := oldDataPoint.Attributes()
		oldStartTimestamp := oldDataPoint.StartTimestamp()
		oldTimestamp := oldDataPoint.Timestamp()

		bucketCounts := oldDataPoint.BucketCounts()
		explicitBounds := oldDataPoint.ExplicitBounds()

		if bucketCounts.Len() > 0 && bucketCounts.Len() != explicitBounds.Len()+1 {
			continue
		}

		bucketSum := uint64(0)
		for j := range bucketCounts.Len() {
			count := bucketCounts.At(j)
			if count == 0 {
				continue
			}
			bucketSum += count

			explicitBound := "+Inf"
			if j != explicitBounds.Len() {
				explicitBound = strconv.FormatFloat(explicitBounds.At(j), 'g', -1, 64)
			}

			newDataPoint := createNumberDataPoint(newDataPoints, oldAttributes, oldStartTimestamp, oldTimestamp)
			newDataPoint.SetIntValue(safeUint64ToInt64(bucketSum))
			newDataPoint.Attributes().PutStr(p.config.HistogramMetricAttribute, explicitBound)
		}
	}

	if newDataPoints.Len() > 0 {
		newMetric.MoveTo(ilm.Metrics().AppendEmpty())
	}
}

func (p *elasticsearchProcessor) processExponentialHistogram(metric *pmetric.Metric, ilm pmetric.ScopeMetrics) {
	oldMetric := metric.ExponentialHistogram()
	oldDataPoints := oldMetric.DataPoints()
	newMetric := createMetric(metric, p.config.ExponentialHistogramMetricSuffix)
	newDataPoints := newMetric.SetEmptyGauge().DataPoints()

	for i := range oldDataPoints.Len() {
		var mapping mapping.Mapping

		oldDataPoint := oldDataPoints.At(i)
		oldAttributes := oldDataPoint.Attributes()
		oldStartTimestamp := oldDataPoint.StartTimestamp()
		oldTimestamp := oldDataPoint.Timestamp()

		if zeroCount := oldDataPoint.ZeroCount(); zeroCount > 0 {
			newDataPoint := createNumberDataPoint(newDataPoints, oldAttributes, oldStartTimestamp, oldTimestamp)
			newDataPoint.SetIntValue(safeUint64ToInt64(zeroCount))
			newDataPoint.Attributes().PutStr(p.config.ExponentialHistogramMetricAttribute, "0")
		}

		if scale := oldDataPoint.Scale(); scale > 0 {
			mapping, _ = logarithm.NewMapping(scale)
		} else {
			mapping, _ = exponent.NewMapping(scale)
		}

		positive := oldDataPoint.Positive()
		positiveOffset := positive.Offset()
		positiveBucketCounts := positive.BucketCounts()
		for j := range positiveBucketCounts.Len() {
			count := positiveBucketCounts.At(j)
			if count == 0 {
				continue
			}
			lb, _ := mapping.LowerBoundary(positiveOffset + int32(j))
			ub, _ := mapping.LowerBoundary(positiveOffset + int32(j) + 1)
			newDataPoint := createNumberDataPoint(newDataPoints, oldAttributes, oldStartTimestamp, oldTimestamp)
			newDataPoint.SetIntValue(safeUint64ToInt64(count))
			newDataPoint.Attributes().PutStr(p.config.ExponentialHistogramMetricAttribute, strconv.FormatFloat((ub-lb)/2+lb, 'g', -1, 64))
		}

		negative := oldDataPoint.Negative()
		negativeOffset := negative.Offset()
		negativeBucketCounts := negative.BucketCounts()

		for j := range negativeBucketCounts.Len() {
			count := negativeBucketCounts.At(j)
			if count == 0 {
				continue
			}
			lb, _ := mapping.LowerBoundary(negativeOffset + int32(j) + 1)
			ub, _ := mapping.LowerBoundary(negativeOffset + int32(j))
			newDataPoint := createNumberDataPoint(newDataPoints, oldAttributes, oldStartTimestamp, oldTimestamp)
			newDataPoint.SetIntValue(safeUint64ToInt64(count))
			newDataPoint.Attributes().PutStr(p.config.ExponentialHistogramMetricAttribute, strconv.FormatFloat((lb-ub)/2-lb, 'g', -1, 64))
		}
	}

	if newDataPoints.Len() > 0 {
		newMetric.MoveTo(ilm.Metrics().AppendEmpty())
	}
}

func createMetric(metric *pmetric.Metric, suffix string) pmetric.Metric {
	newMetric := pmetric.NewMetric()
	newMetric.SetName(metric.Name() + suffix)
	newMetric.SetDescription(metric.Description())
	newMetric.SetUnit(metric.Unit())

	return newMetric
}

func createNumberDataPoint(newDataPoints pmetric.NumberDataPointSlice, oldAttributes pcommon.Map, oldStartTimestamp, oldTimestamp pcommon.Timestamp) pmetric.NumberDataPoint {
	newDataPoint := newDataPoints.AppendEmpty()
	newDataPoint.SetStartTimestamp(oldStartTimestamp)
	newDataPoint.SetTimestamp(oldTimestamp)
	oldAttributes.CopyTo(newDataPoint.Attributes())

	return newDataPoint
}

func safeUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}
