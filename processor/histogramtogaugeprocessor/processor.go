// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package histogramtogaugeprocessor // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/processor/histogramtogaugeprocessor"

import (
	"context"
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type histogramToGaugeProcessor struct {
	logger *zap.Logger
	config *Config
}

func newProcessor(config *Config, logger *zap.Logger) *histogramToGaugeProcessor {
	return &histogramToGaugeProcessor{
		logger: logger,
		config: config,
	}
}

func (p *histogramToGaugeProcessor) processMetrics(
	_ context.Context,
	md pmetric.Metrics,
) (pmetric.Metrics, error) {
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
				}
			}
		}
	}
	return md, nil
}

func (p *histogramToGaugeProcessor) processHistogram(
	metric *pmetric.Metric,
	ilm pmetric.ScopeMetrics,
) {
	oldMetric := metric.Histogram()
	oldDataPoints := oldMetric.DataPoints()
	newMetric, newDataPoints := p.createMetricAndNumberDataPointSlice(metric, p.config.MetricSuffix)

	for i := range oldDataPoints.Len() {
		oldDataPoint := oldDataPoints.At(i)
		oldAttributes := oldDataPoint.Attributes()
		oldStartTimestamp := oldDataPoint.StartTimestamp()
		oldTimestamp := oldDataPoint.Timestamp()

		bucketCounts := oldDataPoint.BucketCounts()
		explicitBounds := oldDataPoint.ExplicitBounds()

		if explicitBounds.Len() == 0 || explicitBounds.Len()+1 != bucketCounts.Len() {
			p.logger.Warn(
				"Invalid histogram explicitBounds/bucketCounts values",
				zap.String("metric", metric.Name()),
			)
			continue
		}

		p.processHistogramBuckets(
			newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
			bucketCounts,
			explicitBounds,
		)
		p.processExtra(
			metric,
			ilm,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
			oldDataPoint.Count(),
			oldDataPoint.HasSum(),
			oldDataPoint.Sum,
			oldDataPoint.HasMin(),
			oldDataPoint.Min,
			oldDataPoint.HasMax(),
			oldDataPoint.Max,
		)
	}

	newMetric.CopyTo(ilm.Metrics().AppendEmpty())
}

func (p *histogramToGaugeProcessor) processExponentialHistogram(
	metric *pmetric.Metric,
	ilm pmetric.ScopeMetrics,
) {
	oldMetric := metric.ExponentialHistogram()
	oldDataPoints := oldMetric.DataPoints()

	newMetric, newDataPoints := p.createMetricAndNumberDataPointSlice(metric, p.config.MetricSuffix)

	for i := range oldDataPoints.Len() {
		oldDataPoint := oldDataPoints.At(i)
		oldAttributes := oldDataPoint.Attributes()
		oldStartTimestamp := oldDataPoint.StartTimestamp()
		oldTimestamp := oldDataPoint.Timestamp()
		oldScale := oldDataPoint.Scale()
		oldZeroCount := oldDataPoint.ZeroCount()

		base := math.Pow(2, math.Pow(2, float64(-oldScale)))

		positive := oldDataPoint.Positive()
		positiveBucketCounts := positive.BucketCounts()
		positiveOffset := positive.Offset()

		negative := oldDataPoint.Negative()
		negativeBucketCounts := negative.BucketCounts()
		negativeOffset := negative.Offset()

		p.processExponentialHistogramBuckets(
			newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
			positiveBucketCounts,
			positiveOffset,
			base,
			1.0,
		)
		p.processExponentialHistogramBuckets(
			newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
			negativeBucketCounts,
			negativeOffset,
			base,
			-1.0,
		)
		p.processExponentialHistogramExtra(newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
			oldZeroCount)
		p.processExtra(
			metric,
			ilm,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
			oldDataPoint.Count(),
			oldDataPoint.HasSum(),
			oldDataPoint.Sum,
			oldDataPoint.HasMin(),
			oldDataPoint.Min,
			oldDataPoint.HasMax(),
			oldDataPoint.Max,
		)
	}

	newMetric.CopyTo(ilm.Metrics().AppendEmpty())
}

func (p *histogramToGaugeProcessor) processHistogramBuckets(
	newDataPoints pmetric.NumberDataPointSlice,
	oldAttributes pcommon.Map,
	oldStartTimestamp pcommon.Timestamp,
	oldTimestamp pcommon.Timestamp,
	bucketCounts pcommon.UInt64Slice,
	explicitBounds pcommon.Float64Slice,
) {
	bucketSum := uint64(0)
	for j := range bucketCounts.Len() {
		bucketSum += bucketCounts.At(j)

		newDataPoint := p.createNumberDataPoint(
			newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
		)
		newDataPoint.SetIntValue(int64(bucketSum))

		if j == explicitBounds.Len() {
			newDataPoint.Attributes().PutStr(p.config.BucketLabel, "+Inf")
		} else {
			newDataPoint.Attributes().PutDouble(p.config.BucketLabel, explicitBounds.At(j))
		}
	}
}

func (p *histogramToGaugeProcessor) processExponentialHistogramBuckets(
	newDataPoints pmetric.NumberDataPointSlice,
	oldAttributes pcommon.Map,
	oldStartTimestamp pcommon.Timestamp,
	oldTimestamp pcommon.Timestamp,
	bucketCounts pcommon.UInt64Slice,
	offset int32,
	base float64,
	sign float64,
) {
	bucketSum := uint64(0)
	for j := range bucketCounts.Len() {
		bucketSum += bucketCounts.At(j)

		newDataPoint := p.createNumberDataPoint(
			newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
		)
		newDataPoint.SetIntValue(int64(bucketSum))
		newDataPoint.Attributes().
			PutDouble(p.config.BucketLabel, sign*math.Pow(base, float64(int(offset)+j+1)))
	}
}

func (p *histogramToGaugeProcessor) processExponentialHistogramExtra(
	newDataPoints pmetric.NumberDataPointSlice,
	oldAttributes pcommon.Map,
	oldStartTimestamp pcommon.Timestamp,
	oldTimestamp pcommon.Timestamp,
	oldZeroCount uint64,
) {
	if oldZeroCount != 0 {
		newDataPoint := p.createNumberDataPoint(
			newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
		)
		newDataPoint.SetIntValue(int64(oldZeroCount))
		newDataPoint.Attributes().PutDouble(p.config.BucketLabel, 0)
	}
}

func (p *histogramToGaugeProcessor) processExtra(
	metric *pmetric.Metric,
	ilm pmetric.ScopeMetrics,
	oldAttributes pcommon.Map,
	oldStartTimestamp pcommon.Timestamp,
	oldTimestamp pcommon.Timestamp,
	count uint64,
	hasSum bool,
	sumFunc func() float64,
	hasMin bool,
	minFunc func() float64,
	hasMax bool,
	maxFunc func() float64,
) {
	newMetric, newDataPoints := p.createMetricAndNumberDataPointSlice(
		metric,
		p.config.CountMetricSuffix,
	)
	newDataPoint := p.createNumberDataPoint(
		newDataPoints,
		oldAttributes,
		oldStartTimestamp,
		oldTimestamp,
	)
	newDataPoint.SetIntValue(int64(count))
	newMetric.CopyTo(ilm.Metrics().AppendEmpty())

	if hasSum {
		newMetric, newDataPoints := p.createMetricAndNumberDataPointSlice(
			metric,
			p.config.SumMetricSuffix,
		)
		newDataPoint := p.createNumberDataPoint(
			newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
		)
		newDataPoint.SetDoubleValue(sumFunc())
		newMetric.CopyTo(ilm.Metrics().AppendEmpty())
	}
	if hasMin {
		newMetric, newDataPoints := p.createMetricAndNumberDataPointSlice(
			metric,
			p.config.MinMetricSuffix,
		)
		newDataPoint := p.createNumberDataPoint(
			newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
		)
		newDataPoint.SetDoubleValue(minFunc())
		newMetric.CopyTo(ilm.Metrics().AppendEmpty())
	}
	if hasMax {
		newMetric, newDataPoints := p.createMetricAndNumberDataPointSlice(
			metric,
			p.config.MaxMetricSuffix,
		)
		newDataPoint := p.createNumberDataPoint(
			newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
		)
		newDataPoint.SetDoubleValue(maxFunc())
		newMetric.CopyTo(ilm.Metrics().AppendEmpty())
	}
}

func (p *histogramToGaugeProcessor) createMetricAndNumberDataPointSlice(
	metric *pmetric.Metric,
	suffix string,
) (pmetric.Metric, pmetric.NumberDataPointSlice) {
	newMetric := pmetric.NewMetric()
	newMetric.SetName(metric.Name() + suffix)
	newMetric.SetDescription(metric.Description())
	newMetric.SetUnit(metric.Unit())
	newDataPoints := newMetric.SetEmptyGauge().DataPoints()

	return newMetric, newDataPoints
}

func (p *histogramToGaugeProcessor) createNumberDataPoint(
	newDataPoints pmetric.NumberDataPointSlice,
	oldAttributes pcommon.Map,
	oldStartTimestamp pcommon.Timestamp,
	oldTimestamp pcommon.Timestamp,
) pmetric.NumberDataPoint {
	newDataPoint := newDataPoints.AppendEmpty()
	newDataPoint.SetStartTimestamp(oldStartTimestamp)
	newDataPoint.SetTimestamp(oldTimestamp)
	oldAttributes.Range(func(k string, v pcommon.Value) bool {
		v.CopyTo(newDataPoint.Attributes().PutEmpty(k))
		return true
	})

	return newDataPoint
}
