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

const (
	minExpoScale = -10
	maxExpoScale = 20
)

// scaleMappings caches stateless expohisto mappings for all standard OTel scales [-10, 20].
var scaleMappings = func() [maxExpoScale - minExpoScale + 1]mapping.Mapping {
	var m [maxExpoScale - minExpoScale + 1]mapping.Mapping
	for s := int32(minExpoScale); s <= maxExpoScale; s++ {
		if s > 0 {
			m[s-minExpoScale], _ = logarithm.NewMapping(s)
		} else {
			m[s-minExpoScale], _ = exponent.NewMapping(s)
		}
	}
	return m
}()

func getMapping(scale int32) (mapping.Mapping, error) {
	if scale >= minExpoScale && scale <= maxExpoScale {
		if m := scaleMappings[scale-minExpoScale]; m != nil {
			return m, nil
		}
	}
	if scale > 0 {
		return logarithm.NewMapping(scale)
	}
	return exponent.NewMapping(scale)
}

type pendingAttr struct {
	dpIdx int
	start int
	end   int
}

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
	rms := md.ResourceMetrics()
	for i := range rms.Len() {
		sms := rms.At(i).ScopeMetrics()
		for j := range sms.Len() {
			ilm := sms.At(j)
			metrics := ilm.Metrics()
			origLen := metrics.Len()

			extra := 0
			for k := range origLen {
				switch metrics.At(k).Type() {
				case pmetric.MetricTypeHistogram, pmetric.MetricTypeExponentialHistogram, pmetric.MetricTypeSummary:
					extra++
				}
			}
			if extra == 0 {
				continue
			}

			metrics.EnsureCapacity(origLen + extra)
			for k := range origLen {
				metric := metrics.At(k)
				switch metric.Type() {
				case pmetric.MetricTypeHistogram:
					p.processHistogram(metric, ilm)
				case pmetric.MetricTypeExponentialHistogram:
					p.processExponentialHistogram(metric, ilm)
				case pmetric.MetricTypeSummary:
					p.processSummary(metric, ilm)
				}
			}
		}
	}
	return md, nil
}

func (p *elasticsearchProcessor) processSummary(metric pmetric.Metric, ilm pmetric.ScopeMetrics) {
	oldDataPoints := metric.Summary().DataPoints()
	dpCount := oldDataPoints.Len()

	totalNewDPs := 0
	for i := range dpCount {
		totalNewDPs += oldDataPoints.At(i).QuantileValues().Len()
	}
	if totalNewDPs == 0 {
		return
	}

	newDataPoints := appendGaugeMetric(ilm, metric, p.config.SummaryMetricSuffix, totalNewDPs)
	attrKey := p.config.SummaryMetricAttribute

	var rawBuf [256]byte
	buf := rawBuf[:0]
	var stackPending [16]pendingAttr
	pending := stackPending[:0]

	for i := range dpCount {
		oldDataPoint := oldDataPoints.At(i)
		oldAttributes := oldDataPoint.Attributes()
		oldStartTimestamp := oldDataPoint.StartTimestamp()
		oldTimestamp := oldDataPoint.Timestamp()
		quantileValues := oldDataPoint.QuantileValues()

		for j := range quantileValues.Len() {
			quantileValue := quantileValues.At(j)
			dpIdx := newDataPoints.Len()

			newDataPoint := appendNumberDataPoint(newDataPoints, oldAttributes, oldStartTimestamp, oldTimestamp)
			newDataPoint.SetDoubleValue(quantileValue.Value())

			if s, ok := commonFloatStr(quantileValue.Quantile()); ok {
				newDataPoint.Attributes().PutStr(attrKey, s)
			} else {
				start := len(buf)
				buf = strconv.AppendFloat(buf, quantileValue.Quantile(), 'g', -1, 64)
				pending = append(pending, pendingAttr{dpIdx: dpIdx, start: start, end: len(buf)})
			}
		}
	}

	if len(pending) > 0 {
		packed := string(buf)
		for _, item := range pending {
			newDataPoints.At(item.dpIdx).Attributes().PutStr(attrKey, packed[item.start:item.end])
		}
	}
}

func (p *elasticsearchProcessor) processHistogram(metric pmetric.Metric, ilm pmetric.ScopeMetrics) {
	oldDataPoints := metric.Histogram().DataPoints()
	dpCount := oldDataPoints.Len()

	totalNewDPs := 0
	for i := range dpCount {
		dp := oldDataPoints.At(i)
		bc := dp.BucketCounts()
		eb := dp.ExplicitBounds()
		if bc.Len() == 0 {
			continue
		}
		if bc.Len() != eb.Len()+1 {
			p.logger.Debug("skipping histogram data point with mismatched bucket counts and explicit bounds",
				zap.String("metric", metric.Name()),
				zap.Int("bucket_counts", bc.Len()),
				zap.Int("explicit_bounds", eb.Len()),
			)
			continue
		}
		for j := range bc.Len() {
			if bc.At(j) != 0 {
				totalNewDPs++
			}
		}
	}
	if totalNewDPs == 0 {
		return
	}

	newDataPoints := appendGaugeMetric(ilm, metric, p.config.HistogramMetricSuffix, totalNewDPs)
	attrKey := p.config.HistogramMetricAttribute

	var stackBounds [32]string
	var cachedBounds []string
	var prevExplicitBounds pcommon.Float64Slice
	hasPrevBounds := false

	for i := range dpCount {
		oldDataPoint := oldDataPoints.At(i)
		bucketCounts := oldDataPoint.BucketCounts()
		explicitBounds := oldDataPoint.ExplicitBounds()
		if bucketCounts.Len() == 0 || bucketCounts.Len() != explicitBounds.Len()+1 {
			continue
		}

		if !hasPrevBounds || !float64SliceEqual(prevExplicitBounds, explicitBounds) {
			cachedBounds = formatFloat64Slice(explicitBounds, stackBounds[:0])
			prevExplicitBounds = explicitBounds
			hasPrevBounds = true
		}

		oldAttributes := oldDataPoint.Attributes()
		oldStartTimestamp := oldDataPoint.StartTimestamp()
		oldTimestamp := oldDataPoint.Timestamp()
		boundsLen := explicitBounds.Len()

		bucketSum := uint64(0)
		for j := range bucketCounts.Len() {
			count := bucketCounts.At(j)
			if count == 0 {
				continue
			}
			bucketSum += count

			explicitBound := "+Inf"
			if j < boundsLen {
				explicitBound = cachedBounds[j]
			}

			newDataPoint := appendNumberDataPoint(newDataPoints, oldAttributes, oldStartTimestamp, oldTimestamp)
			newDataPoint.SetIntValue(safeUint64ToInt64(bucketSum))
			newDataPoint.Attributes().PutStr(attrKey, explicitBound)
		}
	}
}

func (p *elasticsearchProcessor) processExponentialHistogram(metric pmetric.Metric, ilm pmetric.ScopeMetrics) {
	oldDataPoints := metric.ExponentialHistogram().DataPoints()
	dpCount := oldDataPoints.Len()

	totalNewDPs := 0
	for i := range dpCount {
		dp := oldDataPoints.At(i)
		if _, err := getMapping(dp.Scale()); err != nil {
			p.logger.Debug("skipping exponential histogram data point with invalid scale",
				zap.String("metric", metric.Name()),
				zap.Int32("scale", dp.Scale()),
				zap.Error(err),
			)
			continue
		}
		if dp.ZeroCount() > 0 {
			totalNewDPs++
		}
		posCounts := dp.Positive().BucketCounts()
		for j := range posCounts.Len() {
			if posCounts.At(j) != 0 {
				totalNewDPs++
			}
		}
		negCounts := dp.Negative().BucketCounts()
		for j := range negCounts.Len() {
			if negCounts.At(j) != 0 {
				totalNewDPs++
			}
		}
	}
	if totalNewDPs == 0 {
		return
	}

	newDataPoints := appendGaugeMetric(ilm, metric, p.config.ExponentialHistogramMetricSuffix, totalNewDPs)
	attrKey := p.config.ExponentialHistogramMetricAttribute

	var rawBuf [512]byte
	buf := rawBuf[:0]
	var stackPending [64]pendingAttr
	pending := stackPending[:0]

	for i := range dpCount {
		oldDataPoint := oldDataPoints.At(i)
		m, err := getMapping(oldDataPoint.Scale())
		if err != nil {
			continue
		}

		oldAttributes := oldDataPoint.Attributes()
		oldStartTimestamp := oldDataPoint.StartTimestamp()
		oldTimestamp := oldDataPoint.Timestamp()

		if zeroCount := oldDataPoint.ZeroCount(); zeroCount > 0 {
			newDataPoint := appendNumberDataPoint(newDataPoints, oldAttributes, oldStartTimestamp, oldTimestamp)
			newDataPoint.SetIntValue(safeUint64ToInt64(zeroCount))
			newDataPoint.Attributes().PutStr(attrKey, "0")
		}

		positive := oldDataPoint.Positive()
		p.appendExpoBuckets(
			newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
			m,
			positive.Offset(),
			positive.BucketCounts(),
			false,
			attrKey,
			&buf,
			&pending,
		)

		negative := oldDataPoint.Negative()
		p.appendExpoBuckets(
			newDataPoints,
			oldAttributes,
			oldStartTimestamp,
			oldTimestamp,
			m,
			negative.Offset(),
			negative.BucketCounts(),
			true,
			attrKey,
			&buf,
			&pending,
		)
	}

	if newDataPoints.Len() == 0 {
		metrics := ilm.Metrics()
		lastIdx := metrics.Len() - 1
		idx := 0
		metrics.RemoveIf(func(_ pmetric.Metric) bool {
			remove := idx == lastIdx
			idx++
			return remove
		})
		return
	}

	if len(pending) > 0 {
		packed := string(buf)
		for _, item := range pending {
			newDataPoints.At(item.dpIdx).Attributes().PutStr(attrKey, packed[item.start:item.end])
		}
	}
}

func (*elasticsearchProcessor) appendExpoBuckets(
	newDataPoints pmetric.NumberDataPointSlice,
	oldAttributes pcommon.Map,
	oldStartTimestamp, oldTimestamp pcommon.Timestamp,
	m mapping.Mapping,
	offset int32,
	bucketCounts pcommon.UInt64Slice,
	negate bool,
	attrKey string,
	buf *[]byte,
	pending *[]pendingAttr,
) {
	prevIdx := int32(0)
	prevUB := 0.0
	hasPrev := false

	for j := range bucketCounts.Len() {
		count := bucketCounts.At(j)
		if count == 0 {
			continue
		}

		idx := offset + int32(j)
		var lowerMag float64
		var err error
		if hasPrev && idx == prevIdx+1 {
			lowerMag = prevUB
		} else {
			lowerMag, err = m.LowerBoundary(idx)
			if err != nil {
				hasPrev = false
				continue
			}
		}

		upperMag, err := m.LowerBoundary(idx + 1)
		if err != nil {
			hasPrev = false
			continue
		}
		prevIdx = idx
		prevUB = upperMag
		hasPrev = true

		midpoint := lowerMag + (upperMag-lowerMag)/2
		if negate {
			midpoint = -midpoint
		}

		dpIdx := newDataPoints.Len()
		newDataPoint := appendNumberDataPoint(newDataPoints, oldAttributes, oldStartTimestamp, oldTimestamp)
		newDataPoint.SetIntValue(safeUint64ToInt64(count))

		if s, ok := commonFloatStr(midpoint); ok {
			newDataPoint.Attributes().PutStr(attrKey, s)
		} else {
			start := len(*buf)
			*buf = strconv.AppendFloat(*buf, midpoint, 'g', -1, 64)
			*pending = append(*pending, pendingAttr{dpIdx: dpIdx, start: start, end: len(*buf)})
		}
	}
}

func appendGaugeMetric(ilm pmetric.ScopeMetrics, src pmetric.Metric, suffix string, dpCapacity int) pmetric.NumberDataPointSlice {
	newMetric := ilm.Metrics().AppendEmpty()
	if suffix == "" {
		newMetric.SetName(src.Name())
	} else {
		newMetric.SetName(src.Name() + suffix)
	}
	newMetric.SetDescription(src.Description())
	newMetric.SetUnit(src.Unit())

	dps := newMetric.SetEmptyGauge().DataPoints()
	dps.EnsureCapacity(dpCapacity)
	return dps
}

func appendNumberDataPoint(
	newDataPoints pmetric.NumberDataPointSlice,
	oldAttributes pcommon.Map,
	oldStartTimestamp, oldTimestamp pcommon.Timestamp,
) pmetric.NumberDataPoint {
	newDataPoint := newDataPoints.AppendEmpty()
	newDataPoint.SetStartTimestamp(oldStartTimestamp)
	newDataPoint.SetTimestamp(oldTimestamp)

	oldLen := oldAttributes.Len()
	attrs := newDataPoint.Attributes()
	attrs.EnsureCapacity(oldLen + 1)
	if oldLen > 0 {
		oldAttributes.CopyTo(attrs)
	}
	return newDataPoint
}

// float64SliceEqual compares two pcommon.Float64Slices in-place without calling AsRaw(),
// which clones and heap-allocates a new []float64 slice in pdata.
func float64SliceEqual(a, b pcommon.Float64Slice) bool {
	if a.Len() != b.Len() {
		return false
	}
	for i := range a.Len() {
		if a.At(i) != b.At(i) {
			return false
		}
	}
	return true
}

func formatFloat64Slice(vals pcommon.Float64Slice, dst []string) []string {
	n := vals.Len()
	if cap(dst) < n {
		dst = make([]string, n)
	} else {
		dst = dst[:n]
	}

	var rawBuf [256]byte
	buf := rawBuf[:0]
	var stackPending [32]pendingAttr
	pending := stackPending[:0]

	for i := range n {
		v := vals.At(i)
		if s, ok := commonFloatStr(v); ok {
			dst[i] = s
			continue
		}
		start := len(buf)
		buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		pending = append(pending, pendingAttr{dpIdx: i, start: start, end: len(buf)})
	}

	if len(pending) > 0 {
		packed := string(buf)
		for _, item := range pending {
			dst[item.dpIdx] = packed[item.start:item.end]
		}
	}
	return dst
}

// commonFloatStr returns compile-time string literals for standard OTel/Prometheus
// explicit bounds, quantiles, and small midpoints, avoiding heap allocations.
func commonFloatStr(v float64) (string, bool) {
	switch v {
	case 0:
		if math.Signbit(v) {
			return "-0", true
		}
		return "0", true
	case 0.005:
		return "0.005", true
	case 0.01:
		return "0.01", true
	case 0.025:
		return "0.025", true
	case 0.05:
		return "0.05", true
	case 0.075:
		return "0.075", true
	case 0.1:
		return "0.1", true
	case 0.25:
		return "0.25", true
	case 0.5:
		return "0.5", true
	case 0.75:
		return "0.75", true
	case 0.9:
		return "0.9", true
	case 0.95:
		return "0.95", true
	case 0.99:
		return "0.99", true
	case 0.999:
		return "0.999", true
	case 1:
		return "1", true
	case 1.5:
		return "1.5", true
	case -1.5:
		return "-1.5", true
	case 2.5:
		return "2.5", true
	case -2.5:
		return "-2.5", true
	case 5:
		return "5", true
	case 7.5:
		return "7.5", true
	case 10:
		return "10", true
	case 25:
		return "25", true
	case 50:
		return "50", true
	case 75:
		return "75", true
	case 100:
		return "100", true
	case 250:
		return "250", true
	case 500:
		return "500", true
	case 750:
		return "750", true
	case 1000:
		return "1000", true
	case 2500:
		return "2500", true
	case 5000:
		return "5000", true
	case 7500:
		return "7500", true
	case 10000:
		return "10000", true
	default:
		return "", false
	}
}

func safeUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}
