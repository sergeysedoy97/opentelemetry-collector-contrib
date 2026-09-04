// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type Histogram struct {
	pmetric.HistogramDataPoint
	metric          pmetric.Metric
	dynamicTemplate string
}

func (hdp *Histogram) Value() pcommon.Value {
	bc := hdp.BucketCounts()
	eb := hdp.ExplicitBounds()

	bcLen := bc.Len()
	ebLen := eb.Len()

	if bcLen > 0 && bcLen != ebLen+1 {
		return pcommon.Value{}
	}

	vm := pcommon.NewValueMap()
	m := vm.Map()
	cnts := m.PutEmptySlice("counts")
	vals := m.PutEmptySlice("values")

	if ebLen == 0 {
		// It is possible for explicit bounds to be nil. In this case create
		// a bucket using the count and sum which are required to be present.
		// See https://opentelemetry.io/docs/specs/otel/metrics/data-model/#histogram
		if hdp.Count() > 0 {
			cnts.AppendEmpty().SetInt(safeUint64ToInt64(hdp.Count()))
			vals.AppendEmpty().SetDouble(hdp.Sum() / float64(hdp.Count()))
		}

		return vm
	}

	cnts.EnsureCapacity(bcLen)
	vals.EnsureCapacity(bcLen)

	for i := range bcLen {
		count := bc.At(i)
		if count == 0 {
			continue
		}
		if i == ebLen {
			// In raw mode, the overflow bucket would have the same value
			// as the last real bucket. Merge the overflow count into the
			// last real bucket to avoid duplicate values, which violates
			// ES histogram's strictly increasing values requirement.
			lastIdx := cnts.Len() - 1
			if lastIdx >= 0 && count > 0 {
				cnts.At(lastIdx).SetInt(cnts.At(lastIdx).Int() + safeUint64ToInt64(count))
			}
			break
		}

		cnts.AppendEmpty().SetInt(safeUint64ToInt64(count))
		vals.AppendEmpty().SetDouble(eb.At(i))
	}

	return vm
}

func (hdp *Histogram) Metric() pmetric.Metric {
	return hdp.metric
}

func (hdp *Histogram) DynamicTemplate() string {
	return hdp.dynamicTemplate
}
