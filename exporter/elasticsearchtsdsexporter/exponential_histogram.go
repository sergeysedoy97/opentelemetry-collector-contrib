// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type ExponentialHistogram struct {
	pmetric.ExponentialHistogramDataPoint
	metric          pmetric.Metric
	dynamicTemplate string
}

func (ehdp *ExponentialHistogram) Value() pcommon.Value {
	vm := pcommon.NewValueMap()
	m := vm.Map()

	m.PutInt("scale", int64(ehdp.Scale()))

	if ehdp.HasSum() {
		m.PutDouble("sum", ehdp.Sum())
	}
	if ehdp.HasMin() {
		m.PutDouble("min", ehdp.Min())
	}
	if ehdp.HasMax() {
		m.PutDouble("max", ehdp.Max())
	}

	zm := m.PutEmptyMap("zero")
	zm.PutDouble("threshold", ehdp.ZeroThreshold())
	zm.PutInt("count", int64(ehdp.ZeroCount()))

	pbc := ehdp.Positive().BucketCounts()
	pbcLen := pbc.Len()
	if pbcLen > 0 {
		pm := m.PutEmptyMap("positive")
		pi := pm.PutEmptySlice("indices")
		pc := pm.PutEmptySlice("counts")

		pi.EnsureCapacity(pbcLen)
		pc.EnsureCapacity(pbcLen)

		po := ehdp.Positive().Offset()
		for i := range pbcLen {
			count := pbc.At(i)
			if count == 0 {
				continue
			}
			pi.AppendEmpty().SetInt(int64(po) + int64(i))
			pc.AppendEmpty().SetInt(safeUint64ToInt64(count))
		}
	}

	nbc := ehdp.Negative().BucketCounts()
	nbcLen := nbc.Len()
	if nbcLen > 0 {
		nm := m.PutEmptyMap("negative")
		ni := nm.PutEmptySlice("indices")
		nc := nm.PutEmptySlice("counts")

		ni.EnsureCapacity(nbcLen)
		nc.EnsureCapacity(nbcLen)

		no := ehdp.Negative().Offset()
		for i := range nbcLen {
			count := nbc.At(i)
			if count == 0 {
				continue
			}
			ni.AppendEmpty().SetInt(int64(no) + int64(i))
			nc.AppendEmpty().SetInt(safeUint64ToInt64(count))
		}
	}

	return vm
}

func (ehdp *ExponentialHistogram) Metric() pmetric.Metric {
	return ehdp.metric
}

func (ehdp *ExponentialHistogram) DynamicTemplate() string {
	return ehdp.dynamicTemplate
}
