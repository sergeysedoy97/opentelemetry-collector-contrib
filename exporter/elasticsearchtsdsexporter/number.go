// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type Number struct {
	pmetric.NumberDataPoint
	metric          pmetric.Metric
	dynamicTemplate string
}

func (ndp *Number) Value() pcommon.Value {
	switch ndp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return pcommon.NewValueDouble(ndp.DoubleValue())
	case pmetric.NumberDataPointValueTypeInt:
		return pcommon.NewValueInt(ndp.IntValue())
	}
	return pcommon.NewValueEmpty()
}

func (ndp *Number) Metric() pmetric.Metric {
	return ndp.metric
}

func (ndp *Number) DynamicTemplate() string {
	return ndp.dynamicTemplate
}
