// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"math"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	DataStreamDataset   = "data_stream.dataset"
	DataStreamNamespace = "data_stream.namespace"
)

type DataPoint interface {
	Value() pcommon.Value
	Metric() pmetric.Metric
	DynamicTemplate() string
}

type DataPointGroup struct {
	resourceMap  *pcommon.Map
	scopeMap     *pcommon.Map
	dpMap        *pcommon.Map
	scopeName    string
	scopeVersion string
	dps          []DataPoint
}

func (dpg *DataPointGroup) stringifyMaps() string {
	var sb strings.Builder
	for _, m := range []*pcommon.Map{dpg.dpMap, dpg.scopeMap, dpg.resourceMap} {
		for k, v := range m.All() {
			if v.Type() == pcommon.ValueTypeStr {
				sb.WriteString(k)
				sb.WriteByte(':')
				sb.WriteString(v.Str())
				sb.WriteByte(',')
			}
		}
	}
	return sb.String()
}

type DataStream struct {
	dataset   string
	namespace string
}

type DataPointHash struct {
	resourceHash uint64
	scopeHash    uint64
	dpHash       uint64
	ts           uint64
}

type DataPointGroupKey struct {
	ds     DataStream
	dpHash DataPointHash
}

type BulkError struct {
	Error struct {
		Reason string `json:"reason"`
		Type   string `json:"type"`
	} `json:"error"`
	Status int `json:"status"`
}

type BulkItems struct {
	Items []struct {
		Create BulkError `json:"create"`
	} `json:"items"`
}

func safeUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}
