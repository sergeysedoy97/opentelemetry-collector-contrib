// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/gzip"
	"github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter/internal/metadata"
	"github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/oneconfextension"
	"gitlab.rip/golang/platform-tech-services/one_configuration"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.uber.org/zap"
)

type ElasticsearchExporter struct {
	config           *Config
	tb               *metadata.TelemetryBuilder
	sel              *Selector
	client           *http.Client
	dsMetrics        atomic.Pointer[map[DataStream]*dsMetrics]
	statusLatencyMap map[int][]metric.RecordOption
	host             component.Host
	attr             attribute.KeyValue
	set              exporter.Settings
	url              string
	optDocsCreated   []metric.AddOption
	optLatencyOK     []metric.RecordOption
	optLatencyRetry  []metric.RecordOption
	dsMetricsMu      sync.Mutex
	statusLatencyMu  sync.RWMutex
}

func NewExporter(config *Config, set exporter.Settings) (*ElasticsearchExporter, error) {
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	v := make(url.Values, 3)
	v.Set("filter_path", "items.create.status,items.create.error.reason,items.create.error.type")
	if config.Pipeline != "" {
		v.Set("pipeline", config.Pipeline)
	}
	v.Set("require_data_stream", "true")

	u := url.URL{
		Path:     "/_bulk",
		RawQuery: v.Encode(),
	}
	uri := u.RequestURI()

	sel := NewSelector(uri)
	sel.SetupRR([]string{config.Endpoint})

	attr := attribute.String("exporter", set.ID.String())
	initMap := make(map[DataStream]*dsMetrics, 64)

	exp := &ElasticsearchExporter{
		config:           config,
		tb:               tb,
		sel:              sel,
		statusLatencyMap: make(map[int][]metric.RecordOption, 8),
		attr:             attr,
		set:              set,
		url:              uri,
		optDocsCreated: []metric.AddOption{
			metric.WithAttributeSet(attribute.NewSet(attr, attrCreated)),
		},
		optLatencyOK: []metric.RecordOption{
			metric.WithAttributeSet(attribute.NewSet(attr, attrOK)),
		},
		optLatencyRetry: []metric.RecordOption{
			metric.WithAttributeSet(attribute.NewSet(attr, attrOK, attrRetry)),
		},
	}
	exp.dsMetrics.Store(&initMap)

	return exp, nil
}

func (exp *ElasticsearchExporter) Start(ctx context.Context, host component.Host) error {
	exp.config.Headers.Set("Content-Type", "application/x-ndjson")
	exp.config.Headers.Set("Content-Encoding", "gzip")

	client, err := exp.config.ToClient(ctx, host.GetExtensions(), exp.set.TelemetrySettings)
	if err != nil {
		return err
	}

	ext := oneconfextension.Get(host)
	if ext != nil {
		conf := ext.GetConf()
		if conf != nil {
			if v, ok := conf.GetString(oneConfProperty); ok {
				e1 := exp.sel.SetupConf(v)
				if e1 == nil {
					exp.sel.unsubscribe = conf.SubscribeString(oneConfProperty, func(_ one_configuration.Key, _, cur *string) {
						if cur == nil {
							return
						}
						e2 := exp.sel.SetupConf(*cur)
						if e2 != nil {
							exp.set.Logger.Error("setupConf", zap.Error(e2))
						}
					})
				} else {
					exp.set.Logger.Error("setupConf", zap.Error(e1))
				}
			}
		}
	}

	exp.host = host
	exp.client = client

	return nil
}

func (exp *ElasticsearchExporter) Shutdown(context.Context) error {
	if exp.tb != nil {
		exp.tb.Shutdown()
		exp.tb = nil
	}
	if exp.sel != nil {
		exp.sel.Unsubscribe()
		exp.sel = nil
	}
	exp.dsMetrics.Store(nil)
	return nil
}

func (exp *ElasticsearchExporter) getDSMetrics(ds DataStream) *dsMetrics {
	if m := exp.dsMetrics.Load(); m != nil {
		if dsm, ok := (*m)[ds]; ok {
			return dsm
		}
	}
	return exp.getOrCreateDSMetricsSlow(ds)
}

func (exp *ElasticsearchExporter) getOrCreateDSMetricsSlow(ds DataStream) *dsMetrics {
	exp.dsMetricsMu.Lock()
	defer exp.dsMetricsMu.Unlock()

	cur := exp.dsMetrics.Load()
	if cur != nil {
		if dsm, ok := (*cur)[ds]; ok {
			return dsm
		}
	}

	clonedDS := DataStream{
		dataset:   strings.Clone(ds.dataset),
		namespace: strings.Clone(ds.namespace),
	}
	dsm := exp.buildDSMetrics(clonedDS)

	var next map[DataStream]*dsMetrics
	if cur == nil || len(*cur) >= maxCachedDataStreams {
		next = make(map[DataStream]*dsMetrics, 64)
	} else {
		next = make(map[DataStream]*dsMetrics, len(*cur)+1)
		maps.Copy(next, *cur)
	}
	next[clonedDS] = dsm
	exp.dsMetrics.Store(&next)

	return dsm
}

func (exp *ElasticsearchExporter) buildDSMetrics(ds DataStream) *dsMetrics {
	attrDS := attribute.String("dataset", ds.dataset)
	attrNS := attribute.String("namespace", ds.namespace)

	return &dsMetrics{
		dpNumEmpty:   []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrNum, attrEmpty))},
		dpNumDup:     []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrNum, attrDup))},
		dpNum:        []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrNum))},
		dpHisDup:     []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrHis, attrDup))},
		dpHis:        []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrHis))},
		dpExpHisDup:  []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrExpHis, attrDup))},
		dpExpHis:     []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrExpHis))},
		docsReceived: []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS))},
	}
}

func (exp *ElasticsearchExporter) getStatusLatencyOpt(statusCode int) []metric.RecordOption {
	exp.statusLatencyMu.RLock()
	opts, ok := exp.statusLatencyMap[statusCode]
	exp.statusLatencyMu.RUnlock()
	if ok {
		return opts
	}

	opts = []metric.RecordOption{
		metric.WithAttributeSet(attribute.NewSet(exp.attr, semconv.HTTPResponseStatusCodeKey.Int(statusCode))),
	}

	exp.statusLatencyMu.Lock()
	if len(exp.statusLatencyMap) < 64 {
		exp.statusLatencyMap[statusCode] = opts
	}
	exp.statusLatencyMu.Unlock()

	return opts
}

func (exp *ElasticsearchExporter) getOrCreateDPG(
	rs *RequestState,
	dph DataPointHash,
	scopeDS DataStream,
	scopeDSM **dsMetrics,
	resourceMap pcommon.Map,
	scopeMap pcommon.Map,
	dpMap pcommon.Map,
	scopeName string,
	scopeVersion string,
) *DataPointGroup {
	if dpg, ok := rs.dph2dpg[dph]; ok {
		return dpg
	}

	dataStream := scopeDS
	if dpMap.Len() > 0 {
		dataStream = dataStream.Update(dpMap)
	}

	var dsm *dsMetrics
	if dataStream == scopeDS {
		if *scopeDSM == nil {
			*scopeDSM = exp.getDSMetrics(scopeDS)
		}
		dsm = *scopeDSM
	} else {
		dsm = exp.getDSMetrics(dataStream)
	}

	dpg := dpgPool.Get().(*DataPointGroup)
	dpg.resourceMap = resourceMap
	dpg.scopeMap = scopeMap
	dpg.dpMap = dpMap
	dpg.metrics = dsm
	dpg.dataStream = dataStream
	dpg.scopeName = scopeName
	dpg.scopeVersion = scopeVersion
	rs.dph2dpg[dph] = dpg

	return dpg
}

func (exp *ElasticsearchExporter) requestConverter(ctx context.Context, md pmetric.Metrics) (xexporterhelper.Request, error) {
	var ndps pmetric.NumberDataPointSlice
	var hdps pmetric.HistogramDataPointSlice
	var ehdps pmetric.ExponentialHistogramDataPointSlice

	b := bufPool.Get().(*bytes.Buffer)
	z := gzipPool.Get().(*gzip.Writer)
	w := bufioPool.Get().(*bufio.Writer)
	rs := requestStatePool.Get().(*RequestState)

	b.Reset()
	z.Reset(b)
	w.Reset(z)

	defer func() {
		for _, dpg := range rs.dph2dpg {
			dpg.Reset()
			dpgPool.Put(dpg)
		}
		rs.Reset()
		requestStatePool.Put(rs)

		if b.Cap() <= maxPooledBufCap {
			bufPool.Put(b)
		}
		gzipPool.Put(z)
		bufioPool.Put(w)
	}()

	defaultDS := DataStream{dataset: "otelcol.v1", namespace: "garbage"}
	rms := md.ResourceMetrics()
	for i := range rms.Len() {
		rm := rms.At(i)
		resourceMap := rm.Resource().Attributes()
		resourceDS := defaultDS
		var resourceHash uint64
		if resourceMap.Len() > 0 {
			resourceHash = hashMap(resourceMap)
			resourceDS = resourceDS.Update(resourceMap)
		}

		sms := rm.ScopeMetrics()
		for j := range sms.Len() {
			sm := sms.At(j)
			scope := sm.Scope()
			scopeName := scope.Name()
			scopeVersion := scope.Version()
			scopeMap := scope.Attributes()
			scopeDS := resourceDS
			if scopeMap.Len() > 0 {
				scopeDS = scopeDS.Update(scopeMap)
			}
			scopeHash := hashScope(scopeName, scopeVersion, scopeMap)
			var scopeDSM *dsMetrics

			ms := sm.Metrics()
			for k := range ms.Len() {
				m := ms.At(k)
				mt := m.Type()
				mn := m.Name()

				switch mt {
				case pmetric.MetricTypeGauge:
					ndps = m.Gauge().DataPoints()
				case pmetric.MetricTypeSum:
					tmp := m.Sum()
					if tmp.IsMonotonic() && tmp.AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						if ce := exp.set.Logger.Check(zap.InfoLevel, "Metric.Temporality"); ce != nil {
							ce.Write(zap.String("metric", mn), zap.String("type", mt.String()), zap.String("attributes", stringifyMaps(resourceMap, scopeMap)))
						}
						continue
					}
					ndps = tmp.DataPoints()
				case pmetric.MetricTypeHistogram:
					tmp := m.Histogram()
					if tmp.AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						if ce := exp.set.Logger.Check(zap.InfoLevel, "Metric.Temporality"); ce != nil {
							ce.Write(zap.String("metric", mn), zap.String("type", mt.String()), zap.String("attributes", stringifyMaps(resourceMap, scopeMap)))
						}
						continue
					}
					hdps = tmp.DataPoints()
				case pmetric.MetricTypeExponentialHistogram:
					tmp := m.ExponentialHistogram()
					if tmp.AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						if ce := exp.set.Logger.Check(zap.InfoLevel, "Metric.Temporality"); ce != nil {
							ce.Write(zap.String("metric", mn), zap.String("type", mt.String()), zap.String("attributes", stringifyMaps(resourceMap, scopeMap)))
						}
						continue
					}
					ehdps = tmp.DataPoints()
				default:
					continue
				}

				switch mt {
				case pmetric.MetricTypeGauge, pmetric.MetricTypeSum:
					for l := range ndps.Len() {
						ndp := ndps.At(l)
						dpMap := ndp.Attributes()
						dph := DataPointHash{
							resourceHash: resourceHash,
							scopeHash:    scopeHash,
							dpHash:       hashMap(dpMap),
							ts:           uint64(ndp.Timestamp()) / 1000000,
						}
						dpg := exp.getOrCreateDPG(rs, dph, scopeDS, &scopeDSM, resourceMap, scopeMap, dpMap, scopeName, scopeVersion)
						dpg.ndps = append(dpg.ndps, namedNDP{dp: ndp, name: mn})
					}
				case pmetric.MetricTypeHistogram:
					for l := range hdps.Len() {
						hdp := hdps.At(l)
						dpMap := hdp.Attributes()
						dph := DataPointHash{
							resourceHash: resourceHash,
							scopeHash:    scopeHash,
							dpHash:       hashMap(dpMap),
							ts:           uint64(hdp.Timestamp()) / 1000000,
						}
						dpg := exp.getOrCreateDPG(rs, dph, scopeDS, &scopeDSM, resourceMap, scopeMap, dpMap, scopeName, scopeVersion)
						dpg.hdps = append(dpg.hdps, namedHDP{dp: hdp, name: mn})
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := range ehdps.Len() {
						ehdp := ehdps.At(l)
						dpMap := ehdp.Attributes()
						dph := DataPointHash{
							resourceHash: resourceHash,
							scopeHash:    scopeHash,
							dpHash:       hashMap(dpMap),
							ts:           uint64(ehdp.Timestamp()) / 1000000,
						}
						dpg := exp.getOrCreateDPG(rs, dph, scopeDS, &scopeDSM, resourceMap, scopeMap, dpMap, scopeName, scopeVersion)
						dpg.ehdps = append(dpg.ehdps, namedEHDP{dp: ehdp, name: mn})
					}
				}
			}
		}
	}

	docCount := len(rs.dph2dpg)
	if docCount == 0 {
		return nil, errors.New("empty request")
	}

	var (
		d                                     xxhash.Digest
		batchDSM                              *dsMetrics
		batchNumEmpty, batchNumDup, batchNum  int64
		batchHisDup, batchHis                 int64
		batchExpHisDup, batchExpHis, batchDoc int64
	)

	for dph, dpg := range rs.dph2dpg {
		_, _ = w.WriteString(json1)
		writeSanitizedString(w, dpg.dataStream.dataset)
		_ = w.WriteByte('-')
		writeSanitizedString(w, dpg.dataStream.namespace)
		_, _ = w.WriteString(json2)

		var empty, dup, suc int64
		isFirstTemplate := true
		clear(rs.seenMap)

		for i := range dpg.ndps {
			entry := &dpg.ndps[i]
			metricName := entry.name
			valueType := entry.dp.ValueType()

			if valueType == pmetric.NumberDataPointValueTypeEmpty {
				empty++
				if ce := exp.set.Logger.Check(zap.InfoLevel, "DataPoint.Empty"); ce != nil {
					ce.Write(zap.String("metric", metricName), zap.String("attributes", stringifyMaps(dpg.resourceMap, dpg.scopeMap, dpg.dpMap)))
				}
				entry.name = ""
				continue
			}

			if _, ok := rs.seenMap[metricName]; ok {
				dup++
				if ce := exp.set.Logger.Check(zap.InfoLevel, "DataPoint.Duplicate"); ce != nil {
					ce.Write(zap.String("metric", metricName), zap.String("attributes", stringifyMaps(dpg.resourceMap, dpg.scopeMap, dpg.dpMap)))
				}
				entry.name = ""
				continue
			}

			rs.seenMap[metricName] = struct{}{}

			if !isFirstTemplate {
				_ = w.WriteByte(',')
			}
			_, _ = w.WriteString(json3)
			writeSanitizedString(w, metricName)
			_, _ = w.WriteString(qcq)

			switch valueType {
			case pmetric.NumberDataPointValueTypeInt:
				_, _ = w.WriteString(gldt)
			case pmetric.NumberDataPointValueTypeDouble:
				_, _ = w.WriteString(gddt)
			}

			_ = w.WriteByte('"')
			isFirstTemplate = false
		}

		dsm := dpg.metrics
		if batchDSM == nil {
			batchDSM = dsm
		}

		suc = int64(len(dpg.ndps)) - empty - dup
		if dsm == batchDSM {
			batchNumEmpty += empty
			batchNumDup += dup
			batchNum += suc
		} else {
			if empty > 0 {
				exp.tb.ElasticsearchDatapointsReceived.Add(ctx, empty, dsm.dpNumEmpty...)
			}
			if dup > 0 {
				exp.tb.ElasticsearchDatapointsReceived.Add(ctx, dup, dsm.dpNumDup...)
			}
			if suc > 0 {
				exp.tb.ElasticsearchDatapointsReceived.Add(ctx, suc, dsm.dpNum...)
			}
		}
		dup = 0

		for i := range dpg.hdps {
			entry := &dpg.hdps[i]
			metricName := entry.name

			if _, ok := rs.seenMap[metricName]; ok {
				dup++
				if ce := exp.set.Logger.Check(zap.InfoLevel, "DataPoint.Duplicate"); ce != nil {
					ce.Write(zap.String("metric", metricName), zap.String("attributes", stringifyMaps(dpg.resourceMap, dpg.scopeMap, dpg.dpMap)))
				}
				entry.name = ""
				continue
			}

			rs.seenMap[metricName] = struct{}{}

			if !isFirstTemplate {
				_ = w.WriteByte(',')
			}
			_, _ = w.WriteString(json3)
			writeSanitizedString(w, metricName)
			_, _ = w.WriteString(qcq)
			_, _ = w.WriteString(hdt)
			_ = w.WriteByte('"')

			isFirstTemplate = false
		}

		suc = int64(len(dpg.hdps)) - dup
		if dsm == batchDSM {
			batchHisDup += dup
			batchHis += suc
		} else {
			if dup > 0 {
				exp.tb.ElasticsearchDatapointsReceived.Add(ctx, dup, dsm.dpHisDup...)
			}
			if suc > 0 {
				exp.tb.ElasticsearchDatapointsReceived.Add(ctx, suc, dsm.dpHis...)
			}
		}
		dup = 0

		for i := range dpg.ehdps {
			entry := &dpg.ehdps[i]
			metricName := entry.name

			if _, ok := rs.seenMap[metricName]; ok {
				dup++
				if ce := exp.set.Logger.Check(zap.InfoLevel, "DataPoint.Duplicate"); ce != nil {
					ce.Write(zap.String("metric", metricName), zap.String("attributes", stringifyMaps(dpg.resourceMap, dpg.scopeMap, dpg.dpMap)))
				}
				entry.name = ""
				continue
			}

			rs.seenMap[metricName] = struct{}{}

			if !isFirstTemplate {
				_ = w.WriteByte(',')
			}
			_, _ = w.WriteString(json3)
			writeSanitizedString(w, metricName)
			_, _ = w.WriteString(qcq)
			_, _ = w.WriteString(ehdt)
			_ = w.WriteByte('"')

			isFirstTemplate = false
		}

		suc = int64(len(dpg.ehdps)) - dup
		if dsm == batchDSM {
			batchExpHisDup += dup
			batchExpHis += suc
			batchDoc++
		} else {
			if dup > 0 {
				exp.tb.ElasticsearchDatapointsReceived.Add(ctx, dup, dsm.dpExpHisDup...)
			}
			if suc > 0 {
				exp.tb.ElasticsearchDatapointsReceived.Add(ctx, suc, dsm.dpExpHis...)
			}
			exp.tb.ElasticsearchDocsReceived.Add(ctx, one, dsm.docsReceived...)
		}

		_, _ = w.WriteString(json4)
		_, _ = w.Write(strconv.AppendUint(rs.stash[:0], dph.ts, 10))
		_, _ = w.WriteString(json5)
		writeSanitizedString(w, dpg.dataStream.dataset)
		_, _ = w.WriteString(json6)
		writeSanitizedString(w, dpg.dataStream.namespace)
		_, _ = w.WriteString(json7)
		rs.WriteMap(w, dpg.dpMap)
		_, _ = w.WriteString(json8)
		rs.WriteMap(w, dpg.resourceMap)
		_, _ = w.WriteString(json9)
		rs.WriteMap(w, dpg.scopeMap)
		_, _ = w.WriteString(json10)
		if dpg.scopeName != "" {
			writeSanitizedString(w, dpg.scopeName)
		}
		_, _ = w.WriteString(json11)
		if dpg.scopeVersion != "" {
			writeSanitizedString(w, dpg.scopeVersion)
		}
		_, _ = w.WriteString(json12)

		isFirstMetric := true
		rs.sortedMetricSlice = rs.sortedMetricSlice[:0]

		for i := range dpg.ndps {
			entry := &dpg.ndps[i]
			if entry.name == "" {
				continue
			}

			rs.sortedMetricSlice = append(rs.sortedMetricSlice, entry.name)

			if !isFirstMetric {
				_ = w.WriteByte(',')
			}
			_ = w.WriteByte('"')
			writeSanitizedString(w, entry.name)
			_, _ = w.WriteString(qc)

			switch entry.dp.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				_, _ = w.Write(strconv.AppendInt(rs.stash[:0], entry.dp.IntValue(), 10))
			case pmetric.NumberDataPointValueTypeDouble:
				rs.WriteFloat(w, entry.dp.DoubleValue())
			}

			isFirstMetric = false
		}

		for i := range dpg.hdps {
			entry := &dpg.hdps[i]
			if entry.name == "" {
				continue
			}

			rs.sortedMetricSlice = append(rs.sortedMetricSlice, entry.name)

			if !isFirstMetric {
				_ = w.WriteByte(',')
			}
			_ = w.WriteByte('"')
			writeSanitizedString(w, entry.name)
			_, _ = w.WriteString(qc)

			eb := entry.dp.ExplicitBounds()
			bc := entry.dp.BucketCounts()
			minLen := min(eb.Len(), bc.Len())

			_, _ = w.WriteString(json15)

			firstBucket := true
			for idx := range minLen {
				if bc.At(idx) == 0 {
					continue
				}
				if !firstBucket {
					_ = w.WriteByte(',')
				}
				rs.WriteFloat(w, eb.At(idx))
				firstBucket = false
			}

			_, _ = w.WriteString(json16)

			firstBucket = true
			for idx := range minLen {
				count := bc.At(idx)
				if count == 0 {
					continue
				}
				if !firstBucket {
					_ = w.WriteByte(',')
				}
				_, _ = w.Write(strconv.AppendInt(rs.stash[:0], safeUint64ToInt64(count), 10))
				firstBucket = false
			}

			_, _ = w.WriteString(json17)
			isFirstMetric = false
		}

		for i := range dpg.ehdps {
			entry := &dpg.ehdps[i]
			if entry.name == "" {
				continue
			}

			rs.sortedMetricSlice = append(rs.sortedMetricSlice, entry.name)

			if !isFirstMetric {
				_ = w.WriteByte(',')
			}
			_ = w.WriteByte('"')
			writeSanitizedString(w, entry.name)
			_, _ = w.WriteString(qc)
			_, _ = w.WriteString(json18)
			_, _ = w.Write(strconv.AppendInt(rs.stash[:0], int64(entry.dp.Scale()), 10))

			if entry.dp.HasSum() {
				sum := entry.dp.Sum()
				if !math.IsInf(sum, 0) && !math.IsNaN(sum) {
					_, _ = w.WriteString(json19)
					rs.WriteFloat(w, sum)
				}
			}
			if entry.dp.HasMin() {
				minVal := entry.dp.Min()
				if !math.IsInf(minVal, 0) && !math.IsNaN(minVal) {
					_, _ = w.WriteString(json20)
					rs.WriteFloat(w, minVal)
				}
			}
			if entry.dp.HasMax() {
				maxVal := entry.dp.Max()
				if !math.IsInf(maxVal, 0) && !math.IsNaN(maxVal) {
					_, _ = w.WriteString(json21)
					rs.WriteFloat(w, maxVal)
				}
			}

			_, _ = w.WriteString(json22)
			rs.WriteFloat(w, entry.dp.ZeroThreshold())
			_, _ = w.WriteString(json23)
			_, _ = w.Write(strconv.AppendInt(rs.stash[:0], safeUint64ToInt64(entry.dp.ZeroCount()), 10))
			_ = w.WriteByte('}')

			p := entry.dp.Positive()
			pbc := p.BucketCounts()
			pbcLen := pbc.Len()
			if pbcLen > 0 {
				o := p.Offset()
				_, _ = w.WriteString(json24)

				firstBucket := true
				for idx := range pbcLen {
					if pbc.At(idx) == 0 {
						continue
					}
					if !firstBucket {
						_ = w.WriteByte(',')
					}
					_, _ = w.Write(strconv.AppendInt(rs.stash[:0], int64(o)+int64(idx), 10))
					firstBucket = false
				}

				_, _ = w.WriteString(json16)

				firstBucket = true
				for idx := range pbcLen {
					count := pbc.At(idx)
					if count == 0 {
						continue
					}
					if !firstBucket {
						_ = w.WriteByte(',')
					}
					_, _ = w.Write(strconv.AppendInt(rs.stash[:0], safeUint64ToInt64(count), 10))
					firstBucket = false
				}

				_, _ = w.WriteString(json17)
			}

			n := entry.dp.Negative()
			nbc := n.BucketCounts()
			nbcLen := nbc.Len()
			if nbcLen > 0 {
				o := n.Offset()
				_, _ = w.WriteString(json25)

				firstBucket := true
				for idx := range nbcLen {
					if nbc.At(idx) == 0 {
						continue
					}
					if !firstBucket {
						_ = w.WriteByte(',')
					}
					_, _ = w.Write(strconv.AppendInt(rs.stash[:0], int64(o)+int64(idx), 10))
					firstBucket = false
				}

				_, _ = w.WriteString(json16)

				firstBucket = true
				for idx := range nbcLen {
					count := nbc.At(idx)
					if count == 0 {
						continue
					}
					if !firstBucket {
						_ = w.WriteByte(',')
					}
					_, _ = w.Write(strconv.AppendInt(rs.stash[:0], safeUint64ToInt64(count), 10))
					firstBucket = false
				}

				_, _ = w.WriteString(json17)
			}

			_ = w.WriteByte('}')
			isFirstMetric = false
		}

		_ = w.WriteByte('}')

		slices.Sort(rs.sortedMetricSlice)
		d.Reset()
		for i := range rs.sortedMetricSlice {
			_, _ = d.WriteString(rs.sortedMetricSlice[i])
		}

		_, _ = w.WriteString(json13)
		_, _ = w.Write(strconv.AppendUint(rs.stash[:0], d.Sum64(), 16))
		_ = w.WriteByte('"')
		_, _ = w.WriteString(json14)
	}

	if batchDSM != nil {
		if batchNumEmpty > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, batchNumEmpty, batchDSM.dpNumEmpty...)
		}
		if batchNumDup > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, batchNumDup, batchDSM.dpNumDup...)
		}
		if batchNum > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, batchNum, batchDSM.dpNum...)
		}
		if batchHisDup > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, batchHisDup, batchDSM.dpHisDup...)
		}
		if batchHis > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, batchHis, batchDSM.dpHis...)
		}
		if batchExpHisDup > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, batchExpHisDup, batchDSM.dpExpHisDup...)
		}
		if batchExpHis > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, batchExpHis, batchDSM.dpExpHis...)
		}
		if batchDoc > 0 {
			exp.tb.ElasticsearchDocsReceived.Add(ctx, batchDoc, batchDSM.docsReceived...)
		}
	}

	if err := w.Flush(); err != nil {
		return nil, err
	}
	if err := z.Close(); err != nil {
		return nil, err
	}

	return &ElasticsearchBulkRequests{
		buf: bytes.Clone(b.Bytes()),
		cnt: docCount,
	}, nil
}

func (exp *ElasticsearchExporter) requestConsume(ctx context.Context, wreq xexporterhelper.Request) error {
	now := time.Now()

	ebr, ok := wreq.(*ElasticsearchBulkRequests)
	if !ok {
		return errors.ErrUnsupported
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, exp.sel.Select(exp.url), bytes.NewReader(ebr.buf))
	if err != nil {
		exp.set.Logger.Error("http.NewRequestWithContext", zap.Error(err))
		return consumererror.NewPermanent(err)
	}

	resp, err := exp.client.Do(req)
	if err != nil {
		exp.set.Logger.Error("http.Do", zap.Error(err))
		exp.tb.ElasticsearchBulkRequestsLatency.Record(
			ctx,
			time.Since(now).Seconds(),
			metric.WithAttributeSet(attribute.NewSet(exp.attr, semconv.ErrorType(err))),
		)
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		b := bufPool.Get().(*bytes.Buffer)
		b.Reset()
		if _, readErr := b.ReadFrom(io.LimitReader(resp.Body, maxResponseBytes)); readErr != nil {
			exp.set.Logger.Error("http.ReadBody", zap.Int("status", http.StatusOK), zap.Error(readErr))
		}

		var processed int64
		if b.Len() > 0 {
			body := bulkItemsPool.Get().(*BulkItems)
			body.Items = body.Items[:0]

			if e := sonic.ConfigFastest.Unmarshal(b.Bytes(), body); e != nil {
				exp.set.Logger.Error("json.Decode", zap.Int("status", http.StatusOK), zap.Error(e))
			}

			for i := range body.Items {
				item := &body.Items[i]
				if item.Create.Status == http.StatusCreated {
					processed++
					continue
				}

				errType := strings.Clone(item.Create.Error.Type)
				exp.tb.ElasticsearchDocsProcessed.Add(
					ctx,
					one,
					metric.WithAttributeSet(attribute.NewSet(
						exp.attr,
						semconv.HTTPResponseStatusCodeKey.Int(item.Create.Status),
						semconv.ErrorTypeKey.String(errType),
					)),
				)

				if item.Create.Error.Type == "version_conflict_engine_exception" {
					continue
				}

				if err == nil && (item.Create.Status == http.StatusUnauthorized || item.Create.Status == http.StatusTooManyRequests) {
					err = errors.New(resp.Status)
				}

				exp.set.Logger.Error(
					"bulk.DocumentError",
					zap.Int("status", item.Create.Status),
					zap.String("type", item.Create.Error.Type),
					zap.String("reason", item.Create.Error.Reason),
				)
			}

			clear(body.Items)
			body.Items = body.Items[:0]
			if cap(body.Items) <= maxPooledBulkItems {
				bulkItemsPool.Put(body)
			}
		}

		if b.Cap() <= maxPooledBufCap {
			bufPool.Put(b)
		}

		if processed > 0 {
			exp.tb.ElasticsearchDocsProcessed.Add(ctx, processed, exp.optDocsCreated...)
		}

		if err == nil {
			exp.tb.ElasticsearchBulkRequestsLatency.Record(ctx, time.Since(now).Seconds(), exp.optLatencyOK...)
			ebr.releaseMergeBuf()
		} else {
			exp.tb.ElasticsearchBulkRequestsLatency.Record(ctx, time.Since(now).Seconds(), exp.optLatencyRetry...)
		}

		return err

	case http.StatusBadRequest:
		b := bufPool.Get().(*bytes.Buffer)
		b.Reset()
		_, _ = b.ReadFrom(io.LimitReader(resp.Body, maxResponseBytes))

		var body BulkError
		if b.Len() > 0 {
			if e := sonic.ConfigFastest.Unmarshal(b.Bytes(), &body); e != nil {
				exp.set.Logger.Error("json.Decode", zap.Int("status", resp.StatusCode), zap.Error(e))
			}
		}

		errType := strings.Clone(body.Error.Type)
		exp.set.Logger.Error("bulk.Error", zap.Int("status", body.Status), zap.String("type", body.Error.Type), zap.String("reason", body.Error.Reason))
		if b.Cap() <= maxPooledBufCap {
			bufPool.Put(b)
		}

		exp.tb.ElasticsearchBulkRequestsLatency.Record(
			ctx,
			time.Since(now).Seconds(),
			metric.WithAttributeSet(attribute.NewSet(exp.attr, attrBadRequest, semconv.ErrorTypeKey.String(errType))),
		)

		return consumererror.NewPermanent(errors.New(resp.Status))

	case http.StatusRequestEntityTooLarge:
		err = consumererror.NewPermanent(errors.New(resp.Status))
	default:
		err = errors.New(resp.Status)
	}

	exp.set.Logger.Error("bulk.Error", zap.Int("status", resp.StatusCode))
	exp.tb.ElasticsearchBulkRequestsLatency.Record(ctx, time.Since(now).Seconds(), exp.getStatusLatencyOpt(resp.StatusCode)...)

	return err
}
