// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/cespare/xxhash/v2"
	"github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter/internal/metadata"
	"github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/oneconfextension"
	"gitlab.rip/golang/platform-tech-services/one_configuration"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/xpdata/xhash"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.uber.org/zap"
)

type ElasticsearchExporter struct {
	config *Config
	set    exporter.Settings
	url    string
	tb     *metadata.TelemetryBuilder
	sel    *Selector
	host   component.Host
	client *http.Client
	attr   attribute.KeyValue
}

func NewExporter(config *Config, set exporter.Settings) (*ElasticsearchExporter, error) {
	var errs []error
	var urls []string

	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	for endpoint := range strings.SplitSeq(config.Endpoint, ",") {
		if endpoint == "" {
			continue
		}
		u, e := url.Parse(strings.TrimRight(endpoint, "/"))
		if e != nil {
			errs = append(errs, e)
			continue
		}
		urls = append(urls, u.String())
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	if len(urls) == 0 {
		return nil, errors.New("at least 1 endpoint must be specified")
	}

	sel := &Selector{index: atomic.Uint64{}, state: atomic.Pointer[SelectorState]{}}
	sel.SetupRR(urls)

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

	return &ElasticsearchExporter{config: config, set: set, url: u.RequestURI(), tb: tb, sel: sel}, nil
}

func (exp *ElasticsearchExporter) Start(ctx context.Context, host component.Host) error {
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
	exp.attr = attribute.String("exporter", exp.set.ID.String())

	return err
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
	return nil
}

func (exp *ElasticsearchExporter) requestConverter(ctx context.Context, md pmetric.Metrics) (xexporterhelper.Request, error) {
	var ndps pmetric.NumberDataPointSlice
	var hdps pmetric.HistogramDataPointSlice
	var ehdps pmetric.ExponentialHistogramDataPointSlice
	var isFirst bool

	d := digestPool.Get().(*xxhash.Digest)
	b := bufPool.Get().(*bytes.Buffer)
	z := gzipPool.Get().(*gzip.Writer)
	w := bufioPool.Get().(*bufio.Writer)
	rs := requestStatePool.Get().(*RequestState)

	b.Reset()
	z.Reset(b)
	w.Reset(z)

	defer func() {
		for _, dpg := range rs.dph2dpg {
			dpg.ndp2mn = dpg.ndp2mn[:0]
			dpg.hdp2mn = dpg.hdp2mn[:0]
			dpg.ehdp2mn = dpg.ehdp2mn[:0]
			dpg.ndps = dpg.ndps[:0]
			dpg.hdps = dpg.hdps[:0]
			dpg.ehdps = dpg.ehdps[:0]
			dpgPool.Put(dpg)
		}

		clear(rs.dph2dpg)

		requestStatePool.Put(rs)
		bufPool.Put(b)
		gzipPool.Put(z)
		bufioPool.Put(w)
		digestPool.Put(d)
	}()

	defaultDS := DataStream{dataset: "otelcol.v1", namespace: "garbage", key: "otelcol.v1|garbage"}
	rms := md.ResourceMetrics()
	for i := range rms.Len() {
		rm := rms.At(i)
		resource := rm.Resource()
		resourceMap := resource.Attributes()
		resourceDS := defaultDS
		var resourceHash uint64
		if resourceMap.Len() > 0 {
			resourceHash = xhash.Hash64(xhash.WithMap(resourceMap))
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
			var scopeHash uint64
			var opts [3]xhash.HashOption
			var optsLen int
			if scopeName != "" {
				opts[optsLen] = xhash.WithString(scopeName)
				optsLen++
			}
			if scopeVersion != "" {
				opts[optsLen] = xhash.WithString(scopeVersion)
				optsLen++
			}
			if scopeMap.Len() > 0 {
				opts[optsLen] = xhash.WithMap(scopeMap)
				optsLen++
				scopeDS = scopeDS.Update(scopeMap)
			}
			if optsLen > 0 {
				scopeHash = xhash.Hash64(opts[:optsLen]...)
			}

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
						exp.set.Logger.Info("Metric.Temporality", zap.String("metric", mn), zap.String("type", mt.String()), zap.String("attributes", stringifyMaps(resourceMap, scopeMap)))
						continue
					}
					ndps = tmp.DataPoints()
				case pmetric.MetricTypeHistogram:
					tmp := m.Histogram()
					if tmp.AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						exp.set.Logger.Info("Metric.Temporality", zap.String("metric", mn), zap.String("type", mt.String()), zap.String("attributes", stringifyMaps(resourceMap, scopeMap)))
						continue
					}
					hdps = tmp.DataPoints()
				case pmetric.MetricTypeExponentialHistogram:
					tmp := m.ExponentialHistogram()
					if tmp.AggregationTemporality() != pmetric.AggregationTemporalityDelta {
						exp.set.Logger.Info("Metric.Temporality", zap.String("metric", mn), zap.String("type", mt.String()), zap.String("attributes", stringifyMaps(resourceMap, scopeMap)))
						continue
					}
					ehdps = tmp.DataPoints()
				}

				switch mt {
				case pmetric.MetricTypeGauge, pmetric.MetricTypeSum:
					for l := range ndps.Len() {
						ndp := ndps.At(l)
						dpMap := ndp.Attributes()
						var dpHash uint64
						if dpMap.Len() > 0 {
							dpHash = xhash.Hash64(xhash.WithMap(dpMap))
						}
						dph := DataPointHash{resourceHash: resourceHash, scopeHash: scopeHash, dpHash: dpHash, ts: uint64(ndp.Timestamp()) / 1000000}
						dpg, ok := rs.dph2dpg[dph]
						if !ok {
							dataStream := scopeDS
							if dpMap.Len() > 0 {
								dataStream = dataStream.Update(dpMap)
							}
							dpg = dpgPool.Get().(*DataPointGroup)
							dpg.dataStream = dataStream
							dpg.resourceMap = resourceMap
							dpg.scopeMap = scopeMap
							dpg.scopeName = scopeName
							dpg.scopeVersion = scopeVersion
							dpg.dpMap = dpMap
							rs.dph2dpg[dph] = dpg
						}
						dpg.ndp2mn = append(dpg.ndp2mn, mn)
						dpg.ndps = append(dpg.ndps, ndp)
					}
				case pmetric.MetricTypeHistogram:
					for l := range hdps.Len() {
						hdp := hdps.At(l)
						dpMap := hdp.Attributes()
						var dpHash uint64
						if dpMap.Len() > 0 {
							dpHash = xhash.Hash64(xhash.WithMap(dpMap))
						}
						dph := DataPointHash{resourceHash: resourceHash, scopeHash: scopeHash, dpHash: dpHash, ts: uint64(hdp.Timestamp()) / 1000000}
						dpg, ok := rs.dph2dpg[dph]
						if !ok {
							dataStream := scopeDS
							if dpMap.Len() > 0 {
								dataStream = dataStream.Update(dpMap)
							}
							dpg = dpgPool.Get().(*DataPointGroup)
							dpg.dataStream = dataStream
							dpg.resourceMap = resourceMap
							dpg.scopeMap = scopeMap
							dpg.scopeName = scopeName
							dpg.scopeVersion = scopeVersion
							dpg.dpMap = dpMap
							rs.dph2dpg[dph] = dpg
						}
						dpg.hdp2mn = append(dpg.hdp2mn, mn)
						dpg.hdps = append(dpg.hdps, hdp)
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := range ehdps.Len() {
						ehdp := ehdps.At(l)
						dpMap := ehdp.Attributes()
						var dpHash uint64
						if dpMap.Len() > 0 {
							dpHash = xhash.Hash64(xhash.WithMap(dpMap))
						}
						dph := DataPointHash{resourceHash: resourceHash, scopeHash: scopeHash, dpHash: dpHash, ts: uint64(ehdp.Timestamp()) / 1000000}
						dpg, ok := rs.dph2dpg[dph]
						if !ok {
							dataStream := scopeDS
							if dpMap.Len() > 0 {
								dataStream = dataStream.Update(dpMap)
							}
							dpg = dpgPool.Get().(*DataPointGroup)
							dpg.dataStream = dataStream
							dpg.resourceMap = resourceMap
							dpg.scopeMap = scopeMap
							dpg.scopeName = scopeName
							dpg.scopeVersion = scopeVersion
							dpg.dpMap = dpMap
							rs.dph2dpg[dph] = dpg
						}
						dpg.ehdp2mn = append(dpg.ehdp2mn, mn)
						dpg.ehdps = append(dpg.ehdps, ehdp)
					}
				}
			}
		}
	}

	if len(rs.dph2dpg) == 0 {
		return nil, errors.New("empty request")
	}

	for dph, dpg := range rs.dph2dpg {
		attrDS := attribute.String("dataset", dpg.dataStream.dataset)
		attrNS := attribute.String("namespace", dpg.dataStream.namespace)

		_, _ = w.Write(json1)
		_, _ = w.Write(stringToBytes(dpg.dataStream.dataset))
		_, _ = w.Write(hyphen)
		_, _ = w.Write(stringToBytes(dpg.dataStream.namespace))
		_, _ = w.Write(json2)

		var empty int64
		var dup int64
		var suc int64
		isFirst = true
		clear(rs.seenMap)
		for i, ndp := range dpg.ndps {
			metricName := dpg.ndp2mn[i]
			valueType := ndp.ValueType()

			if valueType == pmetric.NumberDataPointValueTypeEmpty {
				empty++
				exp.set.Logger.Info("DataPoint.Empty", zap.String("metric", metricName), zap.String("attributes", stringifyMaps(dpg.resourceMap, dpg.scopeMap, dpg.dpMap)))
				dpg.ndp2mn[i] = ""
				continue
			}

			if _, ok := rs.seenMap[metricName]; ok {
				dup++
				exp.set.Logger.Info("DataPoint.Duplicate", zap.String("metric", metricName), zap.String("attributes", stringifyMaps(dpg.resourceMap, dpg.scopeMap, dpg.dpMap)))
				dpg.ndp2mn[i] = ""
				continue
			}

			rs.seenMap[metricName] = struct{}{}

			if !isFirst {
				_, _ = w.Write(comma)
			}
			_, _ = w.Write(json3)
			_, _ = w.Write(stringToBytes(metricName))
			_, _ = w.Write(qcq)

			switch valueType {
			case pmetric.NumberDataPointValueTypeInt:
				_, _ = w.Write(gldt)
			case pmetric.NumberDataPointValueTypeDouble:
				_, _ = w.Write(gddt)
			}

			_, _ = w.Write(quote)

			isFirst = false
		}
		suc = int64(len(dpg.ndps)) - empty - dup
		if empty > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, empty, metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrNum, attrEmpty)))
		}
		if dup > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, dup, metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrNum, attrDup)))
			dup = 0
		}
		if suc > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, suc, metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrNum)))
		}
		for i := range dpg.hdps {
			metricName := dpg.hdp2mn[i]

			if _, ok := rs.seenMap[metricName]; ok {
				dup++
				exp.set.Logger.Info("DataPoint.Duplicate", zap.String("metric", metricName), zap.String("attributes", stringifyMaps(dpg.resourceMap, dpg.scopeMap, dpg.dpMap)))
				dpg.hdp2mn[i] = ""
				continue
			}

			rs.seenMap[metricName] = struct{}{}

			if !isFirst {
				_, _ = w.Write(comma)
			}
			_, _ = w.Write(json3)
			_, _ = w.Write(stringToBytes(metricName))
			_, _ = w.Write(qcq)
			_, _ = w.Write(hdt)
			_, _ = w.Write(quote)

			isFirst = false
		}
		suc = int64(len(dpg.hdps)) - dup
		if dup > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, dup, metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrHis, attrDup)))
			dup = 0
		}
		if suc > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, suc, metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrHis)))
		}
		for i := range dpg.ehdps {
			metricName := dpg.ehdp2mn[i]

			if _, ok := rs.seenMap[metricName]; ok {
				dup++
				exp.set.Logger.Info("DataPoint.Duplicate", zap.String("metric", metricName), zap.String("attributes", stringifyMaps(dpg.resourceMap, dpg.scopeMap, dpg.dpMap)))
				dpg.ehdp2mn[i] = ""
				continue
			}

			rs.seenMap[metricName] = struct{}{}

			if !isFirst {
				_, _ = w.Write(comma)
			}
			_, _ = w.Write(json3)
			_, _ = w.Write(stringToBytes(metricName))
			_, _ = w.Write(qcq)
			_, _ = w.Write(ehdt)
			_, _ = w.Write(quote)

			isFirst = false
		}
		suc = int64(len(dpg.ehdps)) - dup
		if dup > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, dup, metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrExpHis, attrDup)))
		}
		if suc > 0 {
			exp.tb.ElasticsearchDatapointsReceived.Add(ctx, suc, metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS, attrExpHis)))
		}

		_, _ = w.Write(json4)
		_, _ = w.Write(strconv.AppendUint(rs.stash[:0], dph.ts, 10))
		_, _ = w.Write(json5)
		_, _ = w.Write(stringToBytes(dpg.dataStream.dataset))
		_, _ = w.Write(json6)
		_, _ = w.Write(stringToBytes(dpg.dataStream.namespace))
		_, _ = w.Write(json7)
		rs.WriteMap(w, dpg.dpMap)
		_, _ = w.Write(json8)
		rs.WriteMap(w, dpg.resourceMap)
		_, _ = w.Write(json9)
		rs.WriteMap(w, dpg.scopeMap)
		_, _ = w.Write(json10)
		if dpg.scopeName != "" {
			_, _ = w.Write(stringToBytes(dpg.scopeName))
		}
		_, _ = w.Write(json11)
		if dpg.scopeVersion != "" {
			_, _ = w.Write(stringToBytes(dpg.scopeVersion))
		}
		_, _ = w.Write(json12)

		isFirst = true
		rs.sortedMetricSlice = rs.sortedMetricSlice[:0]
		for i, ndp := range dpg.ndps {
			metricName := dpg.ndp2mn[i]

			if metricName == "" {
				continue
			}

			rs.sortedMetricSlice = append(rs.sortedMetricSlice, metricName)

			if !isFirst {
				_, _ = w.Write(comma)
			}

			_, _ = w.Write(quote)
			_, _ = w.Write(stringToBytes(metricName))
			_, _ = w.Write(qc)

			switch ndp.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				_, _ = w.Write(strconv.AppendInt(rs.stash[:0], ndp.IntValue(), 10))
			case pmetric.NumberDataPointValueTypeDouble:
				rs.WriteFloat(w, ndp.DoubleValue())
			}

			isFirst = false
		}
		for i, hdp := range dpg.hdps {
			metricName := dpg.hdp2mn[i]

			if metricName == "" {
				continue
			}

			rs.sortedMetricSlice = append(rs.sortedMetricSlice, metricName)

			if !isFirst {
				_, _ = w.Write(comma)
			}

			_, _ = w.Write(quote)
			_, _ = w.Write(stringToBytes(metricName))
			_, _ = w.Write(qc)

			eb := hdp.ExplicitBounds()
			ebLen := eb.Len()

			bc := hdp.BucketCounts()
			bcLen := bc.Len()

			minLen := min(ebLen, bcLen)

			_, _ = w.Write(json15)

			isFirst = true
			for i := range minLen {
				if bc.At(i) == 0 {
					continue
				}
				if !isFirst {
					_, _ = w.Write(comma)
				}
				rs.WriteFloat(w, eb.At(i))

				isFirst = false
			}

			_, _ = w.Write(json16)

			isFirst = true
			for i := range minLen {
				count := bc.At(i)
				if count == 0 {
					continue
				}
				if !isFirst {
					_, _ = w.Write(comma)
				}
				_, _ = w.Write(strconv.AppendInt(rs.stash[:0], safeUint64ToInt64(count), 10))

				isFirst = false
			}

			_, _ = w.Write(json17)

			isFirst = false
		}
		for i, ehdp := range dpg.ehdps {
			metricName := dpg.ehdp2mn[i]

			if metricName == "" {
				continue
			}

			rs.sortedMetricSlice = append(rs.sortedMetricSlice, metricName)

			if !isFirst {
				_, _ = w.Write(comma)
			}

			_, _ = w.Write(quote)
			_, _ = w.Write(stringToBytes(metricName))
			_, _ = w.Write(qc)
			_, _ = w.Write(json18)
			_, _ = w.Write(strconv.AppendInt(rs.stash[:0], int64(ehdp.Scale()), 10))

			if ehdp.HasSum() && !math.IsInf(ehdp.Sum(), 0) && !math.IsNaN(ehdp.Sum()) {
				_, _ = w.Write(json19)
				rs.WriteFloat(w, ehdp.Sum())
			}
			if ehdp.HasMin() && !math.IsInf(ehdp.Min(), 0) && !math.IsNaN(ehdp.Min()) {
				_, _ = w.Write(json20)
				rs.WriteFloat(w, ehdp.Min())
			}
			if ehdp.HasMax() && !math.IsInf(ehdp.Max(), 0) && !math.IsNaN(ehdp.Max()) {
				_, _ = w.Write(json21)
				rs.WriteFloat(w, ehdp.Max())
			}

			_, _ = w.Write(json22)
			rs.WriteFloat(w, ehdp.ZeroThreshold())
			_, _ = w.Write(json23)
			_, _ = w.Write(strconv.AppendInt(rs.stash[:0], int64(ehdp.ZeroCount()), 10))
			_, _ = w.Write(rcBrace)

			p := ehdp.Positive()
			pbc := p.BucketCounts()
			pbcLen := pbc.Len()
			if pbcLen > 0 {
				o := p.Offset()

				_, _ = w.Write(json24)

				isFirst = true
				for i := range pbcLen {
					if pbc.At(i) == 0 {
						continue
					}
					if !isFirst {
						_, _ = w.Write(comma)
					}
					_, _ = w.Write(strconv.AppendInt(rs.stash[:0], int64(o)+int64(i), 10))

					isFirst = false
				}

				_, _ = w.Write(json16)

				isFirst = true
				for i := range pbcLen {
					count := pbc.At(i)
					if count == 0 {
						continue
					}
					if !isFirst {
						_, _ = w.Write(comma)
					}
					_, _ = w.Write(strconv.AppendInt(rs.stash[:0], safeUint64ToInt64(count), 10))

					isFirst = false
				}

				_, _ = w.Write(json17)
			}

			n := ehdp.Negative()
			nbc := n.BucketCounts()
			nbcLen := nbc.Len()
			if nbcLen > 0 {
				o := n.Offset()

				_, _ = w.Write(json25)

				isFirst = true
				for i := range nbcLen {
					if nbc.At(i) == 0 {
						continue
					}
					if !isFirst {
						_, _ = w.Write(comma)
					}
					_, _ = w.Write(strconv.AppendInt(rs.stash[:0], int64(o)+int64(i), 10))

					isFirst = false
				}

				_, _ = w.Write(json16)

				isFirst = true
				for i := range nbcLen {
					count := nbc.At(i)
					if count == 0 {
						continue
					}
					if !isFirst {
						_, _ = w.Write(comma)
					}
					_, _ = w.Write(strconv.AppendInt(rs.stash[:0], safeUint64ToInt64(count), 10))

					isFirst = false
				}

				_, _ = w.Write(json17)
			}

			_, _ = w.Write(rcBrace)

			isFirst = false
		}

		_, _ = w.Write(rcBrace)

		slices.Sort(rs.sortedMetricSlice)
		for i := range rs.sortedMetricSlice {
			_, _ = d.WriteString(rs.sortedMetricSlice[i])
		}

		_, _ = w.Write(json13)
		_, _ = w.Write(strconv.AppendUint(rs.stash[:0], d.Sum64(), 16))
		_, _ = w.Write(quote)

		d.Reset()

		_, _ = w.Write(json14)

		exp.tb.ElasticsearchDocsReceived.Add(ctx, one, metric.WithAttributeSet(attribute.NewSet(exp.attr, attrDS, attrNS)))
	}

	if err := w.Flush(); err != nil {
		return nil, err
	}

	if err := z.Close(); err != nil {
		return nil, err
	}

	return &ElasticsearchBulkRequests{buf: bytes.Clone(b.Bytes())}, nil
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
		exp.tb.ElasticsearchBulkRequestsLatency.Record(ctx, time.Since(now).Seconds(), metric.WithAttributeSet(attribute.NewSet(exp.attr, semconv.ErrorType(err))))
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var body BulkItems
		if e := sonic.ConfigFastest.NewDecoder(resp.Body).Decode(&body); e != nil {
			exp.set.Logger.Error("json.Decode", zap.Int("status", http.StatusOK), zap.Error(e))
		}

		var processed int64
		for i := range body.Items {
			item := &body.Items[i]
			if item.Create.Status == http.StatusCreated {
				processed++
				continue
			}

			exp.tb.ElasticsearchDocsProcessed.Add(ctx, one, metric.WithAttributeSet(attribute.NewSet(exp.attr, semconv.HTTPResponseStatusCodeKey.Int(item.Create.Status), semconv.ErrorTypeKey.String(item.Create.Error.Type))))

			if item.Create.Error.Type == "version_conflict_engine_exception" {
				continue
			}

			if err == nil && (item.Create.Status == http.StatusUnauthorized || item.Create.Status == http.StatusTooManyRequests) {
				err = errors.New(resp.Status)
			}

			exp.set.Logger.Error("bulk.DocumentError", zap.Int("status", item.Create.Status), zap.String("type", item.Create.Error.Type), zap.String("reason", item.Create.Error.Reason))
		}

		if processed > 0 {
			exp.tb.ElasticsearchDocsProcessed.Add(ctx, processed, metric.WithAttributeSet(attribute.NewSet(exp.attr, attrCreated)))
		}

		if err == nil {
			exp.tb.ElasticsearchBulkRequestsLatency.Record(ctx, time.Since(now).Seconds(), metric.WithAttributeSet(attribute.NewSet(exp.attr, attrOK)))
		} else {
			exp.tb.ElasticsearchBulkRequestsLatency.Record(ctx, time.Since(now).Seconds(), metric.WithAttributeSet(attribute.NewSet(exp.attr, attrOK, attrRetry)))
		}

		return err
	case http.StatusBadRequest:
		var body BulkError
		if e := sonic.ConfigFastest.NewDecoder(resp.Body).Decode(&body); e != nil {
			exp.set.Logger.Error("json.Decode", zap.Int("status", resp.StatusCode), zap.Error(e))
		}

		exp.set.Logger.Error("bulk.Error", zap.Int("status", body.Status), zap.String("type", body.Error.Type), zap.String("reason", body.Error.Reason))
		exp.tb.ElasticsearchBulkRequestsLatency.Record(ctx, time.Since(now).Seconds(), metric.WithAttributeSet(attribute.NewSet(exp.attr, attrBadRequest, semconv.ErrorTypeKey.String(body.Error.Type))))

		return consumererror.NewPermanent(errors.New(resp.Status))
	case http.StatusRequestEntityTooLarge:
		err = consumererror.NewPermanent(errors.New(resp.Status))
	default:
		err = errors.New(resp.Status)
	}

	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	exp.set.Logger.Error("bulk.Error", zap.Int("status", resp.StatusCode))
	exp.tb.ElasticsearchBulkRequestsLatency.Record(ctx, time.Since(now).Seconds(), metric.WithAttributeSet(attribute.NewSet(exp.attr, semconv.HTTPResponseStatusCodeKey.Int(resp.StatusCode))))

	return err
}
