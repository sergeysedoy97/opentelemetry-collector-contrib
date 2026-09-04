// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"bytes"
	"context"
	"errors"
	"io"
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
	"github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter/internal/metadata"
	"github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/oneconfextension"
	"gitlab.rip/golang/platform-tech-services/one_configuration"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
)

const (
	oneConfProperty = "internal.elasticsearchexporter"
	one             = int64(1)
)

type ElasticsearchExporter struct {
	config *Config
	set    exporter.Settings
	url    string
	tb     *metadata.TelemetryBuilder
	buf    *sync.Pool
	cs     *CustomSelector
	host   component.Host
	client *http.Client
	attrs  []attribute.KeyValue
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

	cs := &CustomSelector{index: atomic.Uint64{}, state: atomic.Pointer[CustomSelectorState]{}}
	cs.SetupRR(urls)

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

	return &ElasticsearchExporter{
		config: config,
		set:    set,
		url:    u.RequestURI(),
		tb:     tb,
		buf:    &sync.Pool{New: func() any { return &bytes.Buffer{} }},
		cs:     cs,
	}, nil
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
				e1 := exp.cs.SetupConf(v)
				if e1 == nil {
					exp.cs.unsubscribe = conf.SubscribeString(oneConfProperty, func(_ one_configuration.Key, _, cur *string) {
						if cur == nil {
							return
						}
						e2 := exp.cs.SetupConf(*cur)
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
	exp.attrs = append(exp.attrs, attribute.String("exporter", exp.set.ID.String()))

	return err
}

func (exp *ElasticsearchExporter) Shutdown(context.Context) error {
	if exp.tb != nil {
		exp.tb.Shutdown()
		exp.tb = nil
	}
	if exp.cs != nil {
		exp.cs.Unsubscribe()
		exp.cs = nil
	}
	return nil
}

func (exp *ElasticsearchExporter) pusher(ctx context.Context, metrics pmetric.Metrics) error {
	var digest xxhash.Digest
	var attrs []attribute.KeyValue
	var event *componentstatus.Event

	now := time.Now()

	dpgk2dpg := make(map[DataPointGroupKey]*DataPointGroup)
	dataStreams := make(map[DataStream]struct{})

	buf := exp.buf.Get().(*bytes.Buffer)
	w := &JSONWriter{buf: buf}

	rms := metrics.ResourceMetrics()
	for i := range rms.Len() {
		rm := rms.At(i)
		resource := rm.Resource()
		resourceMap := resource.Attributes()
		resourceHash := pdatautil.Hash64(pdatautil.WithMap(resourceMap))

		resourceDS := DataStream{dataset: "otelcol.v1", namespace: "garbage"}
		if v, ok := resourceMap.Get(DataStreamDataset); ok && v.Type() == pcommon.ValueTypeStr {
			if s := v.Str(); s != "" {
				resourceDS.dataset = s
			}
		}
		if v, ok := resourceMap.Get(DataStreamNamespace); ok && v.Type() == pcommon.ValueTypeStr {
			if s := v.Str(); s != "" {
				resourceDS.namespace = s
			}
		}

		sms := rm.ScopeMetrics()
		for j := range sms.Len() {
			sm := sms.At(j)
			scope := sm.Scope()
			scopeMap := scope.Attributes()
			scopeName := scope.Name()
			scopeVersion := scope.Version()
			scopeHash := pdatautil.Hash64(pdatautil.WithMap(scopeMap), pdatautil.WithString(scopeName), pdatautil.WithString(scopeVersion))

			scopeDS := resourceDS
			if v, ok := scopeMap.Get(DataStreamDataset); ok && v.Type() == pcommon.ValueTypeStr {
				if s := v.Str(); s != "" {
					scopeDS.dataset = s
				}
			}
			if v, ok := scopeMap.Get(DataStreamNamespace); ok && v.Type() == pcommon.ValueTypeStr {
				if s := v.Str(); s != "" {
					scopeDS.namespace = s
				}
			}

			ms := sm.Metrics()
			for k := range ms.Len() {
				var ndps pmetric.NumberDataPointSlice
				var hdps pmetric.HistogramDataPointSlice
				var ehdps pmetric.ExponentialHistogramDataPointSlice

				m := ms.At(k)
				mt := m.Type()

				switch mt {
				case pmetric.MetricTypeGauge:
					ndps = m.Gauge().DataPoints()
				case pmetric.MetricTypeSum:
					ndps = m.Sum().DataPoints()
				case pmetric.MetricTypeHistogram:
					if m.Histogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative {
						continue
					}
					hdps = m.Histogram().DataPoints()
				case pmetric.MetricTypeExponentialHistogram:
					if m.ExponentialHistogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative {
						continue
					}
					ehdps = m.ExponentialHistogram().DataPoints()
				}

				switch mt {
				case pmetric.MetricTypeGauge, pmetric.MetricTypeSum:
					if ndps.Len() == 0 {
						continue
					}
					var dt string
					switch mt {
					case pmetric.MetricTypeSum:
						sum := m.Sum()
						isCounter := sum.IsMonotonic() && sum.AggregationTemporality() == pmetric.AggregationTemporalityCumulative
						switch ndps.At(0).ValueType() {
						case pmetric.NumberDataPointValueTypeDouble:
							if isCounter {
								dt = "counter_double"
							} else {
								dt = "gauge_double"
							}
						case pmetric.NumberDataPointValueTypeInt:
							if isCounter {
								dt = "counter_long"
							} else {
								dt = "gauge_long"
							}
						}
					case pmetric.MetricTypeGauge:
						switch ndps.At(0).ValueType() {
						case pmetric.NumberDataPointValueTypeDouble:
							dt = "gauge_double"
						case pmetric.NumberDataPointValueTypeInt:
							dt = "gauge_long"
						}
					}
					for l := range ndps.Len() {
						// hot-path, copy-paste
						ndp := ndps.At(l)
						if ndp.Flags().NoRecordedValue() {
							continue
						}

						dpMap := ndp.Attributes()
						ds := scopeDS
						if v, ok := dpMap.Get(DataStreamDataset); ok && v.Type() == pcommon.ValueTypeStr {
							if s := v.Str(); s != "" {
								ds.dataset = s
							}
						}
						if v, ok := dpMap.Get(DataStreamNamespace); ok && v.Type() == pcommon.ValueTypeStr {
							if s := v.Str(); s != "" {
								ds.namespace = s
							}
						}
						if _, ok := dataStreams[ds]; !ok {
							dataStreams[ds] = struct{}{}
						}
						dpgk := DataPointGroupKey{ds: ds, dpHash: DataPointHash{resourceHash: resourceHash, scopeHash: scopeHash, dpHash: pdatautil.Hash64(pdatautil.WithMap(dpMap)), ts: uint64(ndp.Timestamp())}}
						dp := &Number{NumberDataPoint: ndp, metric: m, dynamicTemplate: dt}

						dpg, ok := dpgk2dpg[dpgk]
						if ok {
							dpg.dps = append(dpg.dps, dp)
						} else {
							dpgk2dpg[dpgk] = &DataPointGroup{
								resourceMap:  &resourceMap,
								scopeMap:     &scopeMap,
								scopeName:    scopeName,
								scopeVersion: scopeVersion,
								dpMap:        &dpMap,
								dps:          []DataPoint{dp},
							}
						}
					}
				case pmetric.MetricTypeHistogram:
					for l := range hdps.Len() {
						// hot-path, copy-paste
						hdp := hdps.At(l)
						if hdp.Flags().NoRecordedValue() {
							continue
						}

						dpMap := hdp.Attributes()
						ds := scopeDS
						if v, ok := dpMap.Get(DataStreamDataset); ok && v.Type() == pcommon.ValueTypeStr {
							if s := v.Str(); s != "" {
								ds.dataset = s
							}
						}
						if v, ok := dpMap.Get(DataStreamNamespace); ok && v.Type() == pcommon.ValueTypeStr {
							if s := v.Str(); s != "" {
								ds.namespace = s
							}
						}
						if _, ok := dataStreams[ds]; !ok {
							dataStreams[ds] = struct{}{}
						}
						dpgk := DataPointGroupKey{ds: ds, dpHash: DataPointHash{resourceHash: resourceHash, scopeHash: scopeHash, dpHash: pdatautil.Hash64(pdatautil.WithMap(dpMap)), ts: uint64(hdp.Timestamp())}}
						dp := &Histogram{HistogramDataPoint: hdp, metric: m, dynamicTemplate: "histogram"}

						dpg, ok := dpgk2dpg[dpgk]
						if ok {
							dpg.dps = append(dpg.dps, dp)
						} else {
							dpgk2dpg[dpgk] = &DataPointGroup{
								resourceMap:  &resourceMap,
								scopeMap:     &scopeMap,
								scopeName:    scopeName,
								scopeVersion: scopeVersion,
								dpMap:        &dpMap,
								dps:          []DataPoint{dp},
							}
						}
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := range ehdps.Len() {
						// hot-path, copy-paste
						ehdp := ehdps.At(l)
						if ehdp.Flags().NoRecordedValue() {
							continue
						}

						dpMap := ehdp.Attributes()
						ds := scopeDS
						if v, ok := dpMap.Get(DataStreamDataset); ok && v.Type() == pcommon.ValueTypeStr {
							if s := v.Str(); s != "" {
								ds.dataset = s
							}
						}
						if v, ok := dpMap.Get(DataStreamNamespace); ok && v.Type() == pcommon.ValueTypeStr {
							if s := v.Str(); s != "" {
								ds.namespace = s
							}
						}
						if _, ok := dataStreams[ds]; !ok {
							dataStreams[ds] = struct{}{}
						}
						dpgk := DataPointGroupKey{ds: ds, dpHash: DataPointHash{resourceHash: resourceHash, scopeHash: scopeHash, dpHash: pdatautil.Hash64(pdatautil.WithMap(dpMap)), ts: uint64(ehdp.Timestamp())}}
						dp := &ExponentialHistogram{ExponentialHistogramDataPoint: ehdp, metric: m, dynamicTemplate: "exponential_histogram"}

						dpg, ok := dpgk2dpg[dpgk]
						if ok {
							dpg.dps = append(dpg.dps, dp)
						} else {
							dpgk2dpg[dpgk] = &DataPointGroup{
								resourceMap:  &resourceMap,
								scopeMap:     &scopeMap,
								scopeName:    scopeName,
								scopeVersion: scopeVersion,
								dpMap:        &dpMap,
								dps:          []DataPoint{dp},
							}
						}
					}
				}
			}
		}
	}

	if len(dataStreams) == 0 {
		exp.buf.Put(buf)
		return nil
	}

	hasMetrics := false
	for ds := range dataStreams {
		attrsDS := []attribute.KeyValue{attribute.String("dataset", ds.dataset), attribute.String("namespace", ds.namespace)}
		moDS := metric.WithAttributeSet(attribute.NewSet(append(exp.attrs, attrsDS...)...))

		for dpkg, dpg := range dpgk2dpg {
			if ds != dpkg.ds {
				continue
			}

			w.startObject()
			w.writeKey("create", true)
			w.startObject()
			w.writeKey("_index", true)
			w.writeString("metrics-" + ds.dataset + "-" + ds.namespace)
			w.writeKey("dynamic_templates", false)
			w.startObject()

			metricNamesMap := make(map[string]int, len(dpg.dps))

			first := true
			for i := range dpg.dps {
				dp := dpg.dps[i]
				metricName := dp.Metric().Name()

				if _, ok := metricNamesMap[metricName]; ok {
					exp.tb.ElasticsearchDatapointsReceived.Add(ctx, one, metric.WithAttributeSet(attribute.NewSet(append(append(exp.attrs, attrsDS...), semconv.ErrorTypeKey.String("datapoint_duplicate"))...)))
					exp.set.Logger.Info("DataPoint.Duplicate", zap.String("metric", metricName), zap.String("attributes", dpg.stringifyMaps()))
					continue
				}

				metricNamesMap[metricName] = i

				first = w.writeKey("metrics."+metricName, first)
				w.writeString(dp.DynamicTemplate())
			}

			w.endObject()
			w.endObject()
			w.endObject()
			w.newLine()

			w.startObject()
			w.writeKey("@timestamp", true)
			w.buf.Write(strconv.AppendUint(w.buf.AvailableBuffer(), dpkg.dpHash.ts/1e6, 10))

			w.writeKey("data_stream", false)
			w.startObject()
			w.writeKey("type", true)
			w.writeString("metrics")
			w.writeKey("dataset", false)
			w.writeString(ds.dataset)
			w.writeKey("namespace", false)
			w.writeString(ds.namespace)
			w.endObject()

			w.writeMap(dpg.dpMap, true, false)

			w.writeKey("resource", false)
			w.startObject()
			w.writeMap(dpg.resourceMap, true, true)
			w.endObject()

			w.writeKey("scope", false)
			w.startObject()
			w.writeKey("name", true)
			w.writeString(dpg.scopeName)
			w.writeKey("version", false)
			w.writeString(dpg.scopeVersion)
			w.writeMap(dpg.scopeMap, true, false)
			w.endObject()

			w.writeKey("metrics", false)
			w.startObject()

			sortedMetricSlice := make([]string, 0, len(metricNamesMap))

			first = true
			for metricName, i := range metricNamesMap {
				v := dpg.dps[i].Value()

				if v.Type() == pcommon.ValueTypeEmpty {
					exp.tb.ElasticsearchDatapointsReceived.Add(ctx, one, metric.WithAttributeSet(attribute.NewSet(append(append(exp.attrs, attrsDS...), semconv.ErrorTypeKey.String("datapoint_empty"))...)))
					exp.set.Logger.Info("DataPoint.Empty", zap.String("metric", metricName), zap.String("attributes", dpg.stringifyMaps()))
					continue
				}

				sortedMetricSlice = append(sortedMetricSlice, metricName)

				first = w.writeKey(metricName, first)
				w.writeValue(v, false)

				exp.tb.ElasticsearchDatapointsReceived.Add(ctx, one, metric.WithAttributeSet(attribute.NewSet(append(append(exp.attrs, attrsDS...), attribute.String("metric", metricName))...)))
			}

			w.endObject()

			if len(sortedMetricSlice) > 0 {
				slices.Sort(sortedMetricSlice)
				for i := range sortedMetricSlice {
					_, _ = digest.WriteString(sortedMetricSlice[i])
				}

				w.writeKey("_metric_names_hash", false)
				w.writeString(strconv.FormatUint(digest.Sum64(), 16))
				w.endObject()

				digest.Reset()

				hasMetrics = true
			}

			w.newLine()

			exp.tb.ElasticsearchDocsReceived.Add(ctx, one, moDS)
		}
	}

	if !hasMetrics {
		buf.Reset()
		exp.buf.Put(buf)
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, exp.cs.Select(exp.url), bytes.NewReader(buf.Bytes()))
	if err != nil {
		buf.Reset()
		exp.buf.Put(buf)
		// should never happen
		exp.set.Logger.Error("http.NewRequestWithContext", zap.Error(err))
		return consumererror.NewPermanent(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := exp.client.Do(req)
	switch {
	case err != nil:
		exp.set.Logger.Error("http.Do", zap.Error(err))

		attrs = append(attrs, semconv.ErrorType(err))
		event = componentstatus.NewRecoverableErrorEvent(err)
	case resp.StatusCode == http.StatusOK:
		var body BulkItems
		if e := sonic.ConfigFastest.NewDecoder(resp.Body).Decode(&body); e != nil {
			// should never happen
			exp.set.Logger.Error("json.Decode", zap.Int("status", resp.StatusCode), zap.Error(e))
		}
		resp.Body.Close()

		for i := range body.Items {
			item := &body.Items[i]
			if item.Create.Status == http.StatusCreated {
				exp.tb.ElasticsearchDocsProcessed.Add(ctx, one, metric.WithAttributeSet(attribute.NewSet(append(exp.attrs, semconv.HTTPResponseStatusCodeKey.Int(item.Create.Status))...)))
				continue
			}

			exp.tb.ElasticsearchDocsProcessed.Add(ctx, one, metric.WithAttributeSet(attribute.NewSet(append(exp.attrs, semconv.HTTPResponseStatusCodeKey.Int(item.Create.Status), semconv.ErrorTypeKey.String(item.Create.Error.Type))...)))

			if item.Create.Error.Type == "version_conflict_engine_exception" {
				continue // do not log
			}

			if err == nil && (item.Create.Status == http.StatusUnauthorized || item.Create.Status == http.StatusTooManyRequests) {
				err = exporterhelper.NewThrottleRetry(errors.New(resp.Status), time.Minute)
			}

			exp.set.Logger.Error("bulk.DocumentError", zap.Int("status", item.Create.Status), zap.String("type", item.Create.Error.Type), zap.String("reson", item.Create.Error.Reason))
		}

		if err == nil {
			attrs = append(attrs, semconv.HTTPResponseStatusCodeKey.Int(resp.StatusCode))
			event = componentstatus.NewEvent(componentstatus.StatusOK)
		} else {
			attrs = append(attrs, semconv.HTTPResponseStatusCodeKey.Int(resp.StatusCode), semconv.ErrorTypeKey.String("document_retry"))
			event = componentstatus.NewRecoverableErrorEvent(err)
		}
	case resp.StatusCode < http.StatusInternalServerError:
		var body BulkError
		if e := sonic.ConfigFastest.NewDecoder(resp.Body).Decode(&body); e != nil {
			// should never happen
			exp.set.Logger.Error("json.Decode", zap.Int("status", resp.StatusCode), zap.Error(e))
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusTooManyRequests {
			err = exporterhelper.NewThrottleRetry(errors.New(resp.Status), time.Minute)
		} else {
			err = consumererror.NewPermanent(errors.New(resp.Status))
		}

		exp.set.Logger.Error("bulk.ClientError", zap.Int("status", body.Status), zap.String("type", body.Error.Type), zap.String("reson", body.Error.Reason))

		attrs = append(attrs, semconv.HTTPResponseStatusCodeKey.Int(resp.StatusCode), semconv.ErrorTypeKey.String(body.Error.Type))
		event = componentstatus.NewRecoverableErrorEvent(err)
	default:
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()

		err = errors.New(resp.Status)

		exp.set.Logger.Error("bulk.ServerError", zap.Int("status", resp.StatusCode))

		attrs = append(attrs, semconv.HTTPResponseStatusCodeKey.Int(resp.StatusCode))
		event = componentstatus.NewRecoverableErrorEvent(err)
	}

	exp.tb.ElasticsearchBulkRequestsLatency.Record(ctx, time.Since(now).Seconds(), metric.WithAttributeSet(attribute.NewSet(append(exp.attrs, attrs...)...)))

	componentstatus.ReportStatus(exp.host, event)

	buf.Reset()
	exp.buf.Put(buf)

	return err
}
