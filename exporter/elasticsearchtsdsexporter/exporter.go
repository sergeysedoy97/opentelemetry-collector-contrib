// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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
	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	elastictransportversion "github.com/elastic/elastic-transport-go/v8/elastictransport/version"
	slogzap "github.com/samber/slog-zap/v2"
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
	header http.Header
	api    sonic.API
	tb     *metadata.TelemetryBuilder
	buf    *sync.Pool
	host   component.Host
	client *elastictransport.Client
	attrs  []attribute.KeyValue
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

	header := make(http.Header, 1)
	header.Set("Content-Type", "application/json")

	return &ElasticsearchExporter{
		config: config,
		set:    set,
		url:    u.RequestURI(),
		header: header,
		api:    sonic.Config{DisallowUnknownFields: true}.Froze(),
		tb:     tb,
		buf:    &sync.Pool{New: func() any { return &bytes.Buffer{} }},
	}, nil
}

func (exp *ElasticsearchExporter) Start(ctx context.Context, host component.Host) error {
	var errs []error
	var urls []*url.URL
	var slogLevel slog.Leveler

	nativeClient, err := exp.config.ToClient(ctx, host.GetExtensions(), exp.set.TelemetrySettings)
	if err != nil {
		return err
	}

	for _, rawURL := range append(exp.config.Endpoints, exp.config.Endpoint) {
		if rawURL == "" {
			continue
		}
		u, e := url.Parse(strings.TrimRight(rawURL, "/"))
		if e != nil {
			errs = append(errs, e)
			continue
		}
		urls = append(urls, u)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	cs := &CustomSelector{index: atomic.Uint64{}, state: atomic.Pointer[CustomSelectorState]{}, logger: exp.set.Logger}

	ext := oneconfextension.Get(host)
	if ext != nil {
		conf := ext.GetConf()
		if conf != nil {
			if v, ok := conf.GetString(oneConfProperty); ok && cs.setupConf(v) {
				cs.unsubscribe = conf.SubscribeString(oneConfProperty, func(_ one_configuration.Key, _, cur *string) {
					if cur != nil {
						cs.setupConf(*cur)
					}
				})
			}
		}
	}

	switch exp.set.Logger.Level() {
	case zap.DebugLevel:
		slogLevel = slog.LevelDebug
	case zap.InfoLevel:
		slogLevel = slog.LevelInfo
	case zap.WarnLevel:
		slogLevel = slog.LevelWarn
	case zap.ErrorLevel:
		slogLevel = slog.LevelError
	}

	client, err := elastictransport.NewClient(
		elastictransport.WithSelector(cs),
		elastictransport.WithConnectionPoolFunc(func(conns []*elastictransport.Connection, selector elastictransport.Selector) elastictransport.ConnectionPool {
			ccp := &CustomConnectionPool{conns: slices.Clone(conns), selector: selector}
			if s, ok := ccp.selector.(*CustomSelector); ok {
				if s.unsubscribe == nil {
					s.setupRR(ccp.conns)
				}
			}
			return ccp
		}),
		elastictransport.WithDisableRetry(),
		elastictransport.WithURLs(urls...),
		elastictransport.WithLeveledLogger(&elastictransport.SlogLogger{Logger: slog.New(slogzap.Option{Level: slogLevel, Logger: exp.set.Logger}.NewZapHandler())}),
		elastictransport.WithTransport(nativeClient.Transport),
		elastictransport.WithInstrumentation(elastictransport.NewOtelInstrumentation(exp.set.TracerProvider, false, elastictransportversion.Version)),
	)
	if err != nil {
		return err
	}

	exp.host = host
	exp.client = client
	exp.attrs = append(exp.attrs, attribute.String("exporter", exp.set.ID.String()))

	return err
}

func (exp *ElasticsearchExporter) Shutdown(ctx context.Context) error {
	if exp.client != nil {
		err := exp.client.Close(ctx)
		if err != nil {
			return err
		}
		exp.client = nil
	}
	if exp.tb != nil {
		exp.tb.Shutdown()
		exp.tb = nil
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
	buf.Reset()
	w := &JSONWriter{buf: buf}
	defer exp.buf.Put(buf)

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

			metricNames := make([]string, 0, len(dpg.dps))
			dpIdx := make([]int, 0, len(dpg.dps))

			first := true
			for i := range dpg.dps {
				dp := dpg.dps[i]
				metricName := dp.Metric().Name()

				if slices.Contains(metricNames, metricName) {
					exp.tb.ElasticsearchDatapointsReceived.Add(ctx, one, metric.WithAttributeSet(attribute.NewSet(append(append(exp.attrs, attrsDS...), attribute.String("metric", metricName), semconv.ErrorTypeKey.String("Duplicate"))...)))
					exp.set.Logger.Info("DataPoint.Duplicate", zap.String("metric", metricName), zap.String("attributes", dpg.stringifyMaps()))
					continue
				}

				metricNames = append(metricNames, metricName)

				first = w.writeKey("metrics."+metricName, first)
				w.writeString(dp.DynamicTemplate())

				dpIdx = append(dpIdx, i)
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

			first = true
			for i, metricName := range metricNames {
				v := dpg.dps[dpIdx[i]].Value()

				if v.Type() == pcommon.ValueTypeEmpty {
					exp.tb.ElasticsearchDatapointsReceived.Add(ctx, one, metric.WithAttributeSet(attribute.NewSet(append(append(exp.attrs, attrsDS...), attribute.String("metric", metricName), semconv.ErrorTypeKey.String("Empty"))...)))
					exp.set.Logger.Info("DataPoint.Empty", zap.String("metric", metricName), zap.String("attributes", dpg.stringifyMaps()))
					continue
				}

				first = w.writeKey(metricName, first)
				w.writeValue(v, false)

				exp.tb.ElasticsearchDatapointsReceived.Add(ctx, one, metric.WithAttributeSet(attribute.NewSet(append(append(exp.attrs, attrsDS...), attribute.String("metric", metricName))...)))
			}

			w.endObject()

			slices.Sort(metricNames)
			digest.Reset()
			for i := range metricNames {
				_, _ = digest.WriteString(metricNames[i])
			}

			w.writeKey("_metric_names_hash", false)
			w.writeString(strconv.FormatUint(digest.Sum64(), 16))
			w.endObject()
			w.newLine()

			exp.tb.ElasticsearchDocsReceived.Add(ctx, one, moDS)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, exp.url, bytes.NewReader(buf.Bytes()))
	if err != nil {
		// should never happen
		exp.set.Logger.Error("http.NewRequestWithContext", zap.Error(err))
		return consumererror.NewPermanent(err)
	}
	req.Header = exp.header

	res, err := exp.client.Perform(req)
	switch {
	case err != nil:
		exp.set.Logger.Error("Bulk.Error", zap.Error(err))

		attrs = append(attrs, semconv.ErrorType(err))
		event = componentstatus.NewRecoverableErrorEvent(err)
	case res.StatusCode == http.StatusOK:
		var body BulkItems
		if e := exp.api.NewDecoder(res.Body).Decode(&body); e == nil {
			// should never happen
			exp.set.Logger.Error("jsoniter.NewDecoder", zap.Error(e))
		}

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
				err = exporterhelper.NewThrottleRetry(errors.New("document_retry"), time.Minute)
			}

			exp.set.Logger.Error("Bulk.DocError", zap.Int("status", item.Create.Status), zap.String("type", item.Create.Error.Type), zap.String("reson", item.Create.Error.Reason))
		}

		if err == nil {
			attrs = append(attrs, semconv.HTTPResponseStatusCodeKey.Int(res.StatusCode))
			event = componentstatus.NewEvent(componentstatus.StatusOK)
		} else {
			attrs = append(attrs, semconv.HTTPResponseStatusCodeKey.Int(res.StatusCode), semconv.ErrorTypeKey.String("document_retry"))
			event = componentstatus.NewRecoverableErrorEvent(err)
		}
	case res.StatusCode < http.StatusInternalServerError:
		var body BulkError
		if e := exp.api.NewDecoder(res.Body).Decode(&body); e == nil {
			// should never happen
			exp.set.Logger.Error("jsoniter.NewDecoder", zap.Error(e))
		}

		if res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusTooManyRequests {
			err = exporterhelper.NewThrottleRetry(errors.New(body.Error.Type), time.Minute)
		} else {
			err = consumererror.NewPermanent(errors.New(body.Error.Type))
		}

		exp.set.Logger.Error("Bulk.ClientError", zap.Int("status", body.Status), zap.String("type", body.Error.Type), zap.String("reson", body.Error.Reason))

		attrs = append(attrs, semconv.HTTPResponseStatusCodeKey.Int(res.StatusCode), semconv.ErrorTypeKey.String(body.Error.Type))
		event = componentstatus.NewRecoverableErrorEvent(err)
	default:
		err = errors.New(res.Status)

		exp.set.Logger.Error("Bulk.ServerError", zap.Error(err))

		attrs = append(attrs, semconv.HTTPResponseStatusCodeKey.Int(res.StatusCode))
		event = componentstatus.NewRecoverableErrorEvent(err)
	}

	mo := metric.WithAttributeSet(attribute.NewSet(append(exp.attrs, attrs...)...))
	exp.tb.ElasticsearchBulkRequestsCount.Add(ctx, one, mo)
	exp.tb.ElasticsearchBulkRequestsLatency.Record(ctx, time.Since(now).Seconds(), mo)

	componentstatus.ReportStatus(exp.host, event)

	return err
}
