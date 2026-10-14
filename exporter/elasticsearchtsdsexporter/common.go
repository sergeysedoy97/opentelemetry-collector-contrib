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
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"gitlab.rip/platform/go-starter/v3/oneconf"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

const (
	DataStreamDataset   = "data_stream.dataset"
	DataStreamNamespace = "data_stream.namespace"
	oneConfProperty     = "internal.elasticsearchexporter"
	one                 = int64(1)
	maxWeight           = 100
)

var (
	dot     = []byte{'.'}
	comma   = []byte{','}
	hyphen  = []byte{'-'}
	quote   = []byte{'"'}
	rcBrace = []byte{'}'}
	zero    = []byte{'0'}

	qc  = []byte{'"', ':'}
	qcq = []byte{'"', ':', '"'}

	gddt = []byte("gauge_double")
	gldt = []byte("gauge_long")
	hdt  = []byte("histogram")
	ehdt = []byte("exponential_histogram")

	json1  = []byte("{\"create\":{\"_index\":\"metrics-")
	json2  = []byte("\",\"dynamic_templates\":{")
	json3  = []byte("\"metrics.")
	json4  = []byte("}}}\n{\"@timestamp\":")
	json5  = []byte(",\"data_stream\":{\"type\":\"metrics\",\"dataset\":\"")
	json6  = []byte("\",\"namespace\":\"")
	json7  = []byte("\"},\"attributes\":{")
	json8  = []byte("},\"resource\":{\"attributes\":{")
	json9  = []byte("}},\"scope\":{\"attributes\":{")
	json10 = []byte("},\"name\":\"")
	json11 = []byte("\",\"version\":\"")
	json12 = []byte("\"},\"metrics\":{")
	json13 = []byte(",\"_metric_names_hash\":\"")
	json14 = []byte("}\n")
	json15 = []byte("{\"values\":[")
	json16 = []byte("],\"counts\":[")
	json17 = []byte("]}")
	json18 = []byte("{\"scale\":")
	json19 = []byte(",\"sum\":")
	json20 = []byte(",\"min\":")
	json21 = []byte(",\"max\":")
	json22 = []byte(",\"zero\":{\"threshold\":")
	json23 = []byte(",\"count\":")
	json24 = []byte(",\"positive\":{\"indices\":[")
	json25 = []byte(",\"negative\":{\"indices\":[")

	digestPool = &sync.Pool{New: func() any {
		return xxhash.New()
	}}
	bufPool = &sync.Pool{New: func() any {
		return bytes.NewBuffer(make([]byte, 1024*1024))
	}}
	gzipPool = &sync.Pool{New: func() any {
		z, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		return z
	}}
	bufioPool = &sync.Pool{New: func() any {
		return bufio.NewWriterSize(io.Discard, 8192)
	}}
	requestStatePool = &sync.Pool{New: func() any {
		return &RequestState{dph2dpg: make(map[DataPointHash]*DataPointGroup, 64), seenMap: make(map[string]struct{}, 64), sortedMetricSlice: make([]string, 0, 64)}
	}}
	dpgPool = &sync.Pool{New: func() any {
		return &DataPointGroup{ndp2mn: make([]string, 0, 64), hdp2mn: make([]string, 0, 64), ehdp2mn: make([]string, 0, 64), ndps: make([]pmetric.NumberDataPoint, 0, 64), hdps: make([]pmetric.HistogramDataPoint, 0, 64), ehdps: make([]pmetric.ExponentialHistogramDataPoint, 0, 64)}
	}}

	attrNum    = attribute.String("type", "number")
	attrHis    = attribute.String("type", "histogram")
	attrExpHis = attribute.String("type", "exponential_histogram")

	attrEmpty = semconv.ErrorTypeKey.String("datapoint_empty")
	attrDup   = semconv.ErrorTypeKey.String("datapoint_duplicate")
	attrRetry = semconv.ErrorTypeKey.String("document_retry")

	attrOK         = semconv.HTTPResponseStatusCodeKey.Int(http.StatusOK)
	attrCreated    = semconv.HTTPResponseStatusCodeKey.Int(http.StatusCreated)
	attrBadRequest = semconv.HTTPResponseStatusCodeKey.Int(http.StatusBadRequest)
)

type DataPointGroup struct {
	dataStream   DataStream
	resourceMap  pcommon.Map
	scopeMap     pcommon.Map
	dpMap        pcommon.Map
	scopeName    string
	scopeVersion string
	ndp2mn       []string
	hdp2mn       []string
	ehdp2mn      []string
	ndps         []pmetric.NumberDataPoint
	hdps         []pmetric.HistogramDataPoint
	ehdps        []pmetric.ExponentialHistogramDataPoint
}

type DataStream struct {
	dataset   string
	namespace string
	key       string
}

func (ds DataStream) Update(m pcommon.Map) DataStream {
	if v, ok := m.Get(DataStreamDataset); ok && v.Type() == pcommon.ValueTypeStr {
		if s := v.Str(); s != "" {
			ds.dataset = s
		}
	}
	if v, ok := m.Get(DataStreamNamespace); ok && v.Type() == pcommon.ValueTypeStr {
		if s := v.Str(); s != "" {
			ds.namespace = s
		}
	}

	return ds
}

type DataPointHash struct {
	resourceHash uint64
	scopeHash    uint64
	dpHash       uint64
	ts           uint64
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

type RequestState struct {
	dph2dpg           map[DataPointHash]*DataPointGroup
	seenMap           map[string]struct{}
	sortedMetricSlice []string
	stash             [64]byte
}

func (rs *RequestState) WriteFloat(w io.Writer, asDouble float64) {
	if math.IsInf(asDouble, 0) || math.IsNaN(asDouble) {
		_, _ = w.Write(zero)
		_, _ = w.Write(dot)
		_, _ = w.Write(zero)
		return
	}

	var hasDot bool
	var tail [8]byte

	head := strconv.AppendFloat(rs.stash[:0], asDouble, 'g', -1, 64)
	eAt := len(head)
	for i, c := range head {
		switch c {
		case 'e':
			eAt = i
		case '.':
			hasDot = true
		}
	}
	if hasDot {
		_, _ = w.Write(head)
		return
	}
	// insert ".0" before exponent
	tailLen := copy(tail[:], head[eAt:])
	_, _ = w.Write(head[:eAt])
	_, _ = w.Write(dot)
	_, _ = w.Write(zero)
	_, _ = w.Write(tail[:tailLen])
}

func (rs *RequestState) WriteMap(w io.Writer, m pcommon.Map) {
	isFirst := true
	clear(rs.seenMap)
	for k, v := range m.All() {
		if k == DataStreamDataset || k == DataStreamNamespace {
			continue
		}
		if _, ok := rs.seenMap[k]; ok {
			continue
		}
		rs.seenMap[k] = struct{}{}

		if !isFirst {
			_, _ = w.Write(comma)
		}
		_, _ = w.Write(quote)
		_, _ = w.Write(stringToBytes(k))
		_, _ = w.Write(qcq)

		switch v.Type() {
		case pcommon.ValueTypeStr:
			s := v.Str()
			p := 0
			for i := 0; i < len(s); i++ {
				c := s[i]
				if c >= utf8.RuneSelf || c < 32 || c == 34 || c == 92 || c == 127 {
					if i-p > 0 {
						_, _ = w.Write(stringToBytes(s[p:i]))
					}
					p = i + 1
					continue
				}
			}
			_, _ = w.Write(stringToBytes(s[p:]))
		case pcommon.ValueTypeInt:
			_, _ = w.Write(strconv.AppendInt(rs.stash[:0], v.Int(), 10))
		case pcommon.ValueTypeDouble:
			_, _ = w.Write(strconv.AppendFloat(rs.stash[:0], v.Double(), 'g', -1, 64))
		case pcommon.ValueTypeBool:
			_, _ = w.Write(strconv.AppendBool(rs.stash[:0], v.Bool()))
		}

		_, _ = w.Write(quote)
		isFirst = false
	}
}

type SelectorState struct {
	urls   []string
	seq    []int
	seqLen int
}

type Selector struct {
	index       atomic.Uint64
	state       atomic.Pointer[SelectorState]
	mutex       sync.Mutex
	unsubscribe oneconf.UnsubscribeFunc
}

func (cs *Selector) Select(path string) string {
	state := cs.state.Load()
	if state == nil || state.seqLen == 0 {
		return ""
	}
	return state.urls[state.seq[cs.index.Add(1)%uint64(state.seqLen)]] + path
}

func (cs *Selector) SetupConf(v string) error {
	if v == "" {
		return errors.New("value cannot be empty")
	}
	var sumWeight int
	var weights []int
	var urls []string
	for endpoint := range strings.SplitSeq(v, "\n") {
		if endpoint == "" || endpoint[0] == '#' {
			continue
		}
		strURL, strWeight, found := strings.Cut(endpoint, "|")
		if !found {
			return errors.New("endpoint weight must be defined")
		}
		parsedURL, err := url.Parse(strings.TrimRight(strURL, "/"))
		if err != nil {
			return err
		}
		parsedWeight, err := strconv.ParseUint(strWeight, 10, 8)
		if err != nil {
			return err
		}
		if parsedWeight > maxWeight {
			return errors.New("endpoint weight must be between 0 and 100")
		}
		weight := int(parsedWeight)
		sumWeight += weight
		weights = append(weights, weight)
		urls = append(urls, parsedURL.String())
	}
	count := len(weights)
	if count == 0 {
		return errors.New("endpoints count is zero")
	}
	if sumWeight == 0 {
		return errors.New("endpoints weight sum cannot be zero")
	}
	rand.Shuffle(count, func(i, j int) {
		weights[i], weights[j] = weights[j], weights[i]
		urls[i], urls[j] = urls[j], urls[i]
	})
	if sumWeight == maxWeight*count {
		cs.SetupRR(urls)
	} else {
		cs.SetupWRR(urls, weights, sumWeight)
	}
	return nil
}

func (cs *Selector) SetupWRR(urls []string, weights []int, sumWeight int) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	seqLen := 0
	seq := make([]int, seqLen, sumWeight)
	for seqLen < sumWeight {
		for i := range weights {
			if weights[i] == 0 {
				continue
			}
			weights[i]--
			seq = append(seq, i)
			seqLen++
		}
	}
	cs.state.Store(&SelectorState{urls: urls, seq: seq, seqLen: seqLen})
	cs.index.Store(0)
}

func (cs *Selector) SetupRR(urls []string) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	seqLen := len(urls)
	seq := make([]int, seqLen)
	for i := range seq {
		seq[i] = i
	}
	cs.state.Store(&SelectorState{urls: urls, seq: seq, seqLen: seqLen})
	cs.index.Store(0)
}

func (cs *Selector) Unsubscribe() {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if cs.unsubscribe != nil {
		cs.unsubscribe()
		cs.unsubscribe = nil
	}
}

type ElasticsearchBulkRequests struct {
	buf []byte
	cnt int
}

func (ebr *ElasticsearchBulkRequests) ItemsCount() int {
	return ebr.cnt
}

func (ebr *ElasticsearchBulkRequests) MergeSplit(_ context.Context, maxSize int, sizerType exporterhelper.RequestSizerType, wreq xexporterhelper.Request) ([]xexporterhelper.Request, error) {
	if wreq == nil {
		return []xexporterhelper.Request{ebr}, nil
	}

	ebrExtra, ok := wreq.(*ElasticsearchBulkRequests)
	if !ok {
		return nil, errors.ErrUnsupported
	}

	if sizerType == exporterhelper.RequestSizerTypeBytes && ebr.BytesSize()+ebrExtra.BytesSize() <= maxSize {
		ebr.buf = append(ebr.buf, ebrExtra.buf...)
		ebr.cnt += ebrExtra.cnt
		return []xexporterhelper.Request{ebr}, nil
	}

	return []xexporterhelper.Request{ebr, ebrExtra}, nil
}

func (ebr *ElasticsearchBulkRequests) BytesSize() int {
	if ebr.buf == nil {
		return 0
	}
	return len(ebr.buf)
}

func (ebr *ElasticsearchBulkRequests) OnError(error) xexporterhelper.Request {
	return ebr
}

type Encoding struct{}

func (Encoding) Unmarshal(buf []byte) (context.Context, xexporterhelper.Request, error) {
	if buf == nil {
		return context.Background(), nil, errors.New("unmarshal: buf cannot be nil")
	}
	return context.Background(), &ElasticsearchBulkRequests{buf: buf, cnt: 1}, nil
}

func (Encoding) Marshal(_ context.Context, wreq xexporterhelper.Request) ([]byte, error) {
	if wreq == nil {
		return nil, errors.New("marshal: wreq cannot be nil")
	}
	ebr, ok := wreq.(*ElasticsearchBulkRequests)
	if !ok {
		return nil, errors.ErrUnsupported
	}
	if ebr.buf == nil {
		return nil, errors.New("marshal: buf cannot be nil")
	}
	return ebr.buf, nil
}

func safeUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

func stringifyMaps(maps ...pcommon.Map) string {
	var sb strings.Builder
	for _, m := range maps {
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

func stringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
