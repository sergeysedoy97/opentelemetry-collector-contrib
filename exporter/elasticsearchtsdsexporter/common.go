// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/bits"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/gzip"
	"gitlab.rip/platform/go-starter/v3/oneconf"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

const (
	DataStreamDataset   = "data_stream.dataset"
	DataStreamNamespace = "data_stream.namespace"
	oneConfProperty     = "internal.elasticsearchexporter"
	one                 = int64(1)
	maxWeight           = 100

	initialBufCap        = 32 * 1024
	maxPooledBufCap      = 512 * 1024
	maxPooledBatchSize   = 4 * 1024 * 1024
	maxPooledBulkItems   = 16384
	maxResponseBytes     = 16 * 1024 * 1024
	maxPooledDPGMapLen   = 4096
	maxPooledSeenMapLen  = 1024
	maxPooledMetricSlice = 2048
	maxPooledSeriesSlice = 256
	maxCachedDataStreams = 4096

	qc  = "\":"
	qcq = "\":\""

	gddt = "gauge_double"
	gldt = "gauge_long"
	hdt  = "histogram"
	ehdt = "exponential_histogram"

	json1  = "{\"create\":{\"_index\":\"metrics-"
	json2  = "\",\"dynamic_templates\":{"
	json3  = "\"metrics."
	json4  = "}}}\n{\"@timestamp\":"
	json5  = ",\"data_stream\":{\"type\":\"metrics\",\"dataset\":\""
	json6  = "\",\"namespace\":\""
	json7  = "\"},\"attributes\":{"
	json8  = "},\"resource\":{\"attributes\":{"
	json9  = "}},\"scope\":{\"attributes\":{"
	json10 = "},\"name\":\""
	json11 = "\",\"version\":\""
	json12 = "\"},\"metrics\":{"
	json13 = ",\"_metric_names_hash\":\""
	json14 = "}\n"
	json15 = "{\"values\":["
	json16 = "],\"counts\":["
	json17 = "]}"
	json18 = "{\"scale\":"
	json19 = ",\"sum\":"
	json20 = ",\"min\":"
	json21 = ",\"max\":"
	json22 = ",\"zero\":{\"threshold\":"
	json23 = ",\"count\":"
	json24 = ",\"positive\":{\"indices\":["
	json25 = ",\"negative\":{\"indices\":["
)

var (
	bufPool = &sync.Pool{New: func() any {
		return bytes.NewBuffer(make([]byte, 0, initialBufCap))
	}}
	gzipPool = &sync.Pool{New: func() any {
		z, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		return z
	}}
	bufioPool = &sync.Pool{New: func() any {
		return bufio.NewWriterSize(io.Discard, 8192)
	}}
	requestStatePool = &sync.Pool{New: func() any {
		return &RequestState{
			dph2dpg:           make(map[DataPointHash]*DataPointGroup, 64),
			seenMap:           make(map[string]struct{}, 64),
			sortedMetricSlice: make([]string, 0, 64),
		}
	}}
	dpgPool = &sync.Pool{New: func() any {
		return &DataPointGroup{
			ndps: make([]namedNDP, 0, 8),
		}
	}}
	batchBufPool = &sync.Pool{New: func() any {
		var b []byte
		return &b
	}}
	bulkItemsPool = &sync.Pool{New: func() any {
		return &BulkItems{
			Items: make([]BulkItem, 0, 256),
		}
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

type dsMetrics struct {
	dpNumEmpty   []metric.AddOption
	dpNumDup     []metric.AddOption
	dpNum        []metric.AddOption
	dpHisDup     []metric.AddOption
	dpHis        []metric.AddOption
	dpExpHisDup  []metric.AddOption
	dpExpHis     []metric.AddOption
	docsReceived []metric.AddOption
}

type namedNDP struct {
	dp   pmetric.NumberDataPoint
	name string
}

type namedHDP struct {
	dp   pmetric.HistogramDataPoint
	name string
}

type namedEHDP struct {
	dp   pmetric.ExponentialHistogramDataPoint
	name string
}

type DataPointGroup struct {
	resourceMap  pcommon.Map
	scopeMap     pcommon.Map
	dpMap        pcommon.Map
	metrics      *dsMetrics
	dataStream   DataStream
	scopeName    string
	scopeVersion string
	ndps         []namedNDP
	hdps         []namedHDP
	ehdps        []namedEHDP
}

func (dpg *DataPointGroup) Reset() {
	clear(dpg.ndps)
	if cap(dpg.ndps) > maxPooledSeriesSlice {
		dpg.ndps = make([]namedNDP, 0, 8)
	} else {
		dpg.ndps = dpg.ndps[:0]
	}

	clear(dpg.hdps)
	if cap(dpg.hdps) > maxPooledSeriesSlice {
		dpg.hdps = nil
	} else {
		dpg.hdps = dpg.hdps[:0]
	}

	clear(dpg.ehdps)
	if cap(dpg.ehdps) > maxPooledSeriesSlice {
		dpg.ehdps = nil
	} else {
		dpg.ehdps = dpg.ehdps[:0]
	}

	dpg.resourceMap = pcommon.Map{}
	dpg.scopeMap = pcommon.Map{}
	dpg.dpMap = pcommon.Map{}
	dpg.metrics = nil
	dpg.dataStream = DataStream{}
	dpg.scopeName = ""
	dpg.scopeVersion = ""
}

type DataStream struct {
	dataset   string
	namespace string
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

type BulkItem struct {
	Create BulkError `json:"create"`
}

type BulkItems struct {
	Items []BulkItem `json:"items"`
}

type RequestState struct {
	dph2dpg           map[DataPointHash]*DataPointGroup
	seenMap           map[string]struct{}
	sortedMetricSlice []string
	stash             [64]byte
}

func (rs *RequestState) Reset() {
	if len(rs.dph2dpg) > maxPooledDPGMapLen {
		rs.dph2dpg = make(map[DataPointHash]*DataPointGroup, 64)
	} else {
		clear(rs.dph2dpg)
	}
	if len(rs.seenMap) > maxPooledSeenMapLen {
		rs.seenMap = make(map[string]struct{}, 64)
	} else {
		clear(rs.seenMap)
	}
	clear(rs.sortedMetricSlice)
	if cap(rs.sortedMetricSlice) > maxPooledMetricSlice {
		rs.sortedMetricSlice = make([]string, 0, 64)
	} else {
		rs.sortedMetricSlice = rs.sortedMetricSlice[:0]
	}
}

func (rs *RequestState) WriteFloat(w *bufio.Writer, asDouble float64) {
	if math.IsInf(asDouble, 0) || math.IsNaN(asDouble) {
		_, _ = w.WriteString("0.0")
		return
	}

	head := strconv.AppendFloat(rs.stash[:0], asDouble, 'g', -1, 64)
	eAt := len(head)
	for i, c := range head {
		if c == '.' {
			_, _ = w.Write(head)
			return
		}
		if c == 'e' {
			eAt = i
			break
		}
	}

	// Insert ".0" before exponent directly in rs.stash without allocating.
	n := len(head)
	head = head[:n+2]
	copy(head[eAt+2:], head[eAt:n])
	head[eAt] = '.'
	head[eAt+1] = '0'
	_, _ = w.Write(head)
}

func (rs *RequestState) WriteMap(w *bufio.Writer, m pcommon.Map) {
	n := m.Len()
	if n == 0 {
		return
	}
	useSeen := n > 1
	if useSeen {
		clear(rs.seenMap)
	}

	isFirst := true
	for k, v := range m.All() {
		if k == DataStreamDataset || k == DataStreamNamespace {
			continue
		}
		if useSeen {
			if _, ok := rs.seenMap[k]; ok {
				continue
			}
			rs.seenMap[k] = struct{}{}
		}

		if !isFirst {
			_ = w.WriteByte(',')
		}
		_ = w.WriteByte('"')
		writeSanitizedString(w, k)
		_, _ = w.WriteString(qcq)

		switch v.Type() {
		case pcommon.ValueTypeStr:
			writeSanitizedString(w, v.Str())
		case pcommon.ValueTypeInt:
			_, _ = w.Write(strconv.AppendInt(rs.stash[:0], v.Int(), 10))
		case pcommon.ValueTypeDouble:
			_, _ = w.Write(strconv.AppendFloat(rs.stash[:0], v.Double(), 'g', -1, 64))
		case pcommon.ValueTypeBool:
			_, _ = w.Write(strconv.AppendBool(rs.stash[:0], v.Bool()))
		}

		_ = w.WriteByte('"')
		isFirst = false
	}

	if useSeen {
		clear(rs.seenMap)
	}
}

func writeSanitizedString(w *bufio.Writer, s string) {
	p := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c < 32 || c == '"' || c == '\\' || c == 127 {
				if i > p {
					_, _ = w.WriteString(s[p:i])
				}
				i++
				p = i
				continue
			}
			i++
			continue
		}
		r, width := utf8.DecodeRuneInString(s[i:])
		if unicode.Is(unicode.Cyrillic, r) {
			i += width
			continue
		}
		if i > p {
			_, _ = w.WriteString(s[p:i])
		}
		i += width
		p = i
	}
	if p < len(s) {
		_, _ = w.WriteString(s[p:])
	}
}

type SelectorState struct {
	urls     []string
	fullURLs []string
	seq      []int
	seqLen   int
}

type Selector struct {
	state       atomic.Pointer[SelectorState]
	unsubscribe oneconf.UnsubscribeFunc
	path        string
	index       atomic.Uint64
	mutex       sync.Mutex
}

func NewSelector(path string) *Selector {
	return &Selector{path: path}
}

func (cs *Selector) Select(path string) string {
	state := cs.state.Load()
	if state == nil || state.seqLen == 0 {
		return ""
	}
	idx := state.seq[cs.index.Add(1)%uint64(state.seqLen)]
	if path == cs.path {
		return state.fullURLs[idx]
	}
	return state.urls[idx] + path
}

func (cs *Selector) SetupConf(v string) error {
	if v == "" {
		return errors.New("value cannot be empty")
	}
	var sumWeight int
	var weights []int
	var urls []string
	for s := range strings.SplitSeq(v, "\n") {
		line := strings.TrimSpace(s)
		if line == "" || line[0] == '#' {
			continue
		}
		strURL, strWeight, found := strings.Cut(line, "|")
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
	seq := make([]int, 0, sumWeight)
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
	cs.state.Store(&SelectorState{
		urls:     urls,
		fullURLs: buildFullURLs(urls, cs.path),
		seq:      seq,
		seqLen:   seqLen,
	})
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
	cs.state.Store(&SelectorState{
		urls:     urls,
		fullURLs: buildFullURLs(urls, cs.path),
		seq:      seq,
		seqLen:   seqLen,
	})
	cs.index.Store(0)
}

func buildFullURLs(urls []string, path string) []string {
	full := make([]string, len(urls))
	for i, u := range urls {
		full[i] = strings.TrimRight(u, "/") + path
	}
	return full
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
	mergeBuf *[]byte
	buf      []byte
	cnt      int
}

func (ebr *ElasticsearchBulkRequests) ItemsCount() int {
	return ebr.cnt
}

func (ebr *ElasticsearchBulkRequests) MergeSplit(
	_ context.Context,
	maxSize int,
	sizerType exporterhelper.RequestSizerType,
	wreq xexporterhelper.Request,
) ([]xexporterhelper.Request, error) {
	if wreq == nil {
		return []xexporterhelper.Request{ebr}, nil
	}

	ebrExtra, ok := wreq.(*ElasticsearchBulkRequests)
	if !ok {
		return nil, errors.ErrUnsupported
	}

	combined := ebr.BytesSize() + ebrExtra.BytesSize()
	if sizerType == exporterhelper.RequestSizerTypeBytes && combined <= maxSize {
		if ebr.mergeBuf == nil || cap(ebr.buf) < combined {
			if maxSize <= maxPooledBatchSize {
				bp := batchBufPool.Get().(*[]byte)
				nb := (*bp)[:0]
				if cap(nb) < maxSize {
					nb = make([]byte, 0, maxSize)
				}
				nb = append(nb, ebr.buf...)
				nb = append(nb, ebrExtra.buf...)
				ebr.releaseMergeBuf()
				ebr.buf = nb
				ebr.mergeBuf = bp
			} else {
				nb := make([]byte, 0, maxSize)
				nb = append(nb, ebr.buf...)
				nb = append(nb, ebrExtra.buf...)
				ebr.releaseMergeBuf()
				ebr.buf = nb
			}
		} else {
			ebr.buf = append(ebr.buf, ebrExtra.buf...)
		}
		ebr.cnt += ebrExtra.cnt
		ebrExtra.releaseMergeBuf()
		return []xexporterhelper.Request{ebr}, nil
	}

	return []xexporterhelper.Request{ebr, ebrExtra}, nil
}

func (ebr *ElasticsearchBulkRequests) releaseMergeBuf() {
	if ebr.mergeBuf == nil {
		return
	}
	bp := ebr.mergeBuf
	ebr.mergeBuf = nil
	if cap(ebr.buf) <= maxPooledBatchSize {
		*bp = ebr.buf[:0]
		batchBufPool.Put(bp)
	}
}

func (ebr *ElasticsearchBulkRequests) BytesSize() int {
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

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func hashMap(m pcommon.Map) uint64 {
	n := m.Len()
	if n == 0 {
		return 0
	}
	var sum, xor uint64
	var d xxhash.Digest
	var buf [17]byte
	for k, v := range m.All() {
		d.Reset()
		binary.LittleEndian.PutUint64(buf[0:8], uint64(len(k)))
		_, _ = d.Write(buf[0:8])
		_, _ = d.WriteString(k)
		hashValue(&d, &buf, v)
		h := splitmix64(d.Sum64())
		sum += h
		xor ^= h
	}
	return splitmix64(sum) ^ bits.RotateLeft64(xor, 32) ^ uint64(n)
}

func hashScope(name, version string, m pcommon.Map) uint64 {
	if name == "" && version == "" && m.Len() == 0 {
		return 0
	}
	var d xxhash.Digest
	var buf [16]byte
	d.Reset()
	binary.LittleEndian.PutUint64(buf[0:8], uint64(len(name)))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(len(version)))
	_, _ = d.Write(buf[0:16])
	_, _ = d.WriteString(name)
	_, _ = d.WriteString(version)
	if m.Len() > 0 {
		binary.LittleEndian.PutUint64(buf[0:8], hashMap(m))
		_, _ = d.Write(buf[0:8])
	}
	return d.Sum64()
}

func hashValue(d *xxhash.Digest, buf *[17]byte, v pcommon.Value) {
	buf[0] = byte(v.Type())
	switch v.Type() {
	case pcommon.ValueTypeStr:
		s := v.Str()
		binary.LittleEndian.PutUint64(buf[1:9], uint64(len(s)))
		_, _ = d.Write(buf[0:9])
		_, _ = d.WriteString(s)
	case pcommon.ValueTypeInt:
		binary.LittleEndian.PutUint64(buf[1:9], uint64(v.Int()))
		_, _ = d.Write(buf[0:9])
	case pcommon.ValueTypeDouble:
		binary.LittleEndian.PutUint64(buf[1:9], math.Float64bits(v.Double()))
		_, _ = d.Write(buf[0:9])
	case pcommon.ValueTypeBool:
		if v.Bool() {
			buf[1] = 1
		} else {
			buf[1] = 0
		}
		_, _ = d.Write(buf[0:2])
	case pcommon.ValueTypeBytes:
		raw := v.Bytes().AsRaw()
		binary.LittleEndian.PutUint64(buf[1:9], uint64(len(raw)))
		_, _ = d.Write(buf[0:9])
		_, _ = d.Write(raw)
	case pcommon.ValueTypeMap:
		binary.LittleEndian.PutUint64(buf[1:9], hashMap(v.Map()))
		_, _ = d.Write(buf[0:9])
	case pcommon.ValueTypeSlice:
		binary.LittleEndian.PutUint64(buf[1:9], hashSlice(v.Slice()))
		_, _ = d.Write(buf[0:9])
	default:
		_, _ = d.Write(buf[0:1])
	}
}

func hashSlice(s pcommon.Slice) uint64 {
	n := s.Len()
	if n == 0 {
		return 0
	}
	var d xxhash.Digest
	var buf [17]byte
	d.Reset()
	for i := range n {
		hashValue(&d, &buf, s.At(i))
	}
	return d.Sum64()
}

func safeUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

func stringifyMaps(maps ...pcommon.Map) string {
	var approx int
	for _, m := range maps {
		approx += m.Len() * 24
	}
	var sb strings.Builder
	sb.Grow(approx)
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
