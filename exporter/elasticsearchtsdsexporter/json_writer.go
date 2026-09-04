// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"bytes"
	"encoding/hex"
	"math"
	"strconv"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

const hexChars = "0123456789abcdef"

// htmlSafeSet matches go-structform's htmlEscapeSet (inverted): true means safe (no escape needed).
var htmlSafeSet [utf8.RuneSelf]bool

func init() {
	for i := range htmlSafeSet {
		htmlSafeSet[i] = true
	}
	for i := range 32 {
		htmlSafeSet[i] = false
	}
	for _, c := range `\"` {
		htmlSafeSet[c] = false
	}
	for _, c := range "&<>" {
		htmlSafeSet[c] = false
	}
}

// JSONWriter is a low-overhead JSON writer that writes directly to a
// bytes.Buffer, avoiding the interface-dispatch and state-machine overhead
// of go-structform/json.Visitor.
type JSONWriter struct {
	buf *bytes.Buffer
}

// writeString writes a JSON-escaped string (with surrounding quotes).
// Uses the same HTML-safe escaping as go-structform and go-fastjson.
func (w *JSONWriter) writeString(v string) {
	w.buf.WriteByte('"')
	p := 0
	for i := 0; i < len(v); {
		c := v[i]
		if c < utf8.RuneSelf {
			if htmlSafeSet[c] {
				i++
				continue
			}
			w.buf.WriteString(v[p:i])
			switch c {
			case '\\':
				w.buf.WriteString(`\\`)
			case '"':
				w.buf.WriteString(`\"`)
			case '\b':
				w.buf.WriteString(`\b`)
			case '\f':
				w.buf.WriteString(`\f`)
			case '\n':
				w.buf.WriteString(`\n`)
			case '\r':
				w.buf.WriteString(`\r`)
			case '\t':
				w.buf.WriteString(`\t`)
			default:
				w.buf.WriteString(`\u00`)
				w.buf.WriteByte(hexChars[c>>4])
				w.buf.WriteByte(hexChars[c&0xf])
			}
			i++
			p = i
			continue
		}
		r, s := utf8.DecodeRuneInString(v[i:])
		if r == utf8.RuneError && s == 1 {
			w.buf.WriteString(v[p:i])
			w.buf.WriteString(`\ufffd`)
			i++
			p = i
			continue
		}
		if r == '\u2028' || r == '\u2029' {
			w.buf.WriteString(v[p:i])
			w.buf.WriteString(`\u202`)
			w.buf.WriteByte(hexChars[r&0xf])
			i += s
			p = i
			continue
		}
		i += s
	}
	w.buf.WriteString(v[p:])
	w.buf.WriteByte('"')
}

func (w *JSONWriter) startObject() {
	w.buf.WriteByte('{')
}

func (w *JSONWriter) endObject() {
	w.buf.WriteByte('}')
}

func (w *JSONWriter) newLine() {
	w.buf.WriteByte('\n')
}

// writeKey writes a JSON object writeKey with a preceding comma if first is false.
// Returns false (the new value for the caller's "first" tracking variable).
func (w *JSONWriter) writeKey(k string, first bool) bool {
	if !first {
		w.buf.WriteByte(',')
	}
	w.writeString(k)
	w.buf.WriteByte(':')
	return false
}

func (w *JSONWriter) writeMap(m *pcommon.Map, stringify, first bool) {
	if m.Len() == 0 {
		return
	}
	f := true
	h := make(map[string]struct{}, m.Len())
	w.writeKey("attributes", first)
	w.startObject()
	for k, v := range m.All() {
		if k == DataStreamDataset || k == DataStreamNamespace {
			continue
		}
		if _, ok := h[k]; ok {
			continue
		}
		h[k] = struct{}{}
		f = w.writeKey(k, f)
		w.writeValue(v, stringify)
	}
	w.endObject()
}

func (w *JSONWriter) writeValue(v pcommon.Value, stringify bool) {
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		w.buf.WriteByte('"')
		w.buf.WriteByte('"')
	case pcommon.ValueTypeStr:
		w.writeString(v.Str())
	case pcommon.ValueTypeInt:
		w.buf.Write(strconv.AppendInt(w.buf.AvailableBuffer(), v.Int(), 10))
	case pcommon.ValueTypeDouble:
		d := v.Double()
		if math.IsInf(d, 0) || math.IsNaN(d) {
			w.buf.WriteString("0")
			return
		}
		// float64Val writes a float64, always including a radix point (e.g. 1.0 not 1)
		// to preserve type information for ES dynamic mapping.
		b := strconv.AppendFloat(w.buf.AvailableBuffer(), d, 'g', -1, 64)
		needDot := true
		expIdx := len(b)
		for i, c := range b {
			if c == 'e' {
				expIdx = i
				break
			}
			if c == '.' {
				needDot = false
				break
			}
		}
		if needDot {
			// Insert ".0" before exponent.
			// Copy tail for reuse below. Any write to buf would overwrite the
			// remaining b content, leading to a corruption in the tail part.
			// tail length is based on IEEE 754 max exponent of +308 or min exponent of -324, padded
			// for alignment.
			var tail [8]byte
			n := copy(tail[:], b[expIdx:])
			w.buf.Write(b[:expIdx])
			w.buf.WriteString(".0")
			w.buf.Write(tail[:n])
		} else {
			w.buf.Write(b)
		}
	case pcommon.ValueTypeBool:
		if v.Bool() {
			w.buf.WriteString("true")
		} else {
			w.buf.WriteString("false")
		}
	case pcommon.ValueTypeMap:
		if stringify {
			w.writeString(v.AsString())
		} else {
			w.startObject()
			f := true
			for k, v := range v.Map().All() {
				f = w.writeKey(k, f)
				w.writeValue(v, false)
			}
			w.endObject()
		}
	case pcommon.ValueTypeSlice:
		w.buf.WriteByte('[')
		f := true
		for _, v := range v.Slice().All() {
			if f {
				f = false
			} else {
				w.buf.WriteByte(',')
			}
			w.writeValue(v, stringify)
		}
		w.buf.WriteByte(']')
	case pcommon.ValueTypeBytes:
		w.buf.WriteByte('"')
		w.buf.Write(hex.AppendEncode(w.buf.AvailableBuffer(), v.Bytes().AsRaw()))
		w.buf.WriteByte('"')
	}
}
