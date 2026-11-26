// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type testHandler struct{}

func (testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	io.Copy(io.Discard, r.Body) //nolint:errcheck
	w.WriteHeader(http.StatusOK)
}

func TestElasticsearchBulkRequests_MergeSplit(t *testing.T) {
	t.Run("nil wreq returns self", func(t *testing.T) {
		req := &ElasticsearchBulkRequests{buf: []byte("foo"), cnt: 1}

		res, err := req.MergeSplit(t.Context(), 10, exporterhelper.RequestSizerTypeBytes, nil)
		require.NoError(t, err)
		require.Len(t, res, 1)

		assert.Equal(t, req, res[0])
	})

	t.Run("merge successful when combined size <= maxSize", func(t *testing.T) {
		req1 := &ElasticsearchBulkRequests{buf: []byte("foo"), cnt: 1}
		req2 := &ElasticsearchBulkRequests{buf: []byte("bar"), cnt: 1}

		// 3 + 3 = 6 <= 6
		res, err := req1.MergeSplit(t.Context(), 6, exporterhelper.RequestSizerTypeBytes, req2)
		require.NoError(t, err)
		require.Len(t, res, 1)

		assert.Equal(t, req1, res[0])
		assert.Equal(t, []byte("foobar"), req1.buf)
		assert.Equal(t, 2, req1.cnt)
	})

	t.Run("split required when combined size > maxSize", func(t *testing.T) {
		req1 := &ElasticsearchBulkRequests{buf: []byte("foo"), cnt: 1}
		req2 := &ElasticsearchBulkRequests{buf: []byte("bar"), cnt: 1}

		// 3 + 3 = 6 > 5
		res, err := req1.MergeSplit(t.Context(), 5, exporterhelper.RequestSizerTypeBytes, req2)
		require.NoError(t, err)
		require.Len(t, res, 2)

		assert.Equal(t, req1, res[0])
		assert.Equal(t, req2, res[1])
	})
}
