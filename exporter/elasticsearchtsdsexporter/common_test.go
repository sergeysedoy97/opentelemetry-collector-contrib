// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/extension/xextension/storage"
)

type testHost struct {
	ext map[component.ID]component.Component
}

func (h *testHost) GetExtensions() map[component.ID]component.Component {
	return h.ext
}

type storageExtension struct {
	component.StartFunc
	component.ShutdownFunc
}

func (*storageExtension) GetClient(context.Context, component.Kind, component.ID, string) (storage.Client, error) {
	return &storageClient{data: make(map[string][]byte)}, nil
}

type storageClient struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (c *storageClient) Get(_ context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data[key], nil
}

func (c *storageClient) Set(_ context.Context, key string, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
	return nil
}

func (c *storageClient) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
	return nil
}

func (c *storageClient) Batch(_ context.Context, ops ...*storage.Operation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, op := range ops {
		switch op.Type {
		case storage.Get:
			op.Value = c.data[op.Key]
		case storage.Set:
			c.data[op.Key] = op.Value
		case storage.Delete:
			delete(c.data, op.Key)
		}
	}
	return nil
}

func (*storageClient) Close(context.Context) error { return nil }

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
