// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package circuitbreakerextension

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func createTestCircuitBreaker(t *testing.T, cfg *Config) (*circuitBreakerExtension, error) {
	t.Helper()
	cb := newCircuitBreakerExtension(cfg, componenttest.NewNopTelemetrySettings()).(*circuitBreakerExtension)
	client, err := cfg.ToClient(t.Context(), nil, cb.ts)
	if err != nil {
		return nil, err
	}
	cb.client = client
	return cb, nil
}

func TestCircuitBreaker_Lifecycle(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("healthy"))
	}))

	cb, err := createTestCircuitBreaker(t, createDefaultConfig().(*Config))
	require.NoError(t, err)

	require.NoError(t, cb.Start(t.Context(), componenttest.NewNopHost()))
	assert.NotNil(t, cb.cancel)

	require.NoError(t, cb.Shutdown(t.Context()))
	assert.Nil(t, cb.cancel)

	t.Cleanup(mock.Close)
}

func TestCircuitBreaker_ProbeTransitions(t *testing.T) {
	var statusCode atomic.Int32
	var responseBody atomic.Pointer[string]

	matchBody := "aaa"
	mismatchBody := "zzz"

	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(statusCode.Load()))
		_, _ = w.Write([]byte(*responseBody.Load()))
	}))

	cfg := &Config{
		Endpoint:         mock.URL,
		StringMatch:      matchBody,
		FailureThreshold: 2,
		SuccessThreshold: 2,
	}

	cb, err := createTestCircuitBreaker(t, cfg)
	require.NoError(t, err)

	ctx := t.Context()

	statusCode.Store(http.StatusOK)
	responseBody.Store(&matchBody)
	cb.probe(ctx)
	assert.Equal(t, 1, cb.consecutiveSuccesses)
	assert.Equal(t, 0, cb.consecutiveFailures)
	assert.False(t, cb.MustRefuse())

	statusCode.Store(http.StatusInternalServerError)
	responseBody.Store(&matchBody)
	cb.probe(ctx)
	assert.Equal(t, 0, cb.consecutiveSuccesses)
	assert.Equal(t, 1, cb.consecutiveFailures)
	assert.False(t, cb.MustRefuse())

	statusCode.Store(http.StatusOK)
	responseBody.Store(&mismatchBody)
	cb.probe(ctx)
	assert.Equal(t, 0, cb.consecutiveSuccesses)
	assert.Equal(t, 2, cb.consecutiveFailures)
	assert.True(t, cb.MustRefuse())

	statusCode.Store(http.StatusOK)
	responseBody.Store(&matchBody)
	cb.probe(ctx)
	assert.Equal(t, 1, cb.consecutiveSuccesses)
	assert.Equal(t, 0, cb.consecutiveFailures)
	assert.True(t, cb.MustRefuse())

	statusCode.Store(http.StatusOK)
	responseBody.Store(&matchBody)
	cb.probe(ctx)
	assert.Equal(t, 2, cb.consecutiveSuccesses)
	assert.Equal(t, 0, cb.consecutiveFailures)
	assert.False(t, cb.MustRefuse())

	t.Cleanup(mock.Close)
}

func TestCircuitBreaker_HTTPMiddleware(t *testing.T) {
	isHandled := false
	cb := &circuitBreakerExtension{}

	handlerFunc, err := cb.GetHTTPHandler(t.Context())
	require.NoError(t, err)

	wrapped, err := handlerFunc(t.Context(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		isHandled = true
		w.WriteHeader(http.StatusOK)
	}))
	require.NoError(t, err)

	cb.isTripped.Store(0)
	req := httptest.NewRequest(http.MethodGet, "/dummy", http.NoBody)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	assert.True(t, isHandled)
	assert.Equal(t, http.StatusOK, rec.Code)

	isHandled = false
	cb.isTripped.Store(1)
	req = httptest.NewRequest(http.MethodGet, "/dummy", http.NoBody)
	rec = httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	assert.False(t, isHandled)
	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
}

type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	return nil, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	if p, ok := v.(*[]byte); ok {
		*p = append((*p)[:0], data...)
	}
	return nil
}

func (rawCodec) Name() string {
	return "raw"
}

func init() {
	encoding.RegisterCodec(rawCodec{})
}

func TestCircuitBreaker_GRPCInterceptors_Integration(t *testing.T) {
	cb := &circuitBreakerExtension{}

	opts, err := cb.GetGRPCServerOptions(t.Context())
	require.NoError(t, err)

	l := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer(opts...)

	sd := &grpc.ServiceDesc{
		ServiceName: "test.Service",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "TestMethod",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					var in []byte
					if e := dec(&in); e != nil {
						return nil, e
					}
					return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/test.Service/TestMethod"}, func(_ context.Context, _ any) (any, error) {
						return []byte("pong"), nil
					})
				},
			},
		},
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "TestStream",
				Handler:       func(_ any, _ grpc.ServerStream) error { return nil },
				ServerStreams: true,
				ClientStreams: true,
			},
		},
	}
	s.RegisterService(sd, struct{}{})

	go func() {
		_ = s.Serve(l)
	}()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return l.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype("raw")),
	)
	require.NoError(t, err)

	t.Run("UnaryInterceptor", func(t *testing.T) {
		method := fmt.Sprintf("/%s/%s", sd.ServiceName, sd.Methods[0].MethodName)
		args := []byte("ping")
		var reply []byte

		cb.isTripped.Store(1)
		e := conn.Invoke(t.Context(), method, args, &reply)
		require.Error(t, e)
		assert.Equal(t, codes.ResourceExhausted, status.Code(e))

		cb.isTripped.Store(0)
		require.NoError(t, conn.Invoke(t.Context(), method, args, &reply))
		assert.Equal(t, "pong", string(reply))
	})

	t.Run("StreamInterceptor", func(t *testing.T) {
		desc := &sd.Streams[0]
		method := fmt.Sprintf("/%s/%s", sd.ServiceName, sd.Streams[0].StreamName)
		var m []byte

		cb.isTripped.Store(1)
		cs, e := conn.NewStream(t.Context(), desc, method)
		require.NoError(t, e)
		e = cs.RecvMsg(&m)
		require.Error(t, e)
		assert.Equal(t, codes.ResourceExhausted, status.Code(e))

		cb.isTripped.Store(0)
		cs, e = conn.NewStream(t.Context(), desc, method)
		require.NoError(t, e)
		require.NoError(t, cs.CloseSend())
		assert.Equal(t, io.EOF, cs.RecvMsg(&m))
	})

	t.Cleanup(func() {
		s.Stop()
		_ = conn.Close()
	})
}
