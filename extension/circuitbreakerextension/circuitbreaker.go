// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package circuitbreakerextension // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/circuitbreakerextension"

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionmiddleware"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxReadBytes = 1024 * 64

var (
	_ extensionmiddleware.GRPCServer = (*circuitBreakerExtension)(nil)
	_ extensionmiddleware.HTTPServer = (*circuitBreakerExtension)(nil)
)

type circuitBreakerExtension struct {
	config               *Config
	ts                   component.TelemetrySettings
	client               *http.Client
	cancel               context.CancelFunc
	isTripped            atomic.Int32
	consecutiveFailures  int
	consecutiveSuccesses int
	wg                   sync.WaitGroup
}

func newCircuitBreakerExtension(config *Config, ts component.TelemetrySettings) extension.Extension {
	return &circuitBreakerExtension{
		config: config,
		ts:     ts,
	}
}

func (cb *circuitBreakerExtension) Start(ctx context.Context, host component.Host) error {
	client, err := cb.config.ToClient(ctx, host.GetExtensions(), cb.ts)
	if err != nil {
		return err
	}
	cb.client = client
	probeCtx, cancel := context.WithCancel(context.Background())
	cb.cancel = cancel
	cb.wg.Go(func() {
		cb.probeLoop(probeCtx)
	})
	return nil
}

func (cb *circuitBreakerExtension) Shutdown(context.Context) error {
	if cb.cancel != nil {
		cb.cancel()
		cb.cancel = nil
	}
	cb.wg.Wait()
	return nil
}

func (cb *circuitBreakerExtension) probeLoop(ctx context.Context) {
	ticker := time.NewTicker(cb.config.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cb.probe(ctx)
		}
	}
}

func (cb *circuitBreakerExtension) probe(ctx context.Context) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cb.config.Endpoint, http.NoBody)
	if err != nil {
		cb.ts.Logger.Error("Failed to create probe request", zap.Error(err))
		cb.handleFailure()
		return
	}
	resp, err := cb.client.Do(req)
	if err != nil {
		cb.ts.Logger.Warn("Failed to send probe request", zap.Error(err))
		cb.handleFailure()
		return
	}
	defer resp.Body.Close()
	limitedReader := io.LimitReader(resp.Body, maxReadBytes)
	bodyBytes, err := io.ReadAll(limitedReader)
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	if err != nil {
		cb.ts.Logger.Error("Failed to read probe response body", zap.Error(err))
		cb.handleFailure()
		return
	}
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices && strings.Contains(string(bodyBytes), cb.config.StringMatch) {
		cb.handleSuccess()
	} else {
		cb.handleFailure()
	}
}

func (cb *circuitBreakerExtension) handleFailure() {
	if cb.consecutiveFailures < cb.config.FailureThreshold {
		cb.consecutiveSuccesses = 0
		cb.consecutiveFailures++
		cb.ts.Logger.Info("Probe failed", zap.Int("consecutive_failures", cb.consecutiveFailures))
		if cb.consecutiveFailures == cb.config.FailureThreshold {
			cb.tripCircuit()
		}
	}
}

func (cb *circuitBreakerExtension) handleSuccess() {
	if cb.consecutiveSuccesses < cb.config.SuccessThreshold {
		cb.consecutiveFailures = 0
		cb.consecutiveSuccesses++
		cb.ts.Logger.Info("Probe succeeded (recovering)", zap.Int("consecutive_successes", cb.consecutiveSuccesses))
		if cb.consecutiveSuccesses == cb.config.SuccessThreshold {
			cb.resetCircuit()
		}
	}
}

func (cb *circuitBreakerExtension) tripCircuit() {
	if cb.isTripped.CompareAndSwap(0, 1) {
		cb.ts.Logger.Warn("Circuit Breaker TRIPPED")
	}
}

func (cb *circuitBreakerExtension) resetCircuit() {
	if cb.isTripped.CompareAndSwap(1, 0) {
		cb.ts.Logger.Warn("Circuit Breaker RESET")
	}
}

func (cb *circuitBreakerExtension) MustRefuse() bool {
	return cb.isTripped.Load() == 1
}

func (cb *circuitBreakerExtension) GetHTTPHandler(_ context.Context) (extensionmiddleware.WrapHTTPHandlerFunc, error) {
	return cb.wrapHTTPHandler, nil
}

func (cb *circuitBreakerExtension) wrapHTTPHandler(_ context.Context, base http.Handler) (http.Handler, error) {
	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		if cb.MustRefuse() {
			http.Error(resp, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
			return
		}
		base.ServeHTTP(resp, req)
	}), nil
}

func (cb *circuitBreakerExtension) GetGRPCServerOptions(_ context.Context) ([]grpc.ServerOption, error) {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
				if cb.MustRefuse() {
					return nil, status.Errorf(codes.ResourceExhausted, "RESOURCE_EXHAUSTED")
				}
				return handler(ctx, req)
			},
		),
		grpc.ChainStreamInterceptor(
			func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				if cb.MustRefuse() {
					return status.Errorf(codes.ResourceExhausted, "RESOURCE_EXHAUSTED")
				}
				return handler(srv, ss)
			},
		),
	}, nil
}
