// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"context"
	"errors"
	"time"

	"github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(metadata.Type, createDefaultConfig, exporter.WithMetrics(createMetrics, metadata.MetricsStability))
}

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint:         "https://127.0.0.1:9200",
		TLS:              configtls.ClientConfig{CipherSuites: []string{"TLS_AES_128_GCM_SHA256"}, CurvePreferences: []string{"P256"}, MinVersion: "1.3"},
		ReadBufferSize:   1024 * 1024,
		WriteBufferSize:  1024 * 1024,
		Timeout:          5 * time.Minute,
		Headers:          configopaque.MapList{configopaque.Pair{Name: "Content-Type", Value: "application/x-ndjson"}, configopaque.Pair{Name: "Content-Encoding", Value: "gzip"}},
		Keepalive:        configoptional.Some(confighttp.KeepaliveClientConfig{IdleConnTimeout: time.Minute}),
		Enabled:          true,
		InitialInterval:  30 * time.Second,
		MaxElapsedTime:   5 * time.Hour,
		MaxInterval:      2 * time.Minute,
		Multiplier:       2,
		QueueBatchConfig: metadata.NewDefaultSendingQueueConfig(),
	}
}

func createMetrics(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	config, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("configuration parsing error")
	}

	exp, err := NewExporter(config, set)
	if err != nil {
		return nil, err
	}

	return xexporterhelper.NewMetricsRequest(
		ctx,
		set,
		exp.requestConverter,
		exp.requestConsume,
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
		xexporterhelper.WithQueueBatch(config.QueueBatchConfig, xexporterhelper.QueueBatchSettings{Encoding: Encoding{}}),
		exporterhelper.WithRetry(config.BackOffConfig),
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: config.Timeout}),
	)
}
