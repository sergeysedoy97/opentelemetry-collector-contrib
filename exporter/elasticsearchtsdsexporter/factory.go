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
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithMetrics(createMetrics, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	cc := confighttp.NewDefaultClientConfig()
	cc.Endpoint = "https://127.0.0.1:9200" // need for TestComponentLifecycle
	cc.Timeout = exporterhelper.NewDefaultTimeoutConfig().Timeout
	cc.Keepalive = configoptional.Some(confighttp.NewDefaultKeepaliveClientConfig())
	qc := exporterhelper.NewDefaultQueueConfig()
	// because pkg.exporterhelper.queueBatchEnabled is false
	qc.Batch = configoptional.Some(exporterhelper.BatchConfig{
		FlushTimeout: 200 * time.Millisecond,
		Sizer:        exporterhelper.RequestSizerTypeItems,
		MinSize:      8192,
	})
	return &Config{
		ClientConfig:     cc,
		BackOffConfig:    configretry.NewDefaultBackOffConfig(),
		QueueBatchConfig: configoptional.Some(qc),
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

	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		exp.pusher,
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
		exporterhelper.WithQueue(config.QueueBatchConfig),
		exporterhelper.WithRetry(config.BackOffConfig),
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: config.Timeout}),
	)
}
