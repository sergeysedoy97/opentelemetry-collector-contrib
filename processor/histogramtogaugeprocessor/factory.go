// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package histogramtogaugeprocessor // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/processor/histogramtogaugeprocessor"

import (
	"context"
	"errors"

	"github.com/sergeysedoy97/opentelemetry-collector-contrib/processor/histogramtogaugeprocessor/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithMetrics(createMetrics, metadata.MetricsStability))
}

func createDefaultConfig() component.Config {
	return &Config{
		BucketLabel:       "le",
		MetricSuffix:      "_bucket",
		CountMetricSuffix: "_count",
		SumMetricSuffix:   "_sum",
		MinMetricSuffix:   "_min",
		MaxMetricSuffix:   "_max",
	}
}

func createMetrics(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	config, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("configuration parsing error")
	}

	metricsProcessor := newProcessor(config, set.Logger)

	return processorhelper.NewMetrics(
		ctx,
		set,
		cfg,
		nextConsumer,
		metricsProcessor.processMetrics,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}
