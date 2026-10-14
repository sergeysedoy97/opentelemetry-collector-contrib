// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchprocessor // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/processor/elasticsearchprocessor"

type Config struct {
	HistogramMetricAttribute            string `mapstructure:"histogram_metric_attribute"`
	HistogramMetricSuffix               string `mapstructure:"histogram_metric_suffix"`
	ExponentialHistogramMetricAttribute string `mapstructure:"exponential_histogram_metric_attribute"`
	ExponentialHistogramMetricSuffix    string `mapstructure:"exponential_histogram_metric_suffix"`
	SummaryMetricAttribute              string `mapstructure:"summary_metric_attribute"`
	SummaryMetricSuffix                 string `mapstructure:"summary_metric_suffix"`
}
