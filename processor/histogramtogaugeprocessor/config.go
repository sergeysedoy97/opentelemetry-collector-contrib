// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package histogramtogaugeprocessor // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/processor/histogramtogaugeprocessor"

type Config struct {
	BucketLabel       string `mapstructure:"bucket_label"`
	MetricSuffix      string `mapstructure:"metric_suffix"`
	CountMetricSuffix string `mapstructure:"count_metric_suffix"`
	SumMetricSuffix   string `mapstructure:"sum_metric_suffix"`
	MinMetricSuffix   string `mapstructure:"min_metric_suffix"`
	MaxMetricSuffix   string `mapstructure:"max_metric_suffix"`
}
