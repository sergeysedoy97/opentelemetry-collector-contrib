// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	confighttp.ClientConfig   `mapstructure:",squash"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	QueueBatchConfig          exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`
	Pipeline                  string                          `mapstructure:"pipeline"`
}

func (c *Config) Validate() error {
	if err := c.ClientConfig.Validate(); err != nil {
		return err
	}
	if err := c.BackOffConfig.Validate(); err != nil {
		return err
	}
	if err := c.QueueBatchConfig.Validate(); err != nil {
		return err
	}
	if !c.QueueBatchConfig.Batch.HasValue() {
		return errors.New("sending_queue::batch must be specified")
	}
	if err := c.QueueBatchConfig.Batch.Get().Validate(); err != nil {
		return err
	}
	if err := c.QueueBatchConfig.Batch.Get().Partition.Validate(); err != nil {
		return err
	}
	if c.Endpoint == "" {
		return errors.New("endpoint must be specified")
	}
	if c.Timeout <= time.Second {
		return errors.New("timeout must be at least 1 second")
	}
	if c.Compression.IsCompressed() {
		return errors.New("compression must be disabled")
	}
	if c.QueueBatchConfig.Sizer != exporterhelper.RequestSizerTypeRequests {
		return errors.New("sending_queue::sizer must be set to requests")
	}
	if c.QueueBatchConfig.Batch.Get().Sizer != exporterhelper.RequestSizerTypeBytes {
		return errors.New("sending_queue::batch::sizer must be set to bytes")
	}
	if c.QueueBatchConfig.Batch.Get().MaxSize == 0 {
		return errors.New("sending_queue::batch::max_size must be specified")
	}

	return nil
}

func (c *Config) Unmarshal(conf *confmap.Conf) error {
	return conf.Unmarshal(c)
}
