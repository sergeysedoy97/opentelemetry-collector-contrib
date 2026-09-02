// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	confighttp.ClientConfig   `mapstructure:",squash"`
	QueueBatchConfig          configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	Endpoints                 []string `mapstructure:"endpoints"`
	Pipeline                  string   `mapstructure:"pipeline"`
}

func (c *Config) Validate() error {
	if err := c.ClientConfig.Validate(); err != nil {
		return err
	}
	if err := c.BackOffConfig.Validate(); err != nil {
		return err
	}
	if c.Endpoint == "" && len(c.Endpoints) == 0 {
		return errors.New("endpoint or endpoints must be specified")
	}
	if c.Timeout <= time.Second {
		return errors.New("timeout must be at least 1 second")
	}
	if c.Compression.IsCompressed() && c.Compression != configcompression.TypeGzip {
		return errors.New("only gzip compression is supported")
	}

	return nil
}
