// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package circuitbreakerextension // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/circuitbreakerextension"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
)

type Config struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	CheckInterval           time.Duration `mapstructure:"check_interval"`
	StringMatch             string        `mapstructure:"string_match"`
	FailureThreshold        int           `mapstructure:"failure_threshold"`
	SuccessThreshold        int           `mapstructure:"success_threshold"`
}

func (c *Config) Validate() error {
	if err := c.ClientConfig.Validate(); err != nil {
		return err
	}
	if c.Endpoint == "" {
		return errors.New("endpoint must be specified")
	}
	if c.Timeout <= time.Second {
		return errors.New("timeout must be at least 1 second")
	}
	if c.CheckInterval <= time.Second {
		return errors.New("check_interval must be at least 1 second")
	}
	if c.StringMatch == "" {
		return errors.New("string_match must be specified")
	}
	if c.FailureThreshold <= 0 {
		return errors.New("failure_threshold must be at least 1")
	}
	if c.SuccessThreshold <= 0 {
		return errors.New("success_threshold must be at least 1")
	}
	return nil
}
