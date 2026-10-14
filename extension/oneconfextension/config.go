// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package oneconfextension // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/oneconfextension"

import (
	"errors"
)

type Config struct {
	AppName     string `mapstructure:"app_name"`
	GrpcAddress string `mapstructure:"grpc_address"`
}

func (c *Config) Validate() error {
	if c.AppName == "" {
		return errors.New("app_name must be specified")
	}
	if c.GrpcAddress == "" {
		return errors.New("grpc_address must be specified")
	}
	return nil
}
