// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package circuitbreakerextension // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/circuitbreakerextension"

import (
	"context"
	"errors"
	"time"

	"github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/circuitbreakerextension/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/extension"
)

func NewFactory() extension.Factory {
	return extension.NewFactory(
		metadata.Type,
		createDefaultConfig,
		createExtension,
		metadata.ExtensionStability,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint: "http://127.0.0.1:81/getstatus",
		TLS: configtls.ClientConfig{
			Insecure: true,
		},
		Timeout:          5 * time.Second,
		CheckInterval:    10 * time.Second,
		StringMatch:      "healthy",
		FailureThreshold: 4,
		SuccessThreshold: 2,
	}
}

func createExtension(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	config, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("configuration parsing error")
	}
	config.Keepalive = configoptional.Some(confighttp.KeepaliveClientConfig{MaxIdleConns: 1, MaxIdleConnsPerHost: 1})
	config.MaxConnsPerHost = 1
	return newCircuitBreakerExtension(config, set.TelemetrySettings), nil
}
