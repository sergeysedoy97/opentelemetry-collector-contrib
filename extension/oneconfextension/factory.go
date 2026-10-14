// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package oneconfextension // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/oneconfextension"

import (
	"context"
	"errors"

	"github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/oneconfextension/internal/metadata"
	"gitlab.rip/platform/go-starter/v3/oneconf"
	"go.opentelemetry.io/collector/component"
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
		AppName:     "otelcol",
		GrpcAddress: "dns:///" + oneconf.ProdOneConfHost + ":" + oneconf.DefaultOneConfPort,
	}
}

func createExtension(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	config, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("configuration parsing error")
	}
	return newOneConfExtension(config, set.TelemetrySettings), nil
}
