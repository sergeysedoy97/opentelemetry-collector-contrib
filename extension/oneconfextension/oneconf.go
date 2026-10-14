// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package oneconfextension // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/oneconfextension"

import (
	"context"

	"github.com/sergeysedoy97/opentelemetry-collector-contrib/extension/oneconfextension/internal/metadata"
	"gitlab.rip/platform/go-starter/v3/oneconf"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

type Extension interface {
	extension.Extension
	GetConf() *oneconf.Conf
}

func Get(host component.Host) Extension {
	for id, ext := range host.GetExtensions() {
		if id.Type() == metadata.Type {
			if target, ok := ext.(Extension); ok {
				return target
			}
		}
	}
	return nil
}

type oneconfextension struct {
	config *Config
	ts     component.TelemetrySettings
	conf   *oneconf.Conf
	cancel context.CancelFunc
}

func newOneConfExtension(config *Config, ts component.TelemetrySettings) extension.Extension {
	return &oneconfextension{
		config: config,
		ts:     ts,
	}
}

func (oc *oneconfextension) Start(_ context.Context, _ component.Host) error {
	var err error
	ctx, cancel := context.WithCancel(context.Background())
	oc.cancel = cancel
	logger := oc.ts.Logger.Sugar()
	oc.conf, err = oneconf.NewConf(
		oneconf.WithConfAppName(oc.config.AppName),
		oneconf.WithConfContext(ctx),
		oneconf.WithConfGrpcAddress(oc.config.GrpcAddress),
		oneconf.WithConfLogger(logger),
	)
	if err != nil {
		return err
	}
	return nil
}

func (oc *oneconfextension) Shutdown(context.Context) error {
	if oc.cancel != nil {
		oc.cancel()
		oc.cancel = nil
	}
	if oc.conf != nil {
		oc.conf.Close()
		oc.conf = nil
	}
	return nil
}

func (oc *oneconfextension) GetConf() *oneconf.Conf {
	return oc.conf
}
