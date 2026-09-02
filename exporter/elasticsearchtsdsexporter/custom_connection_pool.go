// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"context"
	"net/url"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
)

type CustomConnectionPool struct {
	conns    []*elastictransport.Connection
	selector elastictransport.Selector
}

func (ccp *CustomConnectionPool) Next() (*elastictransport.Connection, error) {
	return ccp.selector.Select(nil)
}

func (*CustomConnectionPool) OnSuccess(*elastictransport.Connection) error { return nil }
func (*CustomConnectionPool) OnFailure(*elastictransport.Connection) error { return nil }

func (ccp *CustomConnectionPool) URLs() []*url.URL {
	var conns []*elastictransport.Connection
	if cs, ok := ccp.selector.(*CustomSelector); ok {
		if state := cs.state.Load(); state != nil {
			conns = state.conns
		}
	} else {
		conns = ccp.conns
	}
	urls := make([]*url.URL, 0, len(conns))
	for _, conn := range conns {
		urls = append(urls, conn.URL)
	}
	return urls
}

func (ccp *CustomConnectionPool) Close(context.Context) error {
	if cs, ok := ccp.selector.(*CustomSelector); ok {
		cs.close()
	}
	return nil
}

func (*CustomConnectionPool) ConcurrentSafe() {}
