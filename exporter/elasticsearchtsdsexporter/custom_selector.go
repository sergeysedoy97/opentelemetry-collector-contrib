// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchtsdsexporter // import "github.com/sergeysedoy97/opentelemetry-collector-contrib/exporter/elasticsearchtsdsexporter"

import (
	"errors"
	"math/rand/v2"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"gitlab.rip/platform/go-starter/v3/oneconf"
	"go.uber.org/zap"
)

const maxWeight = 100

type CustomSelectorState struct {
	conns  []*elastictransport.Connection
	seq    []int
	seqLen int
}

type CustomSelector struct {
	index       atomic.Uint64
	state       atomic.Pointer[CustomSelectorState]
	mutex       sync.Mutex
	logger      *zap.Logger
	unsubscribe oneconf.UnsubscribeFunc
}

func (cs *CustomSelector) Select([]*elastictransport.Connection) (*elastictransport.Connection, error) {
	state := cs.state.Load()
	if state == nil || state.seqLen == 0 {
		return nil, errors.New("no active connections available")
	}
	return state.conns[state.seq[cs.index.Add(1)%uint64(state.seqLen)]], nil
}

func (cs *CustomSelector) setupConf(v string) bool {
	cs.logger.Info("Got the value from 01.conf", zap.String("value", v))
	if v == "" {
		cs.logger.Warn("The value is empty")
		return false
	}
	var sumWeight int
	var weights []int
	var conns []*elastictransport.Connection
	endpoints := strings.SplitSeq(v, "\n")
	for endpoint := range endpoints {
		if endpoint == "" {
			continue
		}
		cs.logger.Info("Parsing", zap.String("endpoint", endpoint))
		strURL, strWeight, found := strings.Cut(endpoint, "|")
		if !found {
			cs.logger.Error("The '|' separator was not found")
			return false
		}
		parsedURL, err := url.Parse(strURL)
		if err != nil {
			cs.logger.Error("The URL parser returned error", zap.Error(err))
			return false
		}
		parsedWeight, err := strconv.ParseUint(strWeight, 10, 8)
		if err != nil {
			cs.logger.Error("The uint parser returned error", zap.Error(err))
			return false
		}
		if parsedWeight > maxWeight {
			cs.logger.Warn("The parsed weight is more than 100, set it to 100")
			parsedWeight = maxWeight
		}
		weight := int(parsedWeight)
		sumWeight += weight
		weights = append(weights, weight)
		conns = append(conns, &elastictransport.Connection{URL: parsedURL})
	}
	if sumWeight == 0 {
		cs.logger.Error("The sum of parsed connection weights cannot be zero")
		return false
	}
	count := len(weights)
	rand.Shuffle(count, func(i, j int) {
		weights[i], weights[j] = weights[j], weights[i]
		conns[i], conns[j] = conns[j], conns[i]
	})
	if sumWeight == maxWeight*count {
		cs.setupRR(conns)
	} else {
		cs.setupWRR(conns, weights, sumWeight)
	}
	return true
}

func (cs *CustomSelector) setupWRR(conns []*elastictransport.Connection, weights []int, sumWeight int) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	seqLen := 0
	seq := make([]int, seqLen, sumWeight)
	for seqLen < sumWeight {
		for i := range weights {
			if weights[i] == 0 {
				continue
			}
			weights[i]--
			seq = append(seq, i)
			seqLen++
		}
	}
	cs.state.Store(&CustomSelectorState{conns: conns, seq: seq, seqLen: seqLen})
	cs.index.Store(0)
}

func (cs *CustomSelector) setupRR(conns []*elastictransport.Connection) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	seqLen := len(conns)
	seq := make([]int, seqLen)
	for i := range seq {
		seq[i] = i
	}
	cs.state.Store(&CustomSelectorState{conns: conns, seq: seq, seqLen: seqLen})
	cs.index.Store(0)
}

func (cs *CustomSelector) close() {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if cs.unsubscribe != nil {
		cs.unsubscribe()
		cs.unsubscribe = nil
	}
}
