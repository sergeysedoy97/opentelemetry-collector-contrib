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

	"gitlab.rip/platform/go-starter/v3/oneconf"
)

const maxWeight = 100

type CustomSelectorState struct {
	urls   []string
	seq    []int
	seqLen int
}

type CustomSelector struct {
	index       atomic.Uint64
	state       atomic.Pointer[CustomSelectorState]
	mutex       sync.Mutex
	unsubscribe oneconf.UnsubscribeFunc
}

func (cs *CustomSelector) Select(path string) string {
	state := cs.state.Load()
	if state == nil || state.seqLen == 0 {
		return ""
	}
	return state.urls[state.seq[cs.index.Add(1)%uint64(state.seqLen)]] + path
}

func (cs *CustomSelector) SetupConf(v string) error {
	if v == "" {
		return errors.New("The value cannot be empty")
	}
	var sumWeight int
	var weights []int
	var urls []string
	for endpoint := range strings.SplitSeq(v, "\n") {
		if endpoint == "" || endpoint[0] == '#' {
			continue
		}
		strURL, strWeight, found := strings.Cut(endpoint, "|")
		if !found {
			return errors.New("The endpoint weight must be defined")
		}
		parsedURL, err := url.Parse(strings.TrimRight(strURL, "/"))
		if err != nil {
			return err
		}
		parsedWeight, err := strconv.ParseUint(strWeight, 10, 8)
		if err != nil {
			return err
		}
		if parsedWeight > maxWeight {
			return errors.New("The endpoint weight must be between 0 and 100")
		}
		weight := int(parsedWeight)
		sumWeight += weight
		weights = append(weights, weight)
		urls = append(urls, parsedURL.String())
	}
	count := len(weights)
	if count == 0 {
		return errors.New("The endpoints count is zero")
	}
	if sumWeight == 0 {
		return errors.New("The endpoints weight sum cannot be zero")
	}
	rand.Shuffle(count, func(i, j int) {
		weights[i], weights[j] = weights[j], weights[i]
		urls[i], urls[j] = urls[j], urls[i]
	})
	if sumWeight == maxWeight*count {
		cs.SetupRR(urls)
	} else {
		cs.SetupWRR(urls, weights, sumWeight)
	}
	return nil
}

func (cs *CustomSelector) SetupWRR(urls []string, weights []int, sumWeight int) {
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
	cs.state.Store(&CustomSelectorState{urls: urls, seq: seq, seqLen: seqLen})
	cs.index.Store(0)
}

func (cs *CustomSelector) SetupRR(urls []string) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	seqLen := len(urls)
	seq := make([]int, seqLen)
	for i := range seq {
		seq[i] = i
	}
	cs.state.Store(&CustomSelectorState{urls: urls, seq: seq, seqLen: seqLen})
	cs.index.Store(0)
}

func (cs *CustomSelector) Unsubscribe() {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if cs.unsubscribe != nil {
		cs.unsubscribe()
		cs.unsubscribe = nil
	}
}
