/*
 * Copyright 2026 Alibaba Cloud, Inc. or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metric

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
)

var logger = utils.GetLogger("cachefs")

// CounterRegistry is a registry of named AtomicCounters.
//
// Duplicate registration by name logs a warning and the last registration wins.
type CounterRegistry struct {
	mu       sync.RWMutex
	counters map[string]*AtomicCounter
}

// Creates an empty CounterRegistry.
func NewCounterRegistry() *CounterRegistry {
	return &CounterRegistry{counters: make(map[string]*AtomicCounter)}
}

// Adds c under name. If name is already registered a warning is
// logged and the new counter overwrites the old one.
func (r *CounterRegistry) Register(name string, c *AtomicCounter) {
	r.mu.Lock()
	if _, exists := r.counters[name]; exists {
		logger.Warnf("CounterRegistry: counter %q already registered, overwriting", name)
	}
	r.counters[name] = c
	r.mu.Unlock()
}

// Creates an AtomicCounter and registers it under opts.Name.
func (r *CounterRegistry) NewCounter(opts prometheus.CounterOpts) *AtomicCounter {
	c := NewCounter(opts)
	r.Register(opts.Name, c)
	return c
}

// Returns the AtomicCounter registered under name, or an error if not found.
func (r *CounterRegistry) GetCounter(name string) (*AtomicCounter, error) {
	r.mu.RLock()
	c, ok := r.counters[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("counter %q not registered", name)
	}
	return c, nil
}

// AtomicCounter wraps prometheus.Counter with a parallel float64 atomic value
// for zero-overhead reads without going through the Prometheus Gather path.
// It fully implements prometheus.Counter, so existing registration and usage
// code requires no changes.
//
// The float64 value is stored as its IEEE-754 bit pattern in an atomic.Uint64;
// updates use a CAS loop to preserve fractional precision.
type AtomicCounter struct {
	prometheus.Counter
	val atomic.Uint64 // stores float64 bits via math.Float64bits
}

// Increments both the prometheus counter and the atomic value.
func (c *AtomicCounter) Inc() {
	c.Counter.Inc()
	c.atomicAddFloat64(1.0)
}

// Adds v to both the prometheus counter and the atomic value.
func (c *AtomicCounter) Add(v float64) {
	c.Counter.Add(v)
	c.atomicAddFloat64(v)
}

// Returns the current value via atomic read, without Gather overhead.
func (c *AtomicCounter) Load() float64 {
	return math.Float64frombits(c.val.Load())
}

func (c *AtomicCounter) atomicAddFloat64(delta float64) {
	for {
		old := c.val.Load()
		newBits := math.Float64bits(math.Float64frombits(old) + delta)
		if c.val.CompareAndSwap(old, newBits) {
			return
		}
	}
}

// Creates a standalone AtomicCounter without registering it in any
// CounterRegistry. Use CounterRegistry.NewCounter when per-name lookup
// is needed.
func NewCounter(opts prometheus.CounterOpts) *AtomicCounter {
	return &AtomicCounter{Counter: prometheus.NewCounter(opts)}
}
