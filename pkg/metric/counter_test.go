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
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestRegistry creates a fresh CounterRegistry for use in a single test case.
func newTestRegistry(t *testing.T) *CounterRegistry {
	t.Helper()
	return NewCounterRegistry()
}

func TestAtomicCounter_Inc(t *testing.T) {
	reg := newTestRegistry(t)
	c := reg.NewCounter(prometheus.CounterOpts{Name: "test_inc", Help: "test"})

	c.Inc()
	assert.Equal(t, float64(1), c.Load())

	c.Inc()
	c.Inc()
	assert.Equal(t, float64(3), c.Load())
}

func TestAtomicCounter_Add(t *testing.T) {
	tests := []struct {
		name     string
		preInc   bool // call Inc() before Add
		adds     []float64
		expected float64
	}{
		{
			name:     "single add",
			adds:     []float64{5},
			expected: 5,
		},
		{
			name:     "multiple adds accumulate",
			adds:     []float64{10, 20, 7},
			expected: 37,
		},
		{
			name:     "add zero has no effect",
			adds:     []float64{0},
			expected: 0,
		},
		{
			name:     "inc then add - GetCounter reflects combined result",
			preInc:   true,
			adds:     []float64{9},
			expected: 10,
		},
		{
			name:     "add fractional value preserved",
			adds:     []float64{2.5, 1.5},
			expected: 4.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := newTestRegistry(t)
			name := "test_add_" + tt.name
			c := reg.NewCounter(prometheus.CounterOpts{Name: name, Help: name})
			if tt.preInc {
				c.Inc()
			}
			for _, v := range tt.adds {
				c.Add(v)
			}
			assert.Equal(t, tt.expected, c.Load())

			// Verify GetCounter returns the same counter pointer.
			gotC, err := reg.GetCounter(name)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, gotC.Load())
		})
	}
}

func TestAtomicCounter_LoadInitialZero(t *testing.T) {
	reg := newTestRegistry(t)
	c := reg.NewCounter(prometheus.CounterOpts{Name: "test_load_zero", Help: "test"})
	assert.Equal(t, float64(0), c.Load())
}

func TestAtomicCounter_PrometheusCounterAlsoUpdated(t *testing.T) {
	promReg := prometheus.NewRegistry()
	reg := newTestRegistry(t)
	c := reg.NewCounter(prometheus.CounterOpts{Name: "test_prom_sync", Help: "test"})
	promReg.MustRegister(c)

	c.Inc()
	c.Add(4)

	mf, err := promReg.Gather()
	require.NoError(t, err)
	require.Len(t, mf, 1)
	require.Len(t, mf[0].GetMetric(), 1)
	assert.Equal(t, float64(5), mf[0].GetMetric()[0].GetCounter().GetValue())
}

func TestCounterRegistry_GetCounter(t *testing.T) {
	t.Run("registered counter is returned", func(t *testing.T) {
		reg := newTestRegistry(t)
		c := reg.NewCounter(prometheus.CounterOpts{Name: "test_get_registered", Help: "test"})

		got, err := reg.GetCounter("test_get_registered")
		require.NoError(t, err)
		assert.Same(t, c, got)
	})

	t.Run("unregistered name returns error", func(t *testing.T) {
		reg := newTestRegistry(t)
		_, err := reg.GetCounter("test_get_nonexistent_xyz")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "test_get_nonexistent_xyz")
	})

	t.Run("unregistered name returns nil and error", func(t *testing.T) {
		reg := newTestRegistry(t)
		c, err := reg.GetCounter("test_get_value_nonexistent_xyz")
		assert.Error(t, err)
		assert.Nil(t, c)
	})
}

func TestCounterRegistry_DuplicateRegisterWarns(t *testing.T) {
	reg := newTestRegistry(t)
	c1 := NewCounter(prometheus.CounterOpts{Name: "dup_a", Help: "first"})
	c2 := NewCounter(prometheus.CounterOpts{Name: "dup_a", Help: "second"})

	reg.Register("dup_a", c1)
	c1.Inc()

	reg.Register("dup_a", c2)
	c2.Add(5)

	gotC, err := reg.GetCounter("dup_a")
	require.NoError(t, err)
	assert.Equal(t, float64(5), gotC.Load())
}

func TestCounterRegistry_Isolation(t *testing.T) {
	reg1 := NewCounterRegistry()
	reg2 := NewCounterRegistry()

	c1 := reg1.NewCounter(prometheus.CounterOpts{Name: "shared_name", Help: "reg1"})
	c2 := reg2.NewCounter(prometheus.CounterOpts{Name: "shared_name", Help: "reg2"})

	c1.Add(10)
	c2.Add(20)

	c1C, _ := reg1.GetCounter("shared_name")
	c2C, _ := reg2.GetCounter("shared_name")
	assert.Equal(t, float64(10), c1C.Load())
	assert.Equal(t, float64(20), c2C.Load())
}

func TestAtomicCounter_ConcurrentInc(t *testing.T) {
	reg := newTestRegistry(t)
	c := reg.NewCounter(prometheus.CounterOpts{Name: "test_concurrent", Help: "test"})
	const goroutines = 100
	const incsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range incsPerGoroutine {
				c.Inc()
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, float64(goroutines*incsPerGoroutine), c.Load())
}
