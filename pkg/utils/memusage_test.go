/*
 * JuiceFS, Copyright 2021 Juicedata, Inc.
 * Copyright 2025 Alibaba Cloud, Inc. or its affiliates.
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

package utils

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func approxEqual(a, b uint64, tolerance float64) bool {
	diff := float64(a) - float64(b)
	if diff < 0 {
		diff = -diff
	}
	return diff/float64(b) <= tolerance
}

func TestMemUsage(t *testing.T) {
	virt, rss := MemoryUsage()
	if virt < (1<<20) || rss < (1<<20) || rss > (100<<20) {
		t.Errorf("invalid memory usage: virt %d, rss %d", virt, rss)
		return
	}

	sizes := []int{10, 100, 512, 1024, 2048} // MB
	tolerance := 0.1

	for _, mb := range sizes {
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
		_, baseRSS := MemoryUsage()

		n := mb * 1024 * 1024
		buf, err := Malloc(n)
		assert.NoError(t, err)
		for i := 0; i < len(buf); i += 4096 {
			buf[i] = 0
		}

		time.Sleep(100 * time.Millisecond)
		_, rssAfter := MemoryUsage()

		delta := rssAfter - baseRSS
		expected := uint64(n)

		if mb <= 100 {
			tolerance = 0.3 // for small allocations, allow larger error
		}

		ok := approxEqual(delta, expected, tolerance)
		assert.Truef(t, ok, "[%dMB mmap] expected delta≈%dB, got %dB", mb, expected, delta)

		assert.NoError(t, Mfree(buf))
	}
}
