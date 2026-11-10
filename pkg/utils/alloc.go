/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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
	"fmt"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

var (
	used int64

	thpShmemAdvise bool = false
	thpMadvise     bool = false
)

// Detect THP policy and check if they're madvise, so we could do madvise only if policy is madvise
func SetupMadvise() {
	thpShmemPolicy, err := GetThpShmemPolicy()
	if err == nil {
		thpShmemAdvise = thpShmemPolicy == "advise"
		if thpShmemAdvise {
			logger.Info("THP Shmem policy is advise, will do madvise")
		}
	}
	thpPolicy, err := GetThpPolicy()
	if err == nil {
		thpMadvise = thpPolicy == "madvise"
		if thpMadvise {
			logger.Info("THP policy is madvise, will do madvise")
		}
	}
}

// Alloc returns size bytes memory from Go heap.
func Alloc(size int) []byte {
	zeros := powerOf2(size)
	b := *pools[zeros].Get().(*[]byte)
	if cap(b) < size {
		panic(fmt.Sprintf("%d < %d", cap(b), size))
	}
	atomic.AddInt64(&used, int64(cap(b)))
	return b[:size]
}

// Free returns memory to Go heap.
func Free(b []byte) {
	// buf could be zero length
	atomic.AddInt64(&used, -int64(cap(b)))
	pools[powerOf2(cap(b))].Put(&b)
}

// Malloc returns size bytes memory from private & anonymous mmap, doesn't allocate from go heap and
// put less pressure on GC. Block-sized allocations are served from the block pool when it holds a
// free block.
func Malloc(size int) ([]byte, error) {
	if p := blkPool.Load(); p != nil && size == p.blockSize {
		if b, _, ok := p.pop(false); ok {
			// The block is already resident and still counted in `used`, so both
			// stay untouched on a pool hit.
			return b, nil
		}
		b, err := rawMalloc(size)
		if err == nil {
			p.recordRealloc()
		}
		return b, err
	}
	return rawMalloc(size)
}

func rawMalloc(size int) ([]byte, error) {
	b, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANONYMOUS|unix.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	if thpMadvise {
		if err = unix.Madvise(b, unix.MADV_HUGEPAGE); err != nil {
			logger.Warnf("Madvise mmap memory: %s", err)
		}
	}
	atomic.AddInt64(&used, int64(cap(b)))
	return b, nil
}

// Mfree returns memory to the block pool if it fits and has room, otherwise to the OS.
func Mfree(b []byte) error {
	if p := blkPool.Load(); p != nil && cap(b) == p.blockSize && p.push(b, -1) {
		// Pooled blocks keep their RSS, so `used` stays as it is.
		return nil
	}
	return doMfree(b)
}

// doMfree returns memory to OS.
func doMfree(b []byte) error {
	atomic.AddInt64(&used, -int64(cap(b)))
	return unix.Munmap(b)
}

// AllocMemory returns the allocated memory. Pooled blocks are still counted: they
// are genuinely resident, since the pool never drops their pages.
func AllocMemory() int64 {
	return atomic.LoadInt64(&used)
}

var pools []*sync.Pool

// PowerOf2 returns the smallest power of 2 that is >= s
func powerOf2(s int) int {
	if s <= 0 {
		return 0
	}
	// Find position of the most significant bit (MSB)
	return bits.Len(uint(s - 1))
}

func init() {
	pools = make([]*sync.Pool, 30) // 1 - 1G
	for i := 0; i < 30; i++ {
		func(bits int) {
			pools[i] = &sync.Pool{
				New: func() interface{} {
					b := make([]byte, 1<<bits)
					return &b
				},
			}
		}(i)
	}
	go func() {
		for {
			time.Sleep(time.Minute * 10)
			runtime.GC()
		}
	}()
}
