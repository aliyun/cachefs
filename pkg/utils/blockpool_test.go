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

package utils

import (
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func assertBufferResident(t *testing.T, b []byte) {
	t.Helper()
	if len(b) == 0 {
		return
	}

	resident := make([]byte, (len(b)+pageSize-1)/pageSize)
	_, _, errno := unix.Syscall(
		unix.SYS_MINCORE,
		uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)),
		uintptr(unsafe.Pointer(&resident[0])),
	)
	if errno != 0 {
		t.Errorf("mincore: %s", errno)
		return
	}
	for i, value := range resident {
		if value&1 == 0 {
			t.Errorf("page %d is not resident", i)
			return
		}
	}
}

// freeTestPoolSlots returns ready blocks from a worker-free test pool to the OS.
func freeTestPoolSlots(p *blockPool) {
	p.mu.Lock()
	victims := append([]blockSlot{}, p.mmapS...)
	victims = append(victims, p.memfdS...)
	p.mmapS = nil
	p.memfdS = nil
	p.mu.Unlock()
	for _, s := range victims {
		p.freeSlot(s)
	}
}

// enableTestPool installs a worker-free reuse pool for one test. Production
// setup is process-lifetime, so tests own and release their local ready slots.
func enableTestPool(t *testing.T, blockSize, slots int) {
	t.Helper()
	p := &blockPool{
		blockSize: blockSize,
		cap:       slots,
		reuse:     true,
		warming:   true,
	}
	old := blkPool.Swap(p)
	require.True(t, GetBlockPoolStats().Enabled)
	t.Cleanup(func() {
		if blkPool.CompareAndSwap(p, old) {
			freeTestPoolSlots(p)
		}
	})
}

func TestPopulateBuffer(t *testing.T) {
	const blockSize = 8192

	t.Run("empty", func(t *testing.T) {
		if err := populateBuffer(nil); err != nil {
			t.Errorf("populateBuffer: %s", err)
		}
	})

	t.Run("mmap", func(t *testing.T) {
		buf, err := rawMalloc(blockSize)
		if err != nil {
			t.Errorf("rawMalloc: %s", err)
			return
		}
		defer func() {
			if err := doMfree(buf); err != nil {
				t.Errorf("doMfree: %s", err)
			}
		}()
		if err := populateBuffer(buf); err != nil {
			t.Errorf("populateBuffer: %s", err)
			return
		}
		assertBufferResident(t, buf)
	})

	t.Run("memfd", func(t *testing.T) {
		buf, fd, err := rawMemfdAlloc(blockSize)
		if err != nil {
			t.Errorf("rawMemfdAlloc: %s", err)
			return
		}
		defer func() {
			if err := doMemfdFree(buf, fd); err != nil {
				t.Errorf("doMemfdFree: %s", err)
			}
		}()
		if err := populateBuffer(buf); err != nil {
			t.Errorf("populateBuffer: %s", err)
			return
		}
		assertBufferResident(t, buf)
	})
}

func TestSetupBlockPool(t *testing.T) {
	old := blkPool.Swap(nil)
	t.Cleanup(func() {
		if p := blkPool.Swap(old); p != nil && p != old {
			freeTestPoolSlots(p)
		}
	})

	SetupBlockPool(nil)
	require.False(t, GetBlockPoolStats().Enabled)
	SetupBlockPool(&BlockPoolConfig{BlockSize: 4096, Reuse: true, Prefault: true})
	require.False(t, GetBlockPoolStats().Enabled)
	SetupBlockPool(&BlockPoolConfig{Slots: 4, Reuse: true, Prefault: true})
	require.False(t, GetBlockPoolStats().Enabled)
	SetupBlockPool(&BlockPoolConfig{BlockSize: 4096, Slots: 4})
	require.False(t, GetBlockPoolStats().Enabled)

	SetupBlockPool(&BlockPoolConfig{BlockSize: 4096, Slots: 4, Reuse: true})
	p := blkPool.Load()
	st := GetBlockPoolStats()
	require.True(t, st.Enabled)
	require.Equal(t, 4096, st.BlockSize)
	require.Equal(t, 4, st.Slots)
	require.Zero(t, st.Cached)
	require.Zero(t, st.CachedBytes)

	SetupBlockPool(nil)
	require.Same(t, p, blkPool.Load())
	SetupBlockPool(&BlockPoolConfig{BlockSize: 4096, Slots: 4})
	require.Same(t, p, blkPool.Load())
}

func TestBlockPoolSingleSlotPrefault(t *testing.T) {
	const blockSize = 4096

	old := blkPool.Swap(nil)
	t.Cleanup(func() {
		if p := blkPool.Swap(old); p != nil && p != old {
			freeTestPoolSlots(p)
		}
	})

	SetupBlockPool(&BlockPoolConfig{BlockSize: blockSize, Slots: 1, Reuse: true, Prefault: true})
	p := blkPool.Load()
	require.NotNil(t, p)
	require.NotNil(t, p.refillCh)
	require.Equal(t, 1, cap(p.refillCh))
	require.True(t, p.warming)

	requireEventually(t, func() bool {
		st := GetBlockPoolStats()
		return st.Cached == 1 && st.PrefaultBlocks >= 1 && st.PrefaultBytes >= blockSize &&
			st.ReallocCount >= 1 && st.ReallocBytes >= blockSize
	})
}

// TestBlockPoolPushPop drives the two backing stacks directly while enforcing
// one shared capacity budget.
func TestBlockPoolPushPop(t *testing.T) {
	p := &blockPool{blockSize: 8, cap: 2, reuse: true, warming: true}
	a, b, c := make([]byte, 8), make([]byte, 8), make([]byte, 8)

	require.True(t, p.push(a, -1))
	require.True(t, p.push(b, -1))
	require.False(t, p.push(c, 7), "mmap and memfd stacks share capacity")

	buf, fd, ok := p.pop(false)
	require.True(t, ok)
	require.Equal(t, &b[0], &buf[0])
	require.Equal(t, -1, fd)
	buf, _, ok = p.pop(false)
	require.True(t, ok)
	require.Equal(t, &a[0], &buf[0])
	_, _, ok = p.pop(false)
	require.False(t, ok)
	_, _, ok = p.pop(true)
	require.False(t, ok)

	require.EqualValues(t, 2, p.reuseCount)
	require.EqualValues(t, 16, p.reuseBytes)
	require.Zero(t, p.reallocCount)
	require.Zero(t, p.reallocBytes)
}

// TestMallocMfreeReuse checks the pool guards in Malloc/Mfree: a freed block of
// the pooled size comes straight back, keeps its RSS counted, and a block of any
// other size bypasses the pool.
func TestMallocMfreeReuse(t *testing.T) {
	const bs = 8192
	enableTestPool(t, bs, 2)

	b, err := Malloc(bs)
	require.NoError(t, err)
	first := &b[0]
	before := AllocMemory()
	require.EqualValues(t, 1, GetBlockPoolStats().ReallocCount)
	require.EqualValues(t, bs, GetBlockPoolStats().ReallocBytes)

	require.NoError(t, Mfree(b))
	// Pooled, so the block stays resident and counted.
	require.Equal(t, 1, GetBlockPoolStats().Cached)
	require.Equal(t, before, AllocMemory())

	b2, err := Malloc(bs)
	require.NoError(t, err)
	require.Equal(t, first, &b2[0], "expected the pooled block back")
	require.Equal(t, bs, len(b2))
	require.Equal(t, 0, GetBlockPoolStats().Cached)
	require.Equal(t, before, AllocMemory())
	require.NoError(t, Mfree(b2))

	// A different size is not poolable: it is really freed, dropping `used`.
	other, err := Malloc(bs / 2)
	require.NoError(t, err)
	before = AllocMemory()
	require.NoError(t, Mfree(other))
	require.Equal(t, 1, GetBlockPoolStats().Cached)
	require.Equal(t, before-int64(bs/2), AllocMemory())
}

// TestMallocMfreeShortenedBlock checks what decides poolability (cap) and what is
// pooled (the whole block) stay in step: freeing a shortened view of a block must
// not hand the next allocation a short buffer.
func TestMallocMfreeShortenedBlock(t *testing.T) {
	const bs = 8192
	enableTestPool(t, bs, 1)

	b, err := Malloc(bs)
	require.NoError(t, err)
	require.NoError(t, Mfree(b[:1]))
	require.Equal(t, 1, GetBlockPoolStats().Cached)

	b2, err := Malloc(bs)
	require.NoError(t, err)
	require.Equal(t, &b[0], &b2[0], "expected the pooled block back")
	require.Equal(t, bs, len(b2))
	require.NoError(t, Mfree(b2))
}

// TestMallocMfreePoolFull checks a free that finds the pool full really frees the
// block instead of dropping it on the floor.
func TestMallocMfreePoolFull(t *testing.T) {
	const bs = 8192
	enableTestPool(t, bs, 1)

	b1, err := Malloc(bs)
	require.NoError(t, err)
	b2, err := Malloc(bs)
	require.NoError(t, err)

	require.NoError(t, Mfree(b1))
	require.Equal(t, 1, GetBlockPoolStats().Cached)

	before := AllocMemory()
	require.NoError(t, Mfree(b2))
	// Still one block cached, and the rejected one gave its memory back.
	require.Equal(t, 1, GetBlockPoolStats().Cached)
	require.Equal(t, before-int64(bs), AllocMemory())
}

// TestMemfdPoolReuse checks a memfd block with no outstanding export is pooled
// with its fd and mapping intact, and handed back by the next allocation. The
// freed Memfd is detached from the block: any further use of the dead handle
// (a second Free, a late ExportFd) fails loudly instead of touching a block
// the pool now owns.
func TestMemfdPoolReuse(t *testing.T) {
	const bs = 8192
	enableTestPool(t, bs, 2)

	m, err := MemfdAlloc("first", bs)
	require.NoError(t, err)
	fd, data := m.Fd, &m.Data()[0]

	require.NoError(t, m.Free())
	require.Equal(t, 1, GetBlockPoolStats().Cached)

	// The dead handle carries no fd, no data, and no way to reach the block.
	require.Equal(t, -1, m.Fd)
	require.Nil(t, m.Data())
	expFd, err := m.ExportFd()
	require.ErrorIs(t, err, unix.EBADF)
	require.Equal(t, -1, expFd)
	require.EqualValues(t, 0, m.exportCount.Load(), "a failed export must not leave a count behind")
	require.ErrorIs(t, m.Free(), unix.EBADF)
	require.Equal(t, 1, GetBlockPoolStats().Cached, "the pooled block must be untouched")

	m2, err := MemfdAlloc("second", bs)
	require.NoError(t, err)
	require.Equal(t, fd, m2.Fd, "expected the pooled fd back")
	require.Equal(t, data, &m2.Data()[0], "expected the pooled mapping back")
	require.Equal(t, "second", m2.Name, "the name belongs to the allocation, not the slot")
	require.EqualValues(t, 0, m2.exportCount.Load(), "a reused block starts with no exports")

	// The fd is still usable, i.e. the pool really kept it open.
	var stat unix.Stat_t
	require.NoError(t, unix.Fstat(m2.Fd, &stat))
	require.EqualValues(t, bs, stat.Size)

	require.NoError(t, m2.Free())
}

// TestMemfdExportGatesPooling is the core reuse-safety test: a block with an
// export that was never proven gone must never return to the pool, and one whose
// export was given back must.
func TestMemfdExportGatesPooling(t *testing.T) {
	const bs = 8192
	enableTestPool(t, bs, 2)

	t.Run("outstanding export is never pooled", func(t *testing.T) {
		m, err := MemfdAlloc("exported", bs)
		require.NoError(t, err)
		fd, err := m.ExportFd()
		require.NoError(t, err)
		require.EqualValues(t, 1, m.exportCount.Load())
		// Closing our own copy says nothing about the copy the client holds.
		require.NoError(t, unix.Close(fd))

		require.NoError(t, m.Free())
		require.Equal(t, 0, GetBlockPoolStats().Cached)
		// Really freed, so the block's own state is cleared too.
		require.Equal(t, -1, m.Fd)
		require.Nil(t, m.Data())
	})

	t.Run("released export is pooled", func(t *testing.T) {
		m, err := MemfdAlloc("released", bs)
		require.NoError(t, err)
		fd, err := m.ExportFd()
		require.NoError(t, err)
		require.NoError(t, unix.Close(fd))
		m.UnexportFd()
		require.EqualValues(t, 0, m.exportCount.Load())

		require.NoError(t, m.Free())
		require.Equal(t, 1, GetBlockPoolStats().Cached)
	})

	t.Run("one unproven export among several poisons the block", func(t *testing.T) {
		m, err := MemfdAlloc("mixed", bs)
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			fd, err := m.ExportFd()
			require.NoError(t, err)
			require.NoError(t, unix.Close(fd))
		}
		// Two authoritative releases, one ambiguous: the count never reaches 0.
		m.UnexportFd()
		m.UnexportFd()
		require.EqualValues(t, 1, m.exportCount.Load())

		cached := GetBlockPoolStats().Cached
		require.NoError(t, m.Free())
		require.Equal(t, cached, GetBlockPoolStats().Cached)
	})
}

// TestMemfdUnexportBelowZeroPoisons checks an over-release cannot be cancelled
// out: a later legitimate export brings the count back to zero, and zero is the
// value that permits pooling, so only the poison keeps the block out.
func TestMemfdUnexportBelowZeroPoisons(t *testing.T) {
	const bs = 8192
	enableTestPool(t, bs, 2)

	m, err := MemfdAlloc("overreleased", bs)
	require.NoError(t, err)

	// One release more than there were exports: the accounting is now broken.
	m.UnexportFd()
	require.EqualValues(t, -1, m.exportCount.Load())
	require.True(t, m.poisoned.Load())

	// A real export takes the count back to zero while its copy is outstanding.
	fd, err := m.ExportFd()
	require.NoError(t, err)
	require.NoError(t, unix.Close(fd))
	require.EqualValues(t, 0, m.exportCount.Load())

	cached := GetBlockPoolStats().Cached
	require.NoError(t, m.Free())
	require.Equal(t, cached, GetBlockPoolStats().Cached,
		"a block whose export count is untrustworthy must never be pooled")
	// Really freed, so the block's own state is cleared too.
	require.Equal(t, -1, m.Fd)
	require.Nil(t, m.Data())
}

// TestMemfdExportFdFailureKeepsCount checks the dup/count pair stays consistent
// when the dup fails: a failed export must not leave a phantom count behind,
// which would poison the block forever.
func TestMemfdExportFdFailureKeepsCount(t *testing.T) {
	m := &Memfd{Name: "closed", Fd: -1}
	fd, err := m.ExportFd()
	require.Error(t, err)
	require.Equal(t, -1, fd)
	require.EqualValues(t, 0, m.exportCount.Load())
}

func TestGetBlockPoolStats(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		old := blkPool.Swap(nil)
		t.Cleanup(func() { blkPool.Store(old) })
		require.Equal(t, BlockPoolStats{}, GetBlockPoolStats())
	})

	t.Run("prefault counters", func(t *testing.T) {
		p := &blockPool{
			blockSize:             4096,
			cap:                   2,
			prefaultSkipBlocks:    3,
			prefaultSkipBytes:     3 * 4096,
			prefaultDiscardBlocks: 2,
			prefaultDiscardBytes:  2 * 4096,
		}
		old := blkPool.Swap(p)
		t.Cleanup(func() { blkPool.Store(old) })

		st := GetBlockPoolStats()
		require.EqualValues(t, 3, st.PrefaultSkipBlocks)
		require.EqualValues(t, 3*4096, st.PrefaultSkipBytes)
		require.EqualValues(t, 2, st.PrefaultDiscardBlocks)
		require.EqualValues(t, 2*4096, st.PrefaultDiscardBytes)
	})
}

func TestBlockPoolPrefault(t *testing.T) {
	const (
		blockSize = 8192
		slots     = 2
	)

	old := blkPool.Swap(nil)
	t.Cleanup(func() {
		if p := blkPool.Swap(old); p != nil && p != old {
			for _, s := range p.expire(time.Now().Add(2 * time.Second)) {
				p.freeSlot(s)
			}
		}
	})

	SetupBlockPool(&BlockPoolConfig{BlockSize: blockSize, Slots: slots, Timeout: time.Second, Reuse: true, Prefault: true})
	p := blkPool.Load()

	requireEventually(t, func() bool {
		st := GetBlockPoolStats()
		return st.Cached == slots && st.PrefaultBlocks >= slots && st.PrefaultBytes >= int64(slots*blockSize) &&
			st.ReallocCount >= slots && st.ReallocBytes >= int64(slots*blockSize)
	})

	b, err := Malloc(blockSize)
	require.NoError(t, err)
	st := GetBlockPoolStats()
	require.EqualValues(t, 1, st.ReuseCount)
	require.GreaterOrEqual(t, st.Cached, slots-1)
	require.LessOrEqual(t, st.Cached, slots)
	require.NoError(t, Mfree(b))
	requireEventually(t, func() bool { return GetBlockPoolStats().Cached == slots })

	for _, s := range p.expire(time.Now().Add(2 * time.Second)) {
		p.freeSlot(s)
	}
	requireEventually(t, func() bool { return GetBlockPoolStats().Cached == 0 })

	b, err = Malloc(blockSize)
	require.NoError(t, err)
	require.NoError(t, Mfree(b))
	requireEventually(t, func() bool { return GetBlockPoolStats().Cached == slots })
}

func TestBlockPoolPrefaultLowWaterHit(t *testing.T) {
	const (
		blockSize = 8192
		slots     = 2
	)
	t.Setenv("CFS_BUFFER_PREFAULT_THRESHOLD", "")

	old := blkPool.Swap(nil)
	t.Cleanup(func() {
		if p := blkPool.Swap(old); p != nil && p != old {
			for _, s := range p.expire(time.Now().Add(2 * time.Second)) {
				p.freeSlot(s)
			}
		}
	})

	SetupBlockPool(&BlockPoolConfig{BlockSize: blockSize, Slots: slots, Timeout: time.Second, Prefault: true})
	requireEventually(t, func() bool { return GetBlockPoolStats().Cached == slots })
	before := GetBlockPoolStats()

	b, err := Malloc(blockSize)
	require.NoError(t, err)
	require.NoError(t, Mfree(b))

	requireEventually(t, func() bool {
		after := GetBlockPoolStats()
		return after.Cached == slots && after.PrefaultBlocks == before.PrefaultBlocks+1 &&
			after.PrefaultBytes == before.PrefaultBytes+blockSize &&
			after.ReallocCount == before.ReallocCount+1 && after.ReallocBytes == before.ReallocBytes+blockSize
	})
}

func TestBlockPoolPopRefillThreshold(t *testing.T) {
	t.Run("above low-water mark", func(t *testing.T) {
		p := &blockPool{
			blockSize:       8,
			cap:             3,
			refillThreshold: 1,
			warming:         true,
			refillCh:        make(chan struct{}, 3),
			mmapS: []blockSlot{
				{buf: make([]byte, 8), fd: -1},
				{buf: make([]byte, 8), fd: -1},
				{buf: make([]byte, 8), fd: -1},
			},
		}

		_, _, ok := p.pop(false)
		require.True(t, ok)
		p.mu.Lock()
		defer p.mu.Unlock()
		require.Equal(t, 2, p.poolSizeLocked())
		require.Zero(t, p.refilling)
		require.Empty(t, p.refillCh)
	})

	t.Run("low-water hit", func(t *testing.T) {
		p := &blockPool{
			blockSize:       8,
			cap:             3,
			refillThreshold: 1,
			warming:         true,
			refillCh:        make(chan struct{}, 3),
			mmapS: []blockSlot{
				{buf: make([]byte, 8), fd: -1},
				{buf: make([]byte, 8), fd: -1},
			},
		}

		_, _, ok := p.pop(false)
		require.True(t, ok)
		p.mu.Lock()
		defer p.mu.Unlock()
		require.Equal(t, 1, p.poolSizeLocked())
		require.Equal(t, 2, p.refilling)
		require.Len(t, p.refillCh, 2)
	})

	t.Run("final hit", func(t *testing.T) {
		p := &blockPool{
			blockSize:       8,
			cap:             3,
			refillThreshold: 1,
			warming:         true,
			refillCh:        make(chan struct{}, 3),
			mmapS:           []blockSlot{{buf: make([]byte, 8), fd: -1}},
		}

		_, _, ok := p.pop(false)
		require.True(t, ok)
		p.mu.Lock()
		defer p.mu.Unlock()
		require.Zero(t, p.poolSizeLocked())
		require.Equal(t, 3, p.refilling)
		require.Len(t, p.refillCh, 3)
	})

	t.Run("backing miss", func(t *testing.T) {
		p := &blockPool{
			blockSize:       8,
			cap:             3,
			refillThreshold: 1,
			warming:         true,
			refillCh:        make(chan struct{}, 3),
			mmapS:           []blockSlot{{buf: make([]byte, 8), fd: -1}},
		}

		_, _, ok := p.pop(true)
		require.False(t, ok)
		p.mu.Lock()
		defer p.mu.Unlock()
		require.Equal(t, 1, p.poolSizeLocked())
		require.Equal(t, 2, p.refilling)
		require.Len(t, p.refillCh, 2)
	})
}

func TestBlockPoolRefillThresholdEnv(t *testing.T) {
	old := blkPool.Swap(nil)
	t.Cleanup(func() {
		blkPool.Store(old)
	})

	t.Run("cap-derived default", func(t *testing.T) {
		t.Setenv("CFS_BUFFER_PREFAULT_THRESHOLD", "")
		SetupBlockPool(&BlockPoolConfig{BlockSize: 8, Slots: 1, Reuse: true})
		require.Equal(t, 0, blkPool.Load().refillThreshold)

		SetupBlockPool(&BlockPoolConfig{BlockSize: 8, Slots: 3, Reuse: true})
		require.Equal(t, 1, blkPool.Load().refillThreshold)
	})

	t.Run("configured threshold controls refill", func(t *testing.T) {
		t.Setenv("CFS_BUFFER_PREFAULT_THRESHOLD", "2")
		SetupBlockPool(&BlockPoolConfig{BlockSize: 8, Slots: 4, Reuse: true})
		p := blkPool.Load()
		require.Equal(t, 2, p.refillThreshold)

		p.warming = true
		p.refillCh = make(chan struct{}, p.cap)
		p.mmapS = []blockSlot{
			{buf: make([]byte, 8), fd: -1},
			{buf: make([]byte, 8), fd: -1},
			{buf: make([]byte, 8), fd: -1},
			{buf: make([]byte, 8), fd: -1},
		}
		_, _, ok := p.pop(false)
		require.True(t, ok)
		require.Zero(t, p.refilling)

		_, _, ok = p.pop(false)
		require.True(t, ok)
		require.Equal(t, 2, p.refilling)
		require.Len(t, p.refillCh, 2)
	})

	t.Run("zero and invalid values", func(t *testing.T) {
		t.Setenv("CFS_BUFFER_PREFAULT_THRESHOLD", "0")
		SetupBlockPool(&BlockPoolConfig{BlockSize: 8, Slots: 3, Reuse: true})
		require.Zero(t, blkPool.Load().refillThreshold)

		t.Setenv("CFS_BUFFER_PREFAULT_THRESHOLD", "invalid")
		SetupBlockPool(&BlockPoolConfig{BlockSize: 8, Slots: 3, Reuse: true})
		require.Equal(t, 1, blkPool.Load().refillThreshold)

		t.Setenv("CFS_BUFFER_PREFAULT_THRESHOLD", "-1")
		SetupBlockPool(&BlockPoolConfig{BlockSize: 8, Slots: 3, Reuse: true})
		require.Equal(t, 1, blkPool.Load().refillThreshold)
	})
}

func TestBlockPoolAddRefills(t *testing.T) {
	p := &blockPool{cap: 4, warming: true, refillCh: make(chan struct{}, 2)}

	p.mu.Lock()
	p.addRefillsLocked()
	p.mu.Unlock()

	require.Equal(t, 2, p.refilling)
	require.Len(t, p.refillCh, 2)
}

func TestBlockPoolPushReclaimsQueuedToken(t *testing.T) {
	p := &blockPool{blockSize: 8, cap: 4, reuse: true, warming: true, refillCh: make(chan struct{}, 2), refilling: 2}
	p.refillCh <- struct{}{}
	p.refillCh <- struct{}{}

	require.True(t, p.push(make([]byte, 8), -1))
	require.Equal(t, 1, p.refilling)
	require.Len(t, p.refillCh, 1)
	require.Equal(t, 1, p.poolSizeLocked())
}

func TestBlockPoolExpireDrainsQueuedTokens(t *testing.T) {
	p := &blockPool{
		cap:         4,
		timeout:     time.Second,
		warming:     true,
		lastRequest: time.Now().Add(-2 * time.Second),
		refillCh:    make(chan struct{}, 2),
		refilling:   2,
	}
	p.refillCh <- struct{}{}
	p.refillCh <- struct{}{}

	require.Empty(t, p.expire(time.Now()))
	require.False(t, p.warming)
	require.Zero(t, p.refilling)
	require.Empty(t, p.refillCh)
}

func TestBlockPoolDormantRelease(t *testing.T) {
	t.Run("next allocation reuses late return", func(t *testing.T) {
		p := &blockPool{blockSize: 8, cap: 1, timeout: time.Second, reuse: true}
		b, err := rawMalloc(8)
		require.NoError(t, err)
		require.True(t, p.push(b, -1))
		require.False(t, p.warming)
		require.Equal(t, 1, p.poolSizeLocked())

		got, _, ok := p.pop(false)
		require.True(t, ok)
		require.Equal(t, &b[0], &got[0])
		require.True(t, p.warming)
		require.NoError(t, doMfree(got))
	})

	t.Run("reaper frees unused late return", func(t *testing.T) {
		p := &blockPool{blockSize: 8, cap: 1, timeout: time.Second, reuse: true}
		b, err := rawMalloc(8)
		require.NoError(t, err)
		require.True(t, p.push(b, -1))

		victims := p.expire(time.Now())
		require.Len(t, victims, 1)
		require.False(t, p.warming)
		require.Zero(t, p.poolSizeLocked())
		p.freeSlot(victims[0])
	})
}

func requireEventually(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Fail(t, "condition did not become true")
}
