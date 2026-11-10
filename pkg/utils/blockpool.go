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
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// blkPool is the process-global block pool, nil when disabled.
//
// It's an atomic pointer rather than a plain global: SetupBlockPool runs at startup, but other
// goroutines may already be allocating through Malloc/MemfdAlloc by then, and a plain pointer write
// racing those reads is a data race. Load() is a single atomic read on the allocation hot path.
var blkPool atomic.Pointer[blockPool]

const gcBlocksInterval = 10 * time.Second

// blockSlot is one pooled block. fd < 0 marks an mmap block; otherwise fd is
// the memfd backing that travels with the buffer.
type blockSlot struct {
	buf []byte
	fd  int
}

// blockPool keeps a bounded process-global reserve of resident full blocks, so
// a pool hit avoids a munmap/mmap/fault cycle.
//
// mmap and memfd blocks are not interchangeable: mmap is a bare buffer while a
// memfd slot must retain its fd. They therefore use separate mutex-guarded LIFO
// stacks, while sharing one capacity budget.
type blockPool struct {
	mu        sync.Mutex
	blockSize int           // only allocations of exactly this size use the pool
	cap       int           // shared slot count for ready blocks
	timeout   time.Duration // request inactivity before the warm reserve is released
	reuse     bool

	mmapS  []blockSlot // mmap stack (slot.fd < 0)
	memfdS []blockSlot // memfd stack

	// Prefault state is protected by mu: warming marks the active retention period; lastRequest
	// extends it; lastPrefaultWarn rate-limits failure warnings; refillCh holds admitted work and
	// refilling counts queued and active tokens.
	warming          bool
	lastRequest      time.Time
	lastPrefaultWarn time.Time
	refillCh         chan struct{}
	refilling        int // count of inflight prefaults
	refillThreshold  int // threshold that triggers prefault work at pop time

	// All counters are protected by mu. realloc records only successful fresh
	// mappings, whether they came from a request miss or a prefault worker.
	reuseCount            int64
	reuseBytes            int64
	reallocCount          int64
	reallocBytes          int64
	prefaultBlocks        int64
	prefaultBytes         int64
	prefaultFail          int64
	prefaultSkipBlocks    int64
	prefaultSkipBytes     int64
	prefaultDiscardBlocks int64
	prefaultDiscardBytes  int64
}

// BlockPoolConfig configures the process-global block pool.
type BlockPoolConfig struct {
	BlockSize int
	Slots     int
	Timeout   time.Duration
	Reuse     bool
	Prefault  bool
	Memfd     bool
}

// SetupBlockPool installs the process-global block pool at startup. Slots caps
// the shared ready-block capacity across mmap and memfd.
//
// A nil config, non-positive BlockSize or Slots, or both capabilities disabled
// leaves the current pool unchanged. SetupBlockPool must be called once.
func SetupBlockPool(conf *BlockPoolConfig) {
	if conf == nil || conf.BlockSize <= 0 || conf.Slots <= 0 || (!conf.Reuse && !conf.Prefault) {
		if conf == nil {
			logger.Info("Block pool disabled (no configuration)")
		} else {
			logger.Infof("Block pool disabled (block size %d, slots %d, reuse %v, prefault %v)",
				conf.BlockSize, conf.Slots, conf.Reuse, conf.Prefault)
		}
		return
	}

	p := &blockPool{
		blockSize: conf.BlockSize,
		cap:       conf.Slots,
		timeout:   conf.Timeout,
		reuse:     conf.Reuse,
	}
	p.refillThreshold = GetEnvInt("CFS_BUFFER_PREFAULT_THRESHOLD", min(1, p.cap/2), 0)

	if conf.Prefault {
		p.refillCh = make(chan struct{}, p.cap)
		p.mu.Lock()
		p.warming = true
		p.lastRequest = time.Now()
		p.addRefillsLocked()
		p.mu.Unlock()
		for range GetEnvInt("CFS_BUFFER_PREFAULT_WORKERS", 4, 1) {
			go p.refillWorker(conf.Memfd)
		}
	}

	if conf.Timeout > 0 {
		go p.gcBlocks()
	}

	blkPool.Store(p)
	backing := "mmap"
	if conf.Memfd {
		backing = "memfd"
	}
	logger.Infof("Block pool enabled: %d slots of %d bytes, timeout %s, reuse=%v, prefault=%v, backing=%s",
		conf.Slots, conf.BlockSize, conf.Timeout, conf.Reuse, conf.Prefault, backing)
}

// poolSizeLocked returns ready blocks only; queued and active refill tokens are excluded.
func (p *blockPool) poolSizeLocked() int {
	return len(p.mmapS) + len(p.memfdS)
}

// addRefillsLocked issues nonblocking refill tokens until the ready pool and
// admitted work reach the configured prefault target.
func (p *blockPool) addRefillsLocked() {
	for p.warming && p.poolSizeLocked()+p.refilling < p.cap {
		select {
		case p.refillCh <- struct{}{}:
			p.refilling++
		default:
			return
		}
	}
}

// push returns a reusable block to the pool. fd < 0 pushes an mmap block;
// otherwise the memfd stays open and travels with the slot. A false result
// leaves the caller responsible for a real free.
//
// A block returned after the retention period stays reusable until the next
// reaper tick, but does not restart preallocation. Poolability is determined by
// the backing mapping, so shortened views are stored at their full capacity and
// a later pop still returns a full block.
func (p *blockPool) push(buf []byte, fd int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.reuse || p.poolSizeLocked() >= p.cap {
		return false
	}
	st := &p.mmapS
	if fd >= 0 {
		st = &p.memfdS
	}
	*st = append(*st, blockSlot{buf: buf[:cap(buf)], fd: fd})

	// take out a refill token from refill channel, as we've just put back a block
	select {
	case <-p.refillCh:
		p.refilling--
	default:
	}
	return true
}

// recordRealloc records a successful request-path raw mapping. Failed mmap or
// memfd allocations deliberately do not contribute to realloc metrics.
func (p *blockPool) recordRealloc() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reallocCount++
	p.reallocBytes += int64(p.blockSize)
}

// pop takes the newest block from the requested backing stack. As the allocation
// request path, it restarts or extends the warm period and admits refill work;
// releases use push and do not wake a dormant pool. Pool hits are counted under
// mu; successful misses are counted by the raw allocation caller.
func (p *blockPool) pop(memfd bool) (buf []byte, fd int, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer func() {
		// Admit refills on a miss or when this pop reaches the configured low-water
		// threshold. While the pool is healthy, returned blocks (push) satisfy demand and
		// cancel queued tokens instead, so churn doesn't start allocate-then-discard
		// cycles.
		if buf == nil || p.poolSizeLocked() <= p.refillThreshold {
			p.addRefillsLocked()
		}
	}()
	p.warming = true
	p.lastRequest = time.Now()

	st := &p.mmapS
	if memfd {
		st = &p.memfdS
	}
	n := len(*st)
	if n == 0 {
		return nil, -1, false
	}
	s := (*st)[n-1]
	(*st)[n-1] = blockSlot{}
	*st = (*st)[:n-1]
	p.reuseCount++
	p.reuseBytes += int64(p.blockSize)
	return s.buf, s.fd, true
}

// refillWorker consumes admitted tokens. It allocates and populates without mu
// so request-path push and pop never wait on those slow operations.
func (p *blockPool) refillWorker(memfd bool) {
	for {
		<-p.refillCh
		p.mu.Lock()
		if !p.warming || p.poolSizeLocked() >= p.cap {
			p.refilling--
			p.mu.Unlock()
			continue
		}
		p.mu.Unlock()

		// there's still free slots for pre-allocation
		var (
			buf []byte
			fd  = -1
			err error
		)
		if memfd {
			buf, fd, err = rawMemfdAlloc(p.blockSize)
		} else {
			buf, err = rawMalloc(p.blockSize)
		}
		slot := blockSlot{buf: buf, fd: fd}
		if err == nil {
			p.mu.Lock()
			p.reallocCount++
			p.reallocBytes += int64(p.blockSize)

			// check free slot again and avoid unnecessary populating, as we released
			// the lock, new slots might be added by another worker or pushed back.
			if !p.warming || p.poolSizeLocked() >= p.cap {
				p.prefaultSkipBlocks++
				p.prefaultSkipBytes += int64(p.blockSize)
				p.refilling--
				p.mu.Unlock()
				p.freeSlot(slot)
				continue
			}
			p.mu.Unlock()
			err = populateBuffer(slot.buf)
		}
		if err != nil {
			p.mu.Lock()
			p.refilling--
			p.prefaultFail++
			warn := time.Since(p.lastPrefaultWarn) >= time.Minute
			if warn {
				p.lastPrefaultWarn = time.Now()
			}
			p.mu.Unlock()
			if slot.buf != nil {
				p.freeSlot(slot)
			}
			if warn {
				logger.Warnf("Block pool prefault failed: %s", err)
			}
			continue
		}

		keep := false
		p.mu.Lock()
		p.refilling--
		p.prefaultBlocks++
		p.prefaultBytes += int64(p.blockSize)
		if p.warming && p.poolSizeLocked() < p.cap {
			st := &p.mmapS
			if fd >= 0 {
				st = &p.memfdS
			}
			*st = append(*st, slot)
			keep = true
		} else {
			p.prefaultDiscardBlocks++
			p.prefaultDiscardBytes += int64(p.blockSize)
		}
		p.mu.Unlock()
		if !keep {
			p.freeSlot(slot)
		}
	}
}

// gcBlocks returns idle warm blocks to the OS after a retention interval.
// expire detaches slots under mu, but munmap and close run below without it so
// allocation, release, and worker coordination never wait on those syscalls.
func (p *blockPool) gcBlocks() {
	ticker := time.NewTicker(gcBlocksInterval)
	defer ticker.Stop()
	for now := range ticker.C {
		for _, s := range p.expire(now) {
			p.freeSlot(s)
		}
	}
}

func (p *blockPool) expire(now time.Time) []blockSlot {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.timeout == 0 {
		return nil
	}
	if p.warming {
		if now.Sub(p.lastRequest) < p.timeout {
			return nil
		}
		p.warming = false

	drain:
		// cancel all pending refill as allocation has expired
		for {
			select {
			case <-p.refillCh:
				p.refilling--
			default:
				break drain
			}
		}
	}
	if p.poolSizeLocked() == 0 {
		return nil
	}

	// Late returns stay available while dormant, then leave on the next tick.
	victims := append([]blockSlot{}, p.mmapS...)
	victims = append(victims, p.memfdS...)
	p.mmapS = nil
	p.memfdS = nil
	return victims
}

func (p *blockPool) freeSlot(s blockSlot) {
	var err error
	if s.fd < 0 {
		err = doMfree(s.buf)
	} else {
		err = doMemfdFree(s.buf, s.fd)
	}
	if err != nil {
		logger.Warnf("Free pooled block fd %d: %s", s.fd, err)
	}
}

// BlockPoolStats reports the block-pool state for metrics. Enabled is false
// when the pool is disabled, in which case every other field is zero.
type BlockPoolStats struct {
	Enabled               bool
	BlockSize             int
	Slots                 int
	Cached                int   // resident warm blocks currently held
	CachedBytes           int64 // resident warm bytes currently held
	ReuseCount            int64
	ReuseBytes            int64
	ReallocCount          int64
	ReallocBytes          int64
	PrefaultBlocks        int64
	PrefaultBytes         int64
	PrefaultFail          int64
	PrefaultSkipBlocks    int64
	PrefaultSkipBytes     int64
	PrefaultDiscardBlocks int64
	PrefaultDiscardBytes  int64
}

// GetBlockPoolStats returns one mutex-consistent snapshot of the pool counters.
func GetBlockPoolStats() BlockPoolStats {
	p := blkPool.Load()
	if p == nil {
		return BlockPoolStats{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	cached := p.poolSizeLocked()
	return BlockPoolStats{
		Enabled:               true,
		BlockSize:             p.blockSize,
		Slots:                 p.cap,
		Cached:                cached,
		CachedBytes:           int64(cached) * int64(p.blockSize),
		ReuseCount:            p.reuseCount,
		ReuseBytes:            p.reuseBytes,
		ReallocCount:          p.reallocCount,
		ReallocBytes:          p.reallocBytes,
		PrefaultBlocks:        p.prefaultBlocks,
		PrefaultBytes:         p.prefaultBytes,
		PrefaultFail:          p.prefaultFail,
		PrefaultSkipBlocks:    p.prefaultSkipBlocks,
		PrefaultSkipBytes:     p.prefaultSkipBytes,
		PrefaultDiscardBlocks: p.prefaultDiscardBlocks,
		PrefaultDiscardBytes:  p.prefaultDiscardBytes,
	}
}

// populateBuffer faults the pages backing b into memory.
func populateBuffer(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	err := unix.Madvise(b, unix.MADV_POPULATE_WRITE)
	if err == nil {
		return nil
	}
	// Older kernels reject MADV_POPULATE_WRITE; valid mmap and memfd buffers
	// can safely fall back to explicit writes only in that case.
	if err != unix.EINVAL && err != unix.ENOSYS && err != unix.EOPNOTSUPP {
		return err
	}
	for i := 0; i < len(b); i += pageSize {
		b[i] = 0
	}
	b[len(b)-1] = 0
	return nil
}
