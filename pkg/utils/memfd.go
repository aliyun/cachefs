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
	"fmt"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// memfdName is the kernel name given to every memfd. A pooled memfd reuses the
// same kernel object, whose name is fixed at creation and cannot track the block
// currently occupying it, so a generic name keeps /proc/<pid>/fd from being
// misleading.
const memfdName = "cachefs"

// Memfd is a memfd-backed block: an fd and the MAP_SHARED mapping over it, freed
// together by Free.
//
// A memfd's inode identity can leave this process - the fd is dup'd and handed to
// another process, which mmaps it and holds the mapping for an arbitrary time. For
// such a block "free" and "recycle" are not equivalent: a real free (close +
// munmap) leaves the foreign fd holding the last reference to the tmpfs object, so
// its contents are frozen and safe, while a pool push keeps the same inode alive
// and later rewrites it, and any still-live foreign mapping then observes foreign
// data.
//
// exportCount is what separates the two. It counts exported copies whose
// disappearance is not proven, and a block is pooled only at zero. See
// ExportFd/UnexportFd.
type Memfd struct {
	Name        string // per-allocation name, Go side only; the kernel name is memfdName
	Fd          int
	data        []byte
	exportCount atomic.Int32
	// poisoned marks the count as untrustworthy, which vetoes pooling on its own.
	// See UnexportFd for why a broken count cannot be repaired by clamping.
	poisoned atomic.Bool
}

// Data returns the mmap'd buffer.
func (m *Memfd) Data() []byte {
	return m.data
}

// ExportFd hands out an fd that will leave this process's control, and counts it.
// Dup and count are fused so a raw memfd fd can never be exported uncounted.
func (m *Memfd) ExportFd() (int, error) {
	m.exportCount.Add(1)
	fd, err := unix.Dup(m.Fd)
	if err != nil {
		m.exportCount.Add(-1)
		return -1, err
	}
	return fd, nil
}

// UnexportFd records that one fd handed out by ExportFd is gone. It closes
// nothing: that fd is out of this process's control, so all this does is update
// the count Free consults before reusing the block.
//
// Only call it once the copy is provably gone, meaning the receiver has confirmed
// that it unmapped the memory and closed the fd, or the exported fd never
// actually left this process. A receiver that merely disconnected or died proves
// nothing: its mapping can outlive whatever channel it was reached through, and
// reusing the block would rewrite memory it is still reading.
//
// A copy whose fate cannot be established is therefore never subtracted: the
// count never returns to zero and the block is never reused again. An
// unaccountable export costs reuse, not correctness.
//
// Subtracting one copy twice is a bug, and driving the count below zero is not a
// safe place to leave it: a later legitimate export would cancel the deficit back
// to zero, which is exactly the value that permits pooling, so the block would be
// reused with a live copy outstanding. A count that has gone negative is therefore
// not repaired but abandoned - the block is poisoned and can never be pooled again.
func (m *Memfd) UnexportFd() {
	if n := m.exportCount.Add(-1); n < 0 {
		m.poisoned.Store(true)
		logger.Errorf("Block %s was released %d more times than it was exported, never reusing it", m.Name, -n)
	}
}

// Free releases the block. It pools the block only when no exported copy may
// still be alive, otherwise it really closes the fd and unmaps the buffer.
//
// Free must not run concurrently with ExportFd on the same block: it reads the
// count and pushes as two steps, so an export landing in between would be handed
// out for a block already back in the pool. The count cannot close that window on
// its own - the caller has to keep the block alive for as long as it may still
// export it, which is what a refcount on the owning object does. Free is the point
// where that ownership ends, so by then no exporter can be left.
func (m *Memfd) Free() error {
	// Clear on every exit path so any use after Free fails loudly (EBADF,
	// nil slice) instead of touching resources that may already have a new
	// owner. Safe for a pooled block too: the slot keeps its own copies.
	defer func() {
		m.data, m.Fd = nil, -1
	}()

	if p := blkPool.Load(); p != nil && len(m.data) == p.blockSize {
		if m.exportCount.Load() == 0 && !m.poisoned.Load() {
			if p.push(m.data, m.Fd) {
				return nil
			}
		}
	}
	return doMemfdFree(m.data, m.Fd)
}

// MemfdAlloc allocates a block from memfd, reusing a pooled one when available.
func MemfdAlloc(name string, size int) (*Memfd, error) {
	if p := blkPool.Load(); p != nil && size == p.blockSize {
		if buf, fd, ok := p.pop(true); ok {
			return &Memfd{Name: name, Fd: fd, data: buf}, nil
		}
		buf, fd, err := rawMemfdAlloc(size)
		if err != nil {
			return nil, err
		}
		p.recordRealloc()
		return &Memfd{Name: name, Fd: fd, data: buf}, nil
	}
	buf, fd, err := rawMemfdAlloc(size)
	if err != nil {
		return nil, err
	}
	return &Memfd{Name: name, Fd: fd, data: buf}, nil
}

func rawMemfdAlloc(size int) ([]byte, int, error) {
	fd, err := unix.MemfdCreate(memfdName, 0)
	if err != nil {
		return nil, -1, fmt.Errorf("memfd_create: %s", err)
	}
	if err = unix.Ftruncate(fd, int64(size)); err != nil {
		unix.Close(fd)
		return nil, -1, err
	}
	b, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		unix.Close(fd)
		return nil, -1, err
	}
	if thpShmemAdvise {
		if err = unix.Madvise(b, unix.MADV_HUGEPAGE); err != nil {
			logger.Warnf("Madvise memfd memory: %s", err)
		}
	}
	atomic.AddInt64(&used, int64(cap(b)))
	return b, fd, nil
}

// doMemfdFree closes the memfd and returns its memory to the OS.
func doMemfdFree(b []byte, fd int) error {
	atomic.AddInt64(&used, -int64(cap(b)))
	err1 := unix.Close(fd)
	err2 := unix.Munmap(b)
	if err1 != nil {
		return err1
	}
	return err2
}
