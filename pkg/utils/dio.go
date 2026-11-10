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
	"os"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// DirectIOAlignSize is the alignment (in bytes) O_DIRECT requires for file
// offset, IO length and the memory buffer address. 4096 covers the logical
// block size of virtually all block devices (512 or 4096). Cache/source block
// size is always a power of 2 >= 4MiB, so full-block offsets and lengths are
// always multiples of it.
const DirectIOAlignSize = 4096

// OpenDirect opens the file with O_DIRECT in addition to the given flags.
func OpenDirect(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag|syscall.O_DIRECT, perm)
}

// BufAligned reports whether buf can back a full O_DIRECT transfer: both its
// start address and its length must be aligned to DirectIOAlignSize (an empty
// buffer is never eligible). The remaining O_DIRECT requirement, file offset
// alignment, is a property of the IO call site and must be checked there.
func BufAligned(buf []byte) bool {
	return len(buf) > 0 &&
		len(buf)%DirectIOAlignSize == 0 &&
		uintptr(unsafe.Pointer(&buf[0]))%DirectIOAlignSize == 0
}

// AllocAlignedBuf returns a heap-backed byte slice of the given size whose start
// address is aligned to DirectIOAlignSize; combined with a size that is a multiple
// of DirectIOAlignSize the result satisfies BufAligned. It over-allocates and
// reslices to the first aligned offset (Go's GC does not move heap objects, so the
// address stays stable for the slice's lifetime). It returns nil for a non-positive
// size. This is mainly a helper for exercising O_DIRECT paths without mmap/memfd
// page allocation.
func AllocAlignedBuf(size int) []byte {
	if size <= 0 {
		return nil
	}
	b := make([]byte, size+DirectIOAlignSize)
	off := 0
	if m := int(uintptr(unsafe.Pointer(&b[0])) % DirectIOAlignSize); m != 0 {
		off = DirectIOAlignSize - m
	}
	return b[off : off+size]
}

// DioGate is a one-way latch that permanently disables O_DIRECT for a subsystem
// once the underlying filesystem rejects it (EINVAL), logging exactly once. It
// is metric-agnostic: callers expose the effective state via their own gauge by
// polling Disabled(). It contains an atomic and must not be copied after use.
type DioGate struct {
	disabled atomic.Bool
	label    string // subsystem label used in the warning message
}

// NewDioGate returns a gate whose warning message identifies the given subsystem.
func NewDioGate(label string) *DioGate {
	return &DioGate{label: label}
}

// Disable permanently falls back to buffered IO. The compare-and-swap guarantees
// the warning is logged exactly once even under concurrent first failures, and
// uses no lock to avoid ordering issues with the caller's mutexes.
func (g *DioGate) Disable(cause error) {
	if g.disabled.CompareAndSwap(false, true) {
		logger.Warnf("O_DIRECT not supported for %s (%s); permanently falling back to buffered IO", g.label, cause)
	}
}

// Disabled reports whether O_DIRECT has been permanently disabled.
func (g *DioGate) Disabled() bool {
	return g.disabled.Load()
}
