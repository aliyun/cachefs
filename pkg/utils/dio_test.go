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
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestAllocAlignedBuf(t *testing.T) {
	// A non-positive size yields no buffer (and BufAligned rejects it).
	for _, size := range []int{0, -1, -4096} {
		require.Nil(t, AllocAlignedBuf(size), "size=%d", size)
	}

	// For positive sizes the returned slice has exactly the requested length and a
	// start address aligned to DirectIOAlignSize, regardless of whether the length
	// itself is a multiple of the alignment.
	sizes := []int{
		1,
		DirectIOAlignSize - 1,
		DirectIOAlignSize,
		DirectIOAlignSize + 1,
		DirectIOAlignSize*3 + 123,
		128 << 10,
	}
	for _, size := range sizes {
		buf := AllocAlignedBuf(size)
		require.Equal(t, size, len(buf), "size=%d", size)
		require.GreaterOrEqual(t, cap(buf), size, "size=%d", size)
		require.Zero(t, uintptr(unsafe.Pointer(&buf[0]))%DirectIOAlignSize,
			"size=%d start address must be aligned to %d", size, DirectIOAlignSize)

		// The whole buffer must be writable without a panic or aliasing surprises.
		for i := range buf {
			buf[i] = byte(i)
		}

		// BufAligned holds iff the length is also a multiple of the alignment; the
		// address is aligned by construction, so the length is the only variable.
		require.Equal(t, size%DirectIOAlignSize == 0, BufAligned(buf),
			"size=%d", size)
	}
}
