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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemfdAlloc(t *testing.T) {
	tests := []struct {
		name        string
		size        int
		expectError bool
		desc        string
	}{
		{
			name:        "valid short name",
			size:        4096,
			expectError: false,
			desc:        "normal case with short name",
		},
		{
			// The kernel object is created with a fixed generic name, so a long
			// name is a Go-side label only and has no length limit to hit.
			name:        strings.Repeat("a", 300),
			size:        4096,
			expectError: false,
			desc:        "name longer than the memfd name limit is fine",
		},
		{
			name:        "",
			size:        4096,
			expectError: false,
			desc:        "empty name should work",
		},
		{
			name:        "test",
			size:        0,
			expectError: true, // Ftruncate with size 0 might be allowed, but mmap with size 0 will fail
			desc:        "zero size should fail",
		},
		{
			name:        "test",
			size:        -1,
			expectError: true,
			desc:        "negative size should fail",
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			m, err := MemfdAlloc(tt.name, tt.size)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
					_ = m.Free()
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if m.Fd < 0 {
				t.Errorf("expected valid fd, got %d", m.Fd)
			}
			if m.Name != tt.name {
				t.Errorf("expected name %q, got %q", tt.name, m.Name)
			}
			if len(m.Data()) != tt.size {
				t.Errorf("expected buffer length %d, got %d", tt.size, len(m.Data()))
			}

			if err := m.Free(); err != nil {
				t.Errorf("Free: %s", err)
			}
		})
	}
}

// TestMemfdFreePoolFull checks a free that finds the pool full really frees the
// block instead of dropping it on the floor.
func TestMemfdFreePoolFull(t *testing.T) {
	const bs = 8192
	enableTestPool(t, bs, 1)

	m1, err := MemfdAlloc("first", bs)
	require.NoError(t, err)
	m2, err := MemfdAlloc("second", bs)
	require.NoError(t, err)

	require.NoError(t, m1.Free())
	require.Equal(t, 1, GetBlockPoolStats().Cached)

	before := AllocMemory()
	require.NoError(t, m2.Free())
	// Still one block cached, and the rejected one gave its memory back.
	require.Equal(t, 1, GetBlockPoolStats().Cached)
	require.Equal(t, before-int64(bs), AllocMemory())
	require.Equal(t, -1, m2.Fd)
	require.Nil(t, m2.Data())
}

// TestMemfdFreeSizeMismatch checks only whole blocks are poolable: any other size
// is really freed even with the pool half empty.
func TestMemfdFreeSizeMismatch(t *testing.T) {
	const bs = 8192
	enableTestPool(t, bs, 2)

	m, err := MemfdAlloc("odd size", bs/2)
	require.NoError(t, err)

	before := AllocMemory()
	require.NoError(t, m.Free())
	require.Equal(t, 0, GetBlockPoolStats().Cached)
	require.Equal(t, before-int64(bs/2), AllocMemory())
	require.Equal(t, -1, m.Fd)
	require.Nil(t, m.Data())
}

// TestMemfdAllocMemoryAccounting checks memfd blocks are counted as memory in use,
// and keep counting while pooled: the pool deliberately holds on to their RSS, and
// a reused block must not be counted twice.
func TestMemfdAllocMemoryAccounting(t *testing.T) {
	const bs = 8192
	enableTestPool(t, bs, 1)

	before := AllocMemory()
	m, err := MemfdAlloc("counted", bs)
	require.NoError(t, err)
	require.Equal(t, before+int64(bs), AllocMemory())

	require.NoError(t, m.Free())
	require.Equal(t, 1, GetBlockPoolStats().Cached)
	require.Equal(t, before+int64(bs), AllocMemory())

	m2, err := MemfdAlloc("counted again", bs)
	require.NoError(t, err)
	require.Equal(t, 0, GetBlockPoolStats().Cached)
	require.Equal(t, before+int64(bs), AllocMemory())
	require.NoError(t, m2.Free())
}
