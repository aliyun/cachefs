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

package chunk

import (
	"math/rand"
	"os"
	"syscall"
	"testing"

	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func newDioTestCache(t *testing.T, dir string, readDIO, writeDIO bool) *diskCache {
	t.Helper()
	return &diskCache{
		pages:           make(map[CacheKey]*Page),
		keys:            make(map[CacheKey]cacheItem),
		toCommit:        make(map[CacheKey]*pendingBlockFile),
		dir:             dir,
		readDIO:         readDIO,
		writeDIO:        writeDIO,
		dio:             utils.NewDioGate("local cache"),
		cacheWrites:     prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheWriteBytes: prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheWriteHist:  prometheus.NewHistogram(prometheus.HistogramOpts{Buckets: prometheus.DefBuckets}),
	}
}

// TestDiskCacheDirectIORoundTrip writes a full page-aligned block with write
// O_DIRECT and reads it back with read O_DIRECT. The data must be identical
// regardless of whether the temp filesystem actually supports O_DIRECT (it may
// permanently downgrade to buffered mid-test).
func TestDiskCacheDirectIORoundTrip(t *testing.T) {
	dir := t.TempDir()
	cache := newDioTestCache(t, dir, true, true)

	key := CacheKey{Target: dir, Blk: 0}
	data := utils.AllocAlignedBuf(utils.DirectIOAlignSize)
	rand.New(rand.NewSource(1)).Read(data)
	require.True(t, utils.BufAligned(data))

	require.NoError(t, cache.flushPage(key, data))

	// Pre-track so load() takes the tracked branch (no rcache announce needed).
	cache.keys[key] = cacheItem{size: int32(len(data)), atime: 1}

	_, rc, err := cache.load(key.Target, key.Blk, true)
	require.NoError(t, err)
	require.NotNil(t, rc)
	defer rc.Close()
	// The wrapper is only returned while O_DIRECT is still enabled; on a
	// filesystem that rejects O_DIRECT the shared gate is disabled during the
	// write above and load() returns a plain *os.File. Either way the data must
	// round-trip correctly.
	if !cache.dio.Disabled() {
		require.IsType(t, &directCacheReader{}, rc)
	}

	got := utils.AllocAlignedBuf(utils.DirectIOAlignSize)
	n, err := rc.ReadAt(got, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, got)
}

// TestDiskCacheWriteUnalignedFallback verifies that a partial/unaligned block is
// written via buffered IO without touching (or disabling) the O_DIRECT gate.
func TestDiskCacheWriteUnalignedFallback(t *testing.T) {
	dir := t.TempDir()
	cache := newDioTestCache(t, dir, false, true)

	key := CacheKey{Target: dir, Blk: 0}
	data := []byte("a short, unaligned tail block")
	require.NoError(t, cache.flushPage(key, data))

	require.False(t, cache.dio.Disabled(), "unaligned write must not attempt or disable O_DIRECT")

	written, err := os.ReadFile(cache.cachePath(key))
	require.NoError(t, err)
	require.Equal(t, data, written)
}

// TestDiskCacheReadWrapperBufferedSurface verifies the read wrapper serves Read
// and ExportFd from a buffered handle, so the fd handed to mmap clients is a
// plain fd.
func TestDiskCacheReadWrapperBufferedSurface(t *testing.T) {
	dir := t.TempDir()
	// writeDIO=false so the shared gate is never disabled during the write; the
	// read gate then stays enabled and load() deterministically returns the
	// directCacheReader wrapper (its Fd/Read surfaces are always buffered).
	cache := newDioTestCache(t, dir, true, false)

	key := CacheKey{Target: dir, Blk: 0}
	data := utils.AllocAlignedBuf(utils.DirectIOAlignSize)
	rand.New(rand.NewSource(2)).Read(data)
	require.NoError(t, cache.flushPage(key, data))
	cache.keys[key] = cacheItem{size: int32(len(data)), atime: 1}

	_, rc, err := cache.load(key.Target, key.Blk, true)
	require.NoError(t, err)
	require.NotNil(t, rc)
	defer rc.Close()
	require.IsType(t, &directCacheReader{}, rc)

	// ExportFd() must export the buffered fd (a valid, addressable descriptor) and
	// report no memfd: a cache file's inode is never poolable.
	fd, memfd, err := rc.ExportFd()
	require.NoError(t, err)
	require.GreaterOrEqual(t, fd, 0)
	require.Nil(t, memfd)
	require.NoError(t, syscall.Close(fd))

	// Read() (io.Reader) reads sequentially from the buffered handle.
	buf := make([]byte, len(data))
	total := 0
	for total < len(data) {
		n, rerr := rc.Read(buf[total:])
		total += n
		if rerr != nil {
			require.ErrorIs(t, rerr, nil)
			break
		}
	}
	require.Equal(t, len(data), total)
	require.Equal(t, data, buf)
}

// TestDiskCacheDioGateDisablesBoth verifies the shared gate disables read and
// write O_DIRECT together.
func TestDiskCacheDioGateDisablesBoth(t *testing.T) {
	cache := newDioTestCache(t, t.TempDir(), true, true)
	require.True(t, cache.readDirectIOEnabled())
	require.True(t, cache.writeDirectIOEnabled())

	cache.dio.Disable(syscall.EINVAL)

	require.False(t, cache.readDirectIOEnabled())
	require.False(t, cache.writeDirectIOEnabled())
}

// TestDioAllowed covers the daemon-server gating: O_DIRECT is refused when a
// cachefsd socket is set (the fd is handed to clients that mmap it).
func TestDioAllowed(t *testing.T) {
	// Not requested: false without probing anything.
	require.False(t, dioAllowed(&StoreConfig{}, false, "read"))
	// Requested and allowed.
	require.True(t, dioAllowed(&StoreConfig{}, true, "read"))
	// Requested but running as a daemon server: refused.
	require.False(t, dioAllowed(&StoreConfig{Socket: "/tmp/cachefsd.sock"}, true, "write"))
}
