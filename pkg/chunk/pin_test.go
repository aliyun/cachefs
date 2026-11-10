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
	"fmt"
	"sync"
	"testing"

	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/serf"
	"github.com/stretchr/testify/assert"
)

func newPinTestStore(t *testing.T, sourceID, keyPrefix string) *store {
	conf, err := NewStoreConfig()
	assert.NoError(t, err)
	conf.SourceID = sourceID
	conf.KeyPrefix = keyPrefix
	conf.CacheDir = "memory"
	rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: conf.CacheAddr})
	assert.NoError(t, err)
	return doNewStore(conf, rpc, nil, serf.NewSerfService(serf.DefaultSerfConfig(), nil), nil)
}

func TestCachePinner(t *testing.T) {
	// Helper to get stored prefixes from pinCache internals
	getStoredPrefixes := func(store *store) map[string]struct{} {
		store.pinCache.mu.RLock()
		defer store.pinCache.mu.RUnlock()
		cp := make(map[string]struct{}, len(store.pinCache.prefixes))
		for k, v := range store.pinCache.prefixes {
			cp[k] = v
		}
		return cp
	}

	t.Run("IsPinned empty", func(t *testing.T) {
		store := newPinTestStore(t, "", "")
		assert.False(t, store.IsPinned("/models/llama/weights.bin"))
		assert.False(t, store.HasPins())
	})

	t.Run("IsPinned directory match", func(t *testing.T) {
		store := newPinTestStore(t, "", "")
		store.PinCache([]string{"/models/llama/"})
		assert.True(t, store.IsPinned("/models/llama/weights.bin"))
		assert.True(t, store.IsPinned("/models/llama/config.json"))
		assert.True(t, store.IsPinned("/models/llama/sub/deep/file"))
	})

	t.Run("IsPinned no false match", func(t *testing.T) {
		store := newPinTestStore(t, "", "")
		store.PinCache([]string{"/models/llama/"})
		assert.False(t, store.IsPinned("/models/llama-v2/weights.bin"))
		assert.False(t, store.IsPinned("/other/path"))
	})

	t.Run("IsPinned SafeCacheKey suffix", func(t *testing.T) {
		store := newPinTestStore(t, "", "")
		store.PinCache([]string{"/models/llama/"})
		assert.True(t, store.IsPinned("/models/llama/file:1024-1234567890-0"))
	})

	t.Run("PinCache stores genTarget-processed keys with prefix", func(t *testing.T) {
		store := newPinTestStore(t, "mybucket", "/prefix")
		store.PinCache([]string{"/models/llama/", "/data/"})
		stored := getStoredPrefixes(store)
		assert.Contains(t, stored, "/prefix/stag_mybucket/models/llama/")
		assert.Contains(t, stored, "/prefix/stag_mybucket/data/")
		assert.Len(t, stored, 2)
	})

	t.Run("PinCache stores genTarget-processed keys without prefix", func(t *testing.T) {
		store := newPinTestStore(t, "", "")
		store.PinCache([]string{"/models/llama/", "/data/"})
		stored := getStoredPrefixes(store)
		// No SourceID, no KeyPrefix: stored key equals input
		assert.Contains(t, stored, "/models/llama/")
		assert.Contains(t, stored, "/data/")
		assert.Len(t, stored, 2)
	})

	t.Run("PinCache accumulates with genTarget", func(t *testing.T) {
		store := newPinTestStore(t, "bkt", "")
		store.PinCache([]string{"/a/"})
		store.PinCache([]string{"/b/"})
		stored := getStoredPrefixes(store)
		assert.Contains(t, stored, "/stag_bkt/a/")
		assert.Contains(t, stored, "/stag_bkt/b/")
		assert.True(t, store.IsPinned("/a/file"))
		assert.True(t, store.IsPinned("/b/file"))
		assert.True(t, store.HasPins())
	})

	t.Run("UnpinCache removes genTarget-processed keys", func(t *testing.T) {
		store := newPinTestStore(t, "bkt", "/p")
		store.PinCache([]string{"/a/", "/b/"})
		store.UnpinCache([]string{"/a/"})
		stored := getStoredPrefixes(store)
		assert.NotContains(t, stored, "/p/stag_bkt/a/")
		assert.Contains(t, stored, "/p/stag_bkt/b/")
		assert.False(t, store.IsPinned("/a/file"))
		assert.True(t, store.IsPinned("/b/file"))
	})

	t.Run("UnpinCache nonexistent is noop", func(t *testing.T) {
		store := newPinTestStore(t, "", "")
		store.PinCache([]string{"/a/"})
		store.UnpinCache([]string{"/nonexistent/"})
		assert.True(t, store.IsPinned("/a/file"))
	})

	t.Run("SetPinCache replaces with genTarget-processed keys", func(t *testing.T) {
		store := newPinTestStore(t, "bkt", "/p")
		store.PinCache([]string{"/a/", "/b/"})
		store.SetPinCache([]string{"/c/"})
		stored := getStoredPrefixes(store)
		assert.NotContains(t, stored, "/p/stag_bkt/a/")
		assert.NotContains(t, stored, "/p/stag_bkt/b/")
		assert.Contains(t, stored, "/p/stag_bkt/c/")
		assert.Len(t, stored, 1)
		assert.False(t, store.IsPinned("/a/file"))
		assert.True(t, store.IsPinned("/c/file"))
	})

	t.Run("SetPinCache with empty slice clears all", func(t *testing.T) {
		store := newPinTestStore(t, "bkt", "")
		store.PinCache([]string{"/a/", "/b/"})
		store.SetPinCache([]string{})
		assert.False(t, store.HasPins())
		assert.False(t, store.IsPinned("/a/file"))
		assert.False(t, store.IsPinned("/b/file"))
		stored := getStoredPrefixes(store)
		assert.Empty(t, stored)
	})

	t.Run("SetPinCache with nil clears all", func(t *testing.T) {
		store := newPinTestStore(t, "", "")
		store.PinCache([]string{"/a/"})
		store.SetPinCache(nil)
		assert.False(t, store.HasPins())
		assert.False(t, store.IsPinned("/a/file"))
	})

	t.Run("concurrent access", func(t *testing.T) {
		store := newPinTestStore(t, "", "")
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(3)
			prefix := fmt.Sprintf("/path%d/", i)
			go func() {
				defer wg.Done()
				store.PinCache([]string{prefix})
			}()
			go func() {
				defer wg.Done()
				store.IsPinned(prefix + "file")
			}()
			go func() {
				defer wg.Done()
				store.HasPins()
			}()
		}
		wg.Wait()
		for i := 0; i < 5; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				store.SetPinCache([]string{"/x/"})
			}()
			go func() {
				defer wg.Done()
				store.SetPinCache(nil)
			}()
		}
		wg.Wait()
	})
}

func TestPinCacheInternal(t *testing.T) {
	assert := assert.New(t)

	t.Run("isPinned empty", func(t *testing.T) {
		pc := newPinCache()
		assert.False(pc.isPinned("/any/path"))
	})

	t.Run("isPinned match", func(t *testing.T) {
		pc := newPinCache()
		pc.add([]string{"/models/llama/"})
		assert.True(pc.isPinned("/models/llama/file.bin"))
		assert.True(pc.isPinned("/models/llama/sub/deep"))
		assert.False(pc.isPinned("/models/llama-v2/file"))
		assert.False(pc.isPinned("/other"))
	})

	t.Run("hasPins", func(t *testing.T) {
		pc := newPinCache()
		assert.False(pc.hasPins())
		pc.add([]string{"/a/"})
		assert.True(pc.hasPins())
		pc.set(nil)
		assert.False(pc.hasPins())
	})

	t.Run("add accumulates", func(t *testing.T) {
		pc := newPinCache()
		pc.add([]string{"/a/"})
		pc.add([]string{"/b/"})
		assert.True(pc.isPinned("/a/file"))
		assert.True(pc.isPinned("/b/file"))
	})

	t.Run("remove", func(t *testing.T) {
		pc := newPinCache()
		pc.add([]string{"/a/", "/b/", "/c/"})
		pc.remove([]string{"/b/"})
		assert.True(pc.isPinned("/a/file"))
		assert.False(pc.isPinned("/b/file"))
		assert.True(pc.isPinned("/c/file"))
	})

	t.Run("remove nonexistent is noop", func(t *testing.T) {
		pc := newPinCache()
		pc.add([]string{"/a/"})
		pc.remove([]string{"/nonexistent/"})
		assert.True(pc.isPinned("/a/file"))
	})

	t.Run("set replaces", func(t *testing.T) {
		pc := newPinCache()
		pc.add([]string{"/a/", "/b/"})
		pc.set([]string{"/c/", "/d/"})
		assert.False(pc.isPinned("/a/file"))
		assert.False(pc.isPinned("/b/file"))
		assert.True(pc.isPinned("/c/file"))
		assert.True(pc.isPinned("/d/file"))
	})

	t.Run("set empty clears", func(t *testing.T) {
		pc := newPinCache()
		pc.add([]string{"/a/"})
		pc.set([]string{})
		assert.False(pc.hasPins())
		assert.False(pc.isPinned("/a/file"))
	})

	t.Run("set nil clears", func(t *testing.T) {
		pc := newPinCache()
		pc.add([]string{"/a/"})
		pc.set(nil)
		assert.False(pc.hasPins())
		assert.False(pc.isPinned("/a/file"))
	})

	t.Run("set nil clears all", func(t *testing.T) {
		pc := newPinCache()
		pc.add([]string{"/a/", "/b/"})
		pc.set(nil)
		assert.False(pc.hasPins())
		assert.False(pc.isPinned("/a/file"))
		assert.False(pc.isPinned("/b/file"))
	})
}

func TestGenTargets(t *testing.T) {
	assert := assert.New(t)

	t.Run("no prefix no source id", func(t *testing.T) {
		store := newPinTestStore(t, "", "")
		result := store.genTargets([]string{"/a/", "/b/c/"})
		assert.Equal([]string{"/a/", "/b/c/"}, result)
	})

	t.Run("with source id", func(t *testing.T) {
		store := newPinTestStore(t, "mybucket", "")
		result := store.genTargets([]string{"/a/", "/b/"})
		assert.Equal([]string{"/stag_mybucket/a/", "/stag_mybucket/b/"}, result)
	})

	t.Run("with key prefix", func(t *testing.T) {
		store := newPinTestStore(t, "", "/prefix")
		result := store.genTargets([]string{"/a/", "/b/"})
		assert.Equal([]string{"/prefix/a/", "/prefix/b/"}, result)
	})

	t.Run("with key prefix and source id", func(t *testing.T) {
		store := newPinTestStore(t, "mybucket", "/prefix")
		result := store.genTargets([]string{"/a/", "/b/"})
		assert.Equal([]string{"/prefix/stag_mybucket/a/", "/prefix/stag_mybucket/b/"}, result)
	})

	t.Run("empty input", func(t *testing.T) {
		store := newPinTestStore(t, "mybucket", "/prefix")
		result := store.genTargets([]string{})
		assert.Empty(result)
	})
}
