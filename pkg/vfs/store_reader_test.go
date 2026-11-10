/*
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

package vfs

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/meta"
	cachefsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/stretchr/testify/assert"
	"github.com/willf/bitset"
)

type mockStore struct {
	loadErr        error
	loadPage       *chunk.Page
	fillErr        error
	size           int64
	removeResult   int64
	removeResult2  int64
	dataReadyCalls int
	isHashHit      bool
}

func (m *mockStore) Load(ctx context.Context, target string, length uint64, blk uint32) (*chunk.Page, error) {
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	if m.loadPage != nil {
		return m.loadPage, nil
	}
	return chunk.AllocPage(1024, "test")
}

func (m *mockStore) ReadBlock(ctx context.Context, target string, length uint64, blk uint32) (chunk.ChunkReader, error) {
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	if m.loadPage != nil {
		return chunk.NewPageReader(m.loadPage), nil
	}
	page, err := chunk.AllocPage(1024, "test")
	if err != nil {
		return nil, err
	}
	return chunk.NewPageReader(page), nil
}

func (m *mockStore) Fill(ctx context.Context, target string, length uint64, blk uint32) error {
	return m.fillErr
}

func (m *mockStore) Cache(target string, blk uint32, page *chunk.Page) {
}

func (m *mockStore) Size() int64 {
	return m.size
}

func (m *mockStore) RemoveByPrefix(remove, keep []string) (int64, int64) {
	return m.removeResult, m.removeResult2
}

func (m *mockStore) DataReady() {
	m.dataReadyCalls++
}

func (m *mockStore) IsHashHit(target string, blk uint32) bool {
	return m.isHashHit
}

func (m *mockStore) Stop() {}

func (m *mockStore) ClearBurst() (int64, int64) {
	return 0, 0
}

func (m *mockStore) SetCacheSize(cacheSize, burstCacheSize int64) (int64, int64, int64, int64) {
	return cacheSize, burstCacheSize, 0, 0
}

func (m *mockStore) GetCacheSize() (int64, int64) {
	return 0, 0
}

func (m *mockStore) PinCache(prefixes []string)    {}
func (m *mockStore) UnpinCache(prefixes []string)  {}
func (m *mockStore) SetPinCache(prefixes []string) {}
func (m *mockStore) IsPinned(target string) bool   { return false }
func (m *mockStore) HasPins() bool                 { return false }
func (m *mockStore) Check(target string, blk uint32) bool {
	return false
}
func (m *mockStore) LockTarget(target string, blocks []uint32)   {}
func (m *mockStore) UnlockTarget(target string, blocks []uint32) {}
func (m *mockStore) GetCounter(name string) (*metric.AtomicCounter, error) {
	return nil, fmt.Errorf("counter %q not available in mock", name)
}
func (m *mockStore) GetCacheState() chunk.CacheState       { return chunk.CacheState{} }
func (m *mockStore) GetPeerLoadStats() chunk.PeerLoadStats { return chunk.PeerLoadStats{} }
func (m *mockStore) TrackerSnapshot(prefixes []string, local bool) map[string]map[string]*bitset.BitSet {
	return map[string]map[string]*bitset.BitSet{}
}

func TestNewStoreReader(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize:   4 << 20,
				PeerTimeout: 30,
			},
		},
	}

	store := &mockStore{}
	sr := newStoreReader(fs, store, nil)

	assert.NotNil(t, sr)
	storeReader, ok := sr.(*storeReader)
	assert.True(t, ok)
	assert.Equal(t, fs, storeReader.fs)
	assert.Equal(t, store, storeReader.store)
	assert.NotNil(t, storeReader.blocks)
}

func TestStoreReaderReadFromStore(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize:   4 << 20,
				PeerTimeout: 30,
			},
			Meta: &cachefsmeta.MetaConfig{},
		},
	}

	store := &mockStore{}
	sr := newStoreReader(fs, store, nil).(*storeReader)

	reader, err := sr.readFromStore(context.Background(), "test", 1024, 0, 1024, true)
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	reader.Close()
}

func TestStoreReaderReadFromStoreError(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize:   4 << 20,
				PeerTimeout: 30,
			},
			Meta: &cachefsmeta.MetaConfig{},
		},
	}

	store := &mockStore{
		loadErr: errors.New("load error"),
	}
	sr := newStoreReader(fs, store, nil).(*storeReader)

	_, err := sr.readFromStore(context.Background(), "test", 1024, 0, 1024, true)
	assert.Error(t, err)
}

func TestStoreReaderFillStore(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize:   4 << 20,
				PeerTimeout: 30,
			},
		},
	}

	store := &mockStore{}
	sr := newStoreReader(fs, store, nil).(*storeReader)

	err := sr.fillStore(meta.Background, "test", 1024, 0)
	assert.NoError(t, err)
}

func TestStoreReaderFillStoreError(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize:   4 << 20,
				PeerTimeout: 30,
			},
		},
	}

	store := &mockStore{
		fillErr: errors.New("fill error"),
	}
	sr := newStoreReader(fs, store, nil).(*storeReader)

	err := sr.fillStore(meta.Background, "test", 1024, 0)
	assert.Error(t, err)
}

func TestStoreReaderAddToStore(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize:   4 << 20,
				PeerTimeout: 30,
			},
		},
	}

	store := &mockStore{}
	sr := newStoreReader(fs, store, nil).(*storeReader)

	page, err := chunk.AllocPage(1024, "test")
	if !assert.NoError(t, err) {
		return
	}
	err = sr.addToStore(meta.Background, "test", 1024, 0, page, true)
	assert.NoError(t, err)
	page.Release()
}

func TestStoreReaderSize(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize:   4 << 20,
				PeerTimeout: 30,
			},
		},
	}

	store := &mockStore{
		size: 12345,
	}
	sr := newStoreReader(fs, store, nil).(*storeReader)

	size := sr.size()
	assert.Equal(t, int64(12345), size)
}

func TestStoreReaderRemoveByPrefix(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize:   4 << 20,
				PeerTimeout: 30,
			},
		},
	}

	store := &mockStore{
		removeResult:  100,
		removeResult2: 200,
	}
	sr := newStoreReader(fs, store, nil).(*storeReader)

	removed, kept := sr.removeByPrefix([]string{"test"}, []string{"keep"})
	assert.Equal(t, int64(100), removed)
	assert.Equal(t, int64(200), kept)
}

func TestStoreReaderDataReady(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize:   4 << 20,
				PeerTimeout: 30,
			},
		},
	}

	store := &mockStore{}
	sr := newStoreReader(fs, store, nil).(*storeReader)

	sr.dataReady()
	assert.Equal(t, 1, store.dataReadyCalls)
}

func TestStoreReaderIsHashHit(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize:   4 << 20,
				PeerTimeout: 30,
			},
		},
	}

	tests := []struct {
		name      string
		isHashHit bool
	}{
		{"hash hit", true},
		{"hash miss", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &mockStore{
				isHashHit: tt.isHashHit,
			}
			sr := newStoreReader(fs, store, nil).(*storeReader)

			result := sr.isHashHit("test", 0)
			assert.Equal(t, tt.isHashHit, result)
		})
	}
}

func TestStoreReaderStop(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize:   4 << 20,
				PeerTimeout: 30,
			},
		},
	}

	store := &mockStore{}
	sr := newStoreReader(fs, store, nil).(*storeReader)

	sr.stop()
}

func TestDaemonReaderSize(t *testing.T) {
	dr := &daemonReader{}
	size := dr.size()
	assert.Equal(t, int64(0), size)
}

func TestDaemonReaderRemoveByPrefix(t *testing.T) {
	dr := &daemonReader{}
	removed, kept := dr.removeByPrefix([]string{"test"}, []string{"keep"})
	assert.Equal(t, int64(0), removed)
	assert.Equal(t, int64(0), kept)
}

func TestDaemonReaderDataReady(t *testing.T) {
	dr := &daemonReader{}
	dr.dataReady()
}

func TestDaemonReaderIsHashHit(t *testing.T) {
	dr := &daemonReader{}
	result := dr.isHashHit("test", 0)
	assert.True(t, result)
}
