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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/meta"
	cachefsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/willf/bitset"
)

type mockCacheReader struct {
	addToStoreCalled bool
	addToStoreErr    error
}

func (m *mockCacheReader) readFromStore(ctx context.Context, target string, length uint64, blk, dataSize uint32, cache bool) (chunk.ChunkReader, error) {
	return nil, io.EOF
}

func (m *mockCacheReader) fillStore(ctx meta.Context, target string, length uint64, blk uint32) error {
	return nil
}

func (m *mockCacheReader) addToStore(ctx meta.Context, target string, length uint64, blk uint32, page *chunk.Page, cache bool) error {
	m.addToStoreCalled = true
	return m.addToStoreErr
}

func (m *mockCacheReader) check(target string, blk uint32) bool {
	return false
}

func (m *mockCacheReader) size() int64 {
	return 0
}

func (m *mockCacheReader) removeByPrefix(remove, keep []string) (int64, int64) {
	return 0, 0
}

func (m *mockCacheReader) dataReady() {}

func (m *mockCacheReader) stop() {}

func (m *mockCacheReader) isHashHit(target string, blk uint32) bool {
	return true
}

func (m *mockCacheReader) clearBurst() (int64, int64) {
	return 0, 0
}

func (m *mockCacheReader) setCacheSize(cacheSize, burstCacheSize int64) (int64, int64, int64, int64) {
	return cacheSize, burstCacheSize, 0, 0
}

func (m *mockCacheReader) getCacheSize() (int64, int64) {
	return 0, 0
}

func (m *mockCacheReader) getCacheState() chunk.CacheState       { return chunk.CacheState{} }
func (m *mockCacheReader) getPeerLoadStats() chunk.PeerLoadStats { return chunk.PeerLoadStats{} }
func (m *mockCacheReader) trackerSnapshot(prefixes []string, local bool) map[string]map[string]*bitset.BitSet {
	return map[string]map[string]*bitset.BitSet{}
}

func (m *mockCacheReader) setPinCache(prefixes []string)               {}
func (m *mockCacheReader) isPinned(target string) bool                 { return false }
func (m *mockCacheReader) hasPins() bool                               { return false }
func (m *mockCacheReader) lockTarget(target string, blocks []uint32)   {}
func (m *mockCacheReader) unlockTarget(target string, blocks []uint32) {}
func (m *mockCacheReader) getCounter(name string) (*metric.AtomicCounter, error) {
	return nil, fmt.Errorf("counter %q not available in mock", name)
}

func TestNewSourceReader(t *testing.T) {
	tmpDir := t.TempDir()

	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize: 4 << 20,
			},
			Meta: &cachefsmeta.MetaConfig{},
		},
	}

	source := newSourceStore(fs, []string{tmpDir}, 4<<20)
	sr := newSourceReader(fs, source, 30)

	assert.NotNil(t, sr)
	assert.Equal(t, fs, sr.fs)
	assert.Equal(t, source, sr.source)
	assert.NotNil(t, sr.blocks)
	assert.Equal(t, time.Second*30, sr.timeout)
}

func TestSourceReaderReadFromSource(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	blockSize := uint32(4 << 20)
	content := make([]byte, blockSize)
	for i := range content {
		content[i] = byte(i % 256)
	}
	err := os.WriteFile(testFile, content, 0644)
	assert.NoError(t, err)

	mockCache := &mockCacheReader{}
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize: blockSize,
			},
			Meta: &cachefsmeta.MetaConfig{},
		},
		cache:      mockCache,
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}

	source := newSourceStore(fs, []string{tmpDir}, blockSize)
	sr := newSourceReader(fs, source, 30)
	fs.sourceReader = sr

	sf := source.newSourceFile("test.txt", uint64(len(content)))

	reader, err := sr.readFromSource(sf, "test.txt", uint64(len(content)), 0, true)
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	buf := make([]byte, blockSize)
	n, err := reader.ReadAt(buf, 0)
	assert.NoError(t, err)
	assert.Equal(t, int(blockSize), n)

	for i := 0; i < n; i++ {
		assert.Equal(t, byte(i%256), buf[i])
	}

	reader.Close()
	sf.release()

	time.Sleep(100 * time.Millisecond)
	assert.True(t, mockCache.addToStoreCalled)
}

func TestSourceReaderReadFromSourceMetaConsumer(t *testing.T) {
	tmpDir := t.TempDir()

	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize: 4 << 20,
			},
			Meta: &cachefsmeta.MetaConfig{
				Sourceless: true,
			},
		},
	}

	source := newSourceStore(fs, []string{tmpDir}, 4<<20)
	sr := newSourceReader(fs, source, 30)

	sf := source.newSourceFile("test.txt", 1024)

	_, err := sr.readFromSource(sf, "test.txt", 1024, 0, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sourceless")

	sf.release()
}

func TestSourceReaderReadFromSourceNonExistent(t *testing.T) {
	tmpDir := t.TempDir()

	mockCache := &mockCacheReader{}
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize: 4 << 20,
			},
			Meta: &cachefsmeta.MetaConfig{},
		},
		cache: mockCache,
	}

	source := newSourceStore(fs, []string{tmpDir}, 4<<20)
	sr := newSourceReader(fs, source, 30)

	sf := source.newSourceFile("nonexistent.txt", 1024)

	_, err := sr.readFromSource(sf, "nonexistent.txt", 1024, 0, true)
	assert.Error(t, err)

	sf.release()
}

func TestSourceReaderConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	blockSize := uint32(4 << 20)
	content := make([]byte, blockSize*2)
	for i := range content {
		content[i] = byte(i % 256)
	}
	err := os.WriteFile(testFile, content, 0644)
	assert.NoError(t, err)

	mockCache := &mockCacheReader{}
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize: blockSize,
			},
			Meta: &cachefsmeta.MetaConfig{},
		},
		cache:      mockCache,
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}

	source := newSourceStore(fs, []string{tmpDir}, blockSize)
	sr := newSourceReader(fs, source, 30)
	fs.sourceReader = sr

	sf := source.newSourceFile("test.txt", uint64(len(content)))

	done := make(chan bool, 2)

	for i := 0; i < 2; i++ {
		blk := uint32(i)
		go func(block uint32) {
			reader, err := sr.readFromSource(sf, "test.txt", uint64(len(content)), block, true)
			assert.NoError(t, err)
			assert.NotNil(t, reader)

			buf := make([]byte, blockSize)
			n, err := reader.ReadAt(buf, 0)
			assert.NoError(t, err)
			assert.Equal(t, int(blockSize), n)

			reader.Close()
			done <- true
		}(blk)
	}

	<-done
	<-done

	sf.release()
}
