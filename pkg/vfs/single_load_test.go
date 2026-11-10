/*
 * Copyright 2024 Alibaba Cloud, Inc. or its affiliates.
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
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/stretchr/testify/assert"
)

type store struct {
	sync.Mutex
	blocks map[chunk.CacheKey]*singleLoad
	name   string
}

func TestSingleLoad(t *testing.T) {
	var created bool
	var ts *store
	var sl1 *singleLoad
	var sl2 *singleLoad
	key1 := chunk.CacheKey{Target: "testtarget", Blk: 0}
	key2 := chunk.CacheKey{Target: "testtarget", Blk: 1}
	cacheReaderName := "cache store"
	daemonReaderName := "daemon store"
	sourceReaderName := "source"

	// 1. first load, should be created
	ts = &store{blocks: make(map[chunk.CacheKey]*singleLoad), name: cacheReaderName}
	sl1, created = getSingleLoad(key1, ts.blocks, ts, time.Second, ts.name)
	assert.Equal(t, true, created)
	assert.NotNil(t, sl1)
	assert.Equal(t, 1, len(ts.blocks))
	assert.Equal(t, false, sl1.ready)
	assert.Equal(t, 1, int(sl1.refs))
	assert.Equal(t, cacheReaderName, sl1.name)

	// close it
	sl1.Close()
	assert.Equal(t, 0, len(ts.blocks))
	assert.Equal(t, false, sl1.ready)
	assert.Equal(t, 0, int(sl1.refs))

	// 2. load two keys
	ts = &store{blocks: make(map[chunk.CacheKey]*singleLoad), name: daemonReaderName}
	sl1, created = getSingleLoad(key1, ts.blocks, ts, time.Second, ts.name)
	sl2, created = getSingleLoad(key2, ts.blocks, ts, time.Second, ts.name)
	assert.Equal(t, true, created)
	assert.NotNil(t, sl2)
	assert.Equal(t, 2, len(ts.blocks))
	assert.Equal(t, false, sl1.ready)
	assert.Equal(t, false, sl2.ready)
	assert.Equal(t, 1, int(sl1.refs))
	assert.Equal(t, 1, int(sl2.refs))
	assert.Equal(t, daemonReaderName, sl1.name)
	assert.Equal(t, daemonReaderName, sl2.name)

	// set fail
	sl1.setFail()
	assert.Equal(t, 1, len(ts.blocks))
	assert.Equal(t, false, sl2.ready)
	assert.Equal(t, 1, int(sl2.refs))

	// 3. load the same key twice
	ts = &store{blocks: make(map[chunk.CacheKey]*singleLoad), name: sourceReaderName}
	sl1, created = getSingleLoad(key1, ts.blocks, ts, time.Second, ts.name)
	sl2, created = getSingleLoad(key1, ts.blocks, ts, time.Millisecond*10, ts.name)
	assert.Equal(t, false, created) // sl1 not set ready, load sl2 timed out, returns nil
	assert.Nil(t, sl2)
	assert.Equal(t, 1, len(ts.blocks)) // sl1 still in blocks
	assert.Equal(t, false, sl1.ready)
	assert.Equal(t, 1, int(sl1.refs))

	// 3. load the same key twice, and set ready
	ts = &store{blocks: make(map[chunk.CacheKey]*singleLoad), name: cacheReaderName}
	sl1, created = getSingleLoad(key1, ts.blocks, ts, time.Second, ts.name)

	readyCh := make(chan struct{})
	data := []byte("test")
	page := chunk.NewPage(data)
	defer page.Release()
	go func() {
		close(readyCh)
		time.Sleep(200 * time.Millisecond)
		sl1.setPageReady(page)
	}()
	<-readyCh
	sl2, created = getSingleLoad(key1, ts.blocks, ts, time.Second, ts.name)
	assert.Equal(t, false, created)
	assert.Same(t, sl1, sl2) // sl1 and sl2 should point to the same object
	assert.NotNil(t, sl2)
	assert.Equal(t, 1, len(ts.blocks))
	assert.Equal(t, true, sl2.ready)
	assert.Equal(t, 2, int(sl2.refs))
	assert.Equal(t, cacheReaderName, sl1.name)
	assert.Equal(t, cacheReaderName, sl2.name)

	// close one ref
	sl1.Close()
	assert.Equal(t, 1, len(ts.blocks))
	assert.Equal(t, true, sl2.ready)
	assert.Equal(t, 1, int(sl2.refs))

	// read data
	buf := make([]byte, len(data))
	n, err := sl2.reader.Read(buf)
	assert.Equal(t, len(data), n)
	assert.Nil(t, err)
	assert.Equal(t, 0, bytes.Compare(buf, data))

	buf = make([]byte, len(data))
	n, err = sl2.reader.ReadAt(buf, 0)
	assert.Equal(t, len(data), n)
	assert.Nil(t, err)
	assert.Equal(t, 0, bytes.Compare(buf, data))
}

// TestGetSingleLoadTimeout tests getSingleLoad returns nil when timeout and not ready
func TestGetSingleLoadTimeout(t *testing.T) {
	key := chunk.CacheKey{Target: "testtarget", Blk: 0}
	ts := &store{blocks: make(map[chunk.CacheKey]*singleLoad), name: "cache store"}

	// first load creates singleLoad
	sl1, created := getSingleLoad(key, ts.blocks, ts, time.Second, ts.name)
	assert.True(t, created)
	assert.NotNil(t, sl1)
	assert.Equal(t, 1, len(ts.blocks))
	assert.False(t, sl1.ready)
	assert.Equal(t, 1, int(sl1.refs))

	// second load with short timeout, sl1 not ready, should return nil
	sl2, created := getSingleLoad(key, ts.blocks, ts, time.Millisecond*10, ts.name)
	assert.False(t, created)
	assert.Nil(t, sl2)
	// sl1 is still in blocks because it still has 1 ref from first call
	assert.Equal(t, 1, len(ts.blocks))
	assert.False(t, sl1.ready)
	assert.Equal(t, 1, int(sl1.refs)) // refs from first call

	// close sl1
	sl1.Close()
	assert.Equal(t, 0, len(ts.blocks))
	assert.Equal(t, 0, int(sl1.refs))
}

// TestSetPageReadyLockedIdempotent tests setPageReadyLocked doesn't override existing page/reader
func TestSetPageReadyLockedIdempotent(t *testing.T) {
	key := chunk.CacheKey{Target: "testtarget", Blk: 0}
	ts := &store{blocks: make(map[chunk.CacheKey]*singleLoad), name: "cache store"}

	sl, _ := getSingleLoad(key, ts.blocks, ts, time.Second, ts.name)

	data1 := []byte("first data")
	page1 := chunk.NewPage(data1)
	defer page1.Release()

	// first setPageReady
	sl.setPageReady(page1)
	assert.True(t, sl.ready)
	firstPage := sl.page
	firstReader := sl.reader
	assert.NotNil(t, firstPage)
	assert.NotNil(t, firstReader)

	data2 := []byte("second data")
	page2 := chunk.NewPage(data2)
	defer page2.Release()

	// second setPageReady should not override
	sl.setPageReady(page2)
	assert.Same(t, firstPage, sl.page)
	assert.Same(t, firstReader, sl.reader)

	// verify data is still from first page
	buf := make([]byte, len(data1))
	n, err := sl.reader.ReadAt(buf, 0)
	assert.Equal(t, len(data1), n)
	assert.Nil(t, err)
	assert.Equal(t, 0, bytes.Compare(buf, data1))
}

// TestSetReaderReadyIdempotent tests setReaderReady doesn't override existing reader
func TestSetReaderReadyIdempotent(t *testing.T) {
	key := chunk.CacheKey{Target: "testtarget", Blk: 0}
	ts := &store{blocks: make(map[chunk.CacheKey]*singleLoad), name: "cache store"}

	sl, _ := getSingleLoad(key, ts.blocks, ts, time.Second, ts.name)

	data1 := []byte("first data")
	page1 := chunk.NewPage(data1)
	defer page1.Release()
	reader1 := chunk.NewPageReader(page1)

	// first setReaderReady
	sl.setReaderReady(reader1)
	assert.True(t, sl.ready)
	firstReader := sl.reader
	assert.NotNil(t, firstReader)

	data2 := []byte("second data")
	page2 := chunk.NewPage(data2)
	defer page2.Release()
	reader2 := chunk.NewPageReader(page2)

	// second setReaderReady should not override, reader2 should be closed
	sl.setReaderReady(reader2)
	assert.Same(t, firstReader, sl.reader)

	// verify data is still from first reader
	buf := make([]byte, len(data1))
	n, err := sl.reader.ReadAt(buf, 0)
	assert.Equal(t, len(data1), n)
	assert.Nil(t, err)
	assert.Equal(t, 0, bytes.Compare(buf, data1))
}

// TestSetFailReturnValue tests setFail return value based on ready state
func TestSetFailReturnValue(t *testing.T) {
	key := chunk.CacheKey{Target: "testtarget", Blk: 0}
	ts := &store{blocks: make(map[chunk.CacheKey]*singleLoad), name: "cache store"}

	// create not-ready singleLoad
	sl, _ := getSingleLoad(key, ts.blocks, ts, time.Second, ts.name)
	assert.False(t, sl.ready)
	assert.Equal(t, 1, len(ts.blocks))

	// setFail on not-ready should return false and remove from blocks
	ready := sl.setFail()
	assert.False(t, ready)
	assert.Equal(t, 0, len(ts.blocks))

	// create another singleLoad and set it ready
	sl2, _ := getSingleLoad(key, ts.blocks, ts, time.Second, ts.name)
	data := []byte("test")
	page := chunk.NewPage(data)
	defer page.Release()
	sl2.setPageReady(page)
	assert.True(t, sl2.ready)
	assert.Equal(t, 1, len(ts.blocks))

	// setFail on ready should return true and not remove from blocks
	ready = sl2.setFail()
	assert.True(t, ready)
	assert.Equal(t, 1, len(ts.blocks)) // should still be in blocks
}
