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
	"sync"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/utils"
)

// reading source is slower than peer load, so we assign a longer timeout.
const sourceSingleLoadTimeout int = 300 // seconds

// `singleLoad` allows only one on-going load from cache store or source store, avoid read
// amplification.
type singleLoad struct {
	lock   sync.Locker
	key    chunk.CacheKey
	page   *chunk.Page
	reader chunk.ChunkReader
	refs   uint32
	name   string

	ready bool
	cond  *utils.Cond

	blocks map[chunk.CacheKey]*singleLoad
}

// locked by lock
func doNewSingleLoad(key chunk.CacheKey, blocks map[chunk.CacheKey]*singleLoad, lock sync.Locker, name string) *singleLoad {
	sl := &singleLoad{
		lock:   lock,
		key:    key,
		refs:   1,
		cond:   utils.NewCond(lock),
		blocks: blocks,
		name:   name,
	}
	blocks[key] = sl
	return sl
}

// return newly created singleLoad, or wait for existing one to be ready, and return nil if the
// existing one fails or times out.
func getSingleLoad(key chunk.CacheKey, blocks map[chunk.CacheKey]*singleLoad, lock sync.Locker, timeout time.Duration, name string) (*singleLoad, bool) {
	lock.Lock()
	defer lock.Unlock()
	created := false
	sl, ok := blocks[key]
	if !ok {
		created = true
		return doNewSingleLoad(key, blocks, lock, name), created
	}
	sl.refs += 1
	if sl.ready {
		return sl, created
	} else {
		start := time.Now()
		target := key.Target
		blk := key.Blk
		if sl.cond.WaitWithTimeout(timeout) {
			logger.Infof("%s: wait for target %s block %d to be ready timed out", sl.name, target, blk)
		}
		if sl.ready {
			logger.Tracef("%s: wait for target %s block %d for %s", sl.name, target, blk, time.Since(start))
			return sl, created
		}
	}
	// Not ready, wait timeout or previous load may hit error, close this one
	sl.closeLocked(true)
	return nil, false
}

func (sl *singleLoad) Read(p []byte) (int, error) {
	return sl.reader.Read(p)
}

func (sl *singleLoad) ReadAt(p []byte, off int64) (int, error) {
	return sl.reader.ReadAt(p, off)
}

func (sl *singleLoad) Close() error {
	return sl.closeLocked(false)
}

// ExportFd delegates to the backing reader. Because every ChunkReader
// implementation now exposes ExportFd, no type assertion is needed.
func (sl *singleLoad) ExportFd() (int, *utils.Memfd, error) {
	return sl.reader.ExportFd()
}

func (sl *singleLoad) closeLocked(locked bool) error {
	if !locked {
		sl.lock.Lock()
		defer sl.lock.Unlock()
	}
	target := sl.key.Target
	blk := sl.key.Blk
	sl.refs -= 1
	if sl.refs > 0 {
		logger.Tracef("%s: Cache file %s block %d still has %d users", sl.name, target, blk, sl.refs)
		return nil
	}

	// no users, release cache file
	logger.Tracef("%s: Close cache file %s block %d", sl.name, target, blk)
	delete(sl.blocks, sl.key)
	if sl.page != nil {
		sl.page.Release()
	}
	if sl.reader != nil {
		sl.reader.Close()
	}
	return nil
}

// locked
func (sl *singleLoad) setPageReadyLocked(page *chunk.Page) {
	if sl.page == nil {
		page.Acquire()
		sl.page = page
	}
	if sl.reader == nil {
		sl.reader = chunk.NewPageReader(page)
	}
	sl.ready = true
	sl.cond.Broadcast()
}

func (sl *singleLoad) setPageReady(page *chunk.Page) {
	sl.lock.Lock()
	sl.setPageReadyLocked(page)
	sl.lock.Unlock()
}

func (sl *singleLoad) setReaderReady(reader chunk.ChunkReader) {
	sl.lock.Lock()
	if sl.reader == nil {
		sl.reader = reader
	} else {
		reader.Close()
	}
	sl.ready = true
	sl.cond.Broadcast()
	sl.lock.Unlock()
}

// set singleLoad as fail if it's not ready, and return its readiness status
func (sl *singleLoad) setFail() bool {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	if sl.ready {
		return true
	}
	delete(sl.blocks, sl.key)
	sl.cond.Broadcast()
	return false
}
