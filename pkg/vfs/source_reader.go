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
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/meta"
	"github.com/aliyun/cachefs/pkg/utils"
)

type sourceReader struct {
	sync.Mutex
	source  sourceStore
	fs      *Cachefs
	blocks  map[chunk.CacheKey]*singleLoad
	timeout time.Duration
	name    string
}

func newSourceReader(fs *Cachefs, source sourceStore, timeout int) *sourceReader {
	return &sourceReader{
		fs:      fs,
		source:  source,
		blocks:  make(map[chunk.CacheKey]*singleLoad),
		timeout: time.Second * time.Duration(timeout),
		name:    "source reader",
	}
}

func (sr *sourceReader) getSourceLoad(sf sourceFileStore, target string, length uint64, blk uint32, cache bool) (rsl *singleLoad, rerr error) {
	var sl *singleLoad
	var created bool
	key := chunk.CacheKey{Target: target, Blk: blk}

	// must get a singleLoad for source read
	for {
		sl, created = getSingleLoad(key, sr.blocks, sr, sr.timeout, sr.name)
		if sl != nil {
			break
		}
	}
	if !created {
		return sl, nil
	}

	// read data from source
	defer func() {
		if rerr != nil {
			if sl.setFail() {
				// someone else has set singleLoad ready, override return value
				rsl = sl
				rerr = nil
			} else {
				sourceReadErrors.Inc()
			}
		}
	}()
	atomic.AddInt32(&sourceReadInFlight, 1)
	defer atomic.AddInt32(&sourceReadInFlight, -1)

	blockSize := sr.fs.Conf.Store.BlockSize
	readoff := int64(blk) * int64(blockSize)
	blklen := int(utils.GetBlockSize(sr.fs.Conf.Store.BlockSize, length, blk))

	page, err := chunk.AllocPage(blklen, key.String())
	if err != nil {
		return nil, err
	}
	defer page.Release()
	start := time.Now()
	read, err := sf.ReadAt(page.Data, readoff)
	if err != nil {
		return nil, err
	}
	if read != blklen {
		logger.Warnf("read source %s offset %v got %v, expect %v", target, readoff, read, blklen)
		return nil, io.EOF
	}
	usedTime := time.Since(start)
	sourceReadHist.Observe(usedTime.Seconds())
	sourceReadBytes.Add(float64(read))
	sourceReads.Inc()

	page.Acquire()
	if sr.fs.addPendingCache(target, blk) {
		go func() {
			start = time.Now()
			err := sr.fs.cache.addToStore(meta.Background, target, length, blk, page, cache)
			if err == nil {
				logger.Debugf("Cached %s block %v in %s", target, blk, time.Since(start))
			} else {
				logger.Infof("Cache %s block %v size %d: %s", target, blk, blklen, err)
			}
			sr.fs.removePendingCache(target, blk)
			page.Release()
		}()
	} else {
		// Another goroutine owns the pending cache write; release this caller's extra page reference.
		page.Release()
	}

	sl.setPageReady(page)
	return sl, nil
}

func (sr *sourceReader) readFromSource(sf sourceFileStore, target string, length uint64, blk uint32, cache bool) (chunk.ChunkReader, error) {
	if sr.fs.Conf.Meta.MetaConsumer() {
		return nil, errors.New("in sourceless meta consumer mode, unable to read source")
	}

	sl, err := sr.getSourceLoad(sf, target, length, blk, cache)
	if err != nil {
		return nil, err
	}
	return sl, nil
}
