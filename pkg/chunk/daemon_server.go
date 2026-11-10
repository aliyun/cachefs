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

package chunk

import (
	"context"
	"fmt"
	"os"
)

func (d *daemon) loadRemote(target string, blk, blklen uint32) error {
	// Load from remote peer
	s := d.store
	page, err := s.loadRemote(context.Background(), target, blk, blklen)
	if err != nil {
		logger.Debugf("Daemon GetBlock target %s block %d: %s", target, blk, err)
		return ErrNotFound
	}
	defer page.Release()
	file, err := s.bcache.prepareBlockFile(target, blk, blklen)
	if err != nil {
		return fmt.Errorf("prepare block file: %s", err)
	}
	if file == nil {
		return ErrExists
	}
	defer file.Close()

	// Write data to cache file
	n, err := file.WriteAt(page.Data, 0)
	if err != nil {
		// discard temp cache file on error
		s.bcache.commitBlockFile(target, blk, false)
		return fmt.Errorf("write block file: %s", err)
	}
	if n != int(blklen) {
		// discard temp cache file on error
		s.bcache.commitBlockFile(target, blk, false)
		return fmt.Errorf("write block file expect %d got %d", blklen, n)
	}
	err = s.bcache.commitBlockFile(target, blk, true)
	if err != nil {
		return fmt.Errorf("commit block file: %s", err)
	}
	return nil
}

// Get existing cache block file
func (d *daemon) GetBlock(req *Req) (res *RespInfo, f *os.File, sts uint8) {
	reqInfo := req.Info[0]
	logger.Debugf("Daemon GetBlock %v", reqInfo)
	config := d.store.config
	target := d.store.genTarget(reqInfo.Target)
	blknum := reqInfo.BlockNum
	blksiz := reqInfo.BlockSize
	blklen := reqInfo.Size
	info := RespInfo{
		Size: uint64(blklen),
	}

	if blklen > config.BlockSize {
		logger.Warnf("Daemon GetBlock target %s block %d: block len(%d) too large", target, blknum, blklen)
		info.Message = fmt.Sprintf("requested size(%d) greater than block size(%d)", blklen, config.BlockSize)
		return &info, nil, STS_FAIL
	}
	if blksiz != config.BlockSize {
		logger.Warnf("Daemon GetBlock target %s block %d: block size mismatch", target, blknum)
		info.Message = fmt.Sprintf("block size mismatch: %d != %d", blksiz, config.BlockSize)
		return &info, nil, STS_FAIL
	}

	defer func() {
		if sts == STS_OK {
			d.cacheHits.Inc()
			d.cacheHitBytes.Add(float64(blklen))
		} else {
			d.cacheMiss.Inc()
			d.cacheMissBytes.Add(float64(blklen))
		}
	}()
	s := d.store
	file, err := s.bcache.getBlockFile(target, blknum)
	if err != nil {
		// not in local cache, try load from remote and write to local cache
		err = d.loadRemote(target, blknum, blklen)
		if err != nil && err != ErrExists {
			info.Message = err.Error()
			if err == ErrNotFound {
				return &info, nil, STS_NOTFOUND
			} else {
				logger.Warnf("Daemon GetBlock target %s block %d: %s", target, blknum, err)
				return &info, nil, STS_FAIL
			}
		}
		file, err = s.bcache.getBlockFile(target, blknum)
		if err != nil {
			// cache might be removed right away due to cache eviction
			logger.Infof("Daemon GetBlock target %s block %d after load peer: %s", target, blknum, err)
			return &info, nil, STS_NOTFOUND
		}
	}

	if fi, err := file.Stat(); err != nil {
		file.Close()
		msg := fmt.Sprintf("stat cache file: %s", err)
		logger.Warnf("Daemon GetBlock target %s block %d: %s", target, blknum, msg)
		info.Message = msg
		return &info, nil, STS_FAIL
	} else {
		fileSize := fi.Size()
		if int64(blklen) != fileSize {
			msg := fmt.Sprintf("cache file size mismatch: want %d, on-disk %d, file %s",
				blklen, fileSize, file.Name())
			logger.Warnf("Daemon GetBlock target %s block %d: %s", target, blknum, msg)
			info.Message = msg
			file.Close()
			return &info, nil, STS_FAIL
		}
	}
	return &info, file, STS_OK
}

// Fill in cache block file
func (d *daemon) FillBlock(req *Req) (res *RespInfo, sts uint8) {
	reqInfo := req.Info[0]
	logger.Debugf("Daemon FillBlock %v", reqInfo)
	config := d.store.config
	target := d.store.genTarget(reqInfo.Target)
	blknum := reqInfo.BlockNum
	blksiz := reqInfo.BlockSize
	blklen := reqInfo.Size
	info := RespInfo{
		Size: uint64(blklen),
	}

	if blklen > config.BlockSize {
		logger.Warnf("Daemon FillBlock target %s block %d: block len(%d) too large", target, blknum, blklen)
		info.Message = fmt.Sprintf("requested size(%d) greater than block size(%d)", blklen, config.BlockSize)
		return &info, STS_FAIL
	}
	if blksiz != config.BlockSize {
		logger.Warnf("Daemon FillBlock target %s block %d: block size mismatch", target, blknum)
		info.Message = fmt.Sprintf("block size mismatch: %d != %d", blksiz, config.BlockSize)
		return &info, STS_FAIL
	}

	defer func() {
		if sts == STS_OK {
			d.cacheHits.Inc()
			d.cacheHitBytes.Add(float64(blklen))
		} else {
			d.cacheMiss.Inc()
			d.cacheMissBytes.Add(float64(blklen))
		}
	}()
	err := d.store.doFill(context.Background(), target, blknum, blklen)
	if err != nil {
		info.Message = err.Error()
		if err == ErrNotFound {
			return &info, STS_NOTFOUND
		} else {
			logger.Warnf("Daemon FillBlock target %s block %d: %s", target, blknum, err)
			return &info, STS_FAIL
		}
	}
	return &info, STS_OK
}

// Prepare a temporary cache block file for writing to add new cache data.
func (d *daemon) PutBlock(req *Req) (*RespInfo, *os.File, uint8) {
	reqInfo := req.Info[0]
	logger.Debugf("Daemon PutBlock %v", reqInfo)
	config := d.store.config
	target := d.store.genTarget(reqInfo.Target)
	blknum := reqInfo.BlockNum
	blksiz := reqInfo.BlockSize
	blklen := reqInfo.Size
	info := RespInfo{
		Size: uint64(blklen),
	}

	if blklen > config.BlockSize {
		logger.Warnf("Daemon PutBlock target %s block %d: block len(%d) too large", target, blknum, blklen)
		info.Message = fmt.Sprintf("cache data size(%d) greater than block size(%d)", blklen, config.BlockSize)
		return &info, nil, STS_FAIL
	}
	if blksiz != config.BlockSize {
		info.Message = fmt.Sprintf("block size mismatch: %d != %d", blksiz, config.BlockSize)
		return &info, nil, STS_FAIL
	}

	// Check if already cached
	s := d.store
	if s.bcache.check(target, blknum) {
		return &info, nil, STS_EXISTS
	}

	// Not cached if we got here, prepare & send back a cache file so client could write cache
	// data to it.
	file, err := s.bcache.prepareBlockFile(target, blknum, blklen)
	if err != nil {
		info.Message = fmt.Sprintf("prepare cache file: %s", err)
		return &info, nil, STS_FAIL
	}
	if file == nil {
		return &info, nil, STS_EXISTS
	}
	return &info, file, STS_OK
}

// Commit previously prepared temporary cache file to cache store.
// Discard it if `req.Size` is 0.
func (d *daemon) CmtBlock(req *Req) (*RespInfo, uint8) {
	reqInfo := req.Info[0]
	logger.Debugf("Daemon CmtBlock %v", reqInfo)
	config := d.store.config
	target := d.store.genTarget(reqInfo.Target)
	blknum := reqInfo.BlockNum
	blksiz := reqInfo.BlockSize
	blklen := reqInfo.Size
	info := RespInfo{}

	if blklen > config.BlockSize {
		logger.Warnf("Daemon CmtBlock target %s block %d: block len(%d) too large", target, blknum, blklen)
		info.Message = fmt.Sprintf("cache data size(%d) greater than block size(%d)", blklen, config.BlockSize)
		return &info, STS_FAIL
	}
	if blksiz != config.BlockSize {
		info.Message = fmt.Sprintf("block size mismatch: %d != %d", blksiz, config.BlockSize)
		return &info, STS_FAIL
	}

	// Commit block for PutBlock request
	err := d.store.bcache.commitBlockFile(target, blknum, blklen != 0)
	if err != nil {
		info.Message = fmt.Sprintf("commit cache file: %s", err)
		return &info, STS_FAIL
	}
	return &info, STS_OK
}

func (d *daemon) Prepare(req *Req) (*RespInfo, uint8) {
	reqInfo := req.Info[0]
	logger.Debugf("Daemon Prepare %v", reqInfo)
	target := d.store.genTarget(reqInfo.Target)
	length := reqInfo.Length
	info := RespInfo{}

	err := d.store.bcache.prepareFile(target, length)
	if err != nil {
		info.Message = err.Error()
		if err == ErrExists {
			return &info, STS_EXISTS
		} else {
			return &info, STS_FAIL
		}
	}
	return &info, STS_OK
}

func (d *daemon) Commit(req *Req) (*RespInfo, uint8) {
	reqInfo := req.Info[0]
	logger.Debugf("Daemon Commit %v", reqInfo)
	target := d.store.genTarget(reqInfo.Target)
	info := RespInfo{}

	err := d.store.bcache.commitFile(target)
	if err != nil {
		info.Message = err.Error()
		if err == ErrNotFound {
			return &info, STS_NOTFOUND
		} else {
			return &info, STS_FAIL
		}
	}
	return &info, STS_OK
}

// Get block size of daemon
func (d *daemon) GetBlockSize() (*RespInfo, uint8) {
	logger.Debug("Daemon GetBlockSize")
	info := RespInfo{
		Size: uint64(d.store.config.BlockSize),
	}
	return &info, STS_OK
}
