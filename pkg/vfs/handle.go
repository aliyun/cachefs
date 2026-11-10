/*
 * Copyright 2023 Alibaba Cloud, Inc. or its affiliates.
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

	"github.com/aliyun/cachefs/pkg/meta"
	cachefsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
)

type handle struct {
	sync.Mutex
	inode  Ino
	fh     uint64
	length uint64
	target string
	blkcnt uint32
	cino   *cachefsmeta.Inode
	ctx    meta.Context

	// for dir
	children []*meta.Entry

	// for file
	sourceFile sourceFileStore
	srcTarget  string // saved to avoid rebuilding the full source path

	// internal files
	off     uint64
	data    []byte
	pending []byte
	bctx    meta.Context
}

func (h *handle) Write(buf []byte) (int, error) {
	h.Lock()
	defer h.Unlock()
	h.data = append(h.data, buf...)
	return len(buf), nil
}

func (c *Cachefs) newHandle(inode Ino, cino *cachefsmeta.Inode, attr *meta.Attr) *handle {
	var sourceFile sourceFileStore
	var length uint64
	var target string
	var srcTarget string
	if cino != nil {
		target = c.inodeToCacheTarget(cino, attr)
		if attr.Typ == meta.TypeFile {
			// Use source-relative path to access source file on disk
			srcTarget = cino.GetPath()
			sourceFile = c.source.newSourceFile(srcTarget, attr.Length)
			length = attr.Length
		}
	}

	blockSize := uint64(c.Conf.Store.BlockSize)
	c.handleM.Lock()
	defer c.handleM.Unlock()
	fh := c.nextfh
	blkcnt := uint32((length + blockSize - 1) / blockSize)
	h := &handle{
		inode:      inode,
		fh:         fh,
		length:     length,
		blkcnt:     blkcnt,
		target:     target,
		srcTarget:  srcTarget,
		cino:       cino,
		sourceFile: sourceFile,
		ctx:        meta.NewContext(0, 0, []uint32{0}),
	}
	c.nextfh++
	c.handles[inode] = append(c.handles[inode], h)
	return h
}

func (c *Cachefs) getHandle(inode Ino, fh uint64) *handle {
	c.handleM.Lock()
	defer c.handleM.Unlock()
	for _, f := range c.handles[inode] {
		if f.fh == fh {
			return f
		}
	}
	return nil
}

func (c *Cachefs) releaseHandle(inode Ino, fh uint64) {
	c.handleM.Lock()
	defer c.handleM.Unlock()
	hs := c.handles[inode]
	for i, f := range hs {
		if f.fh == fh {
			if i+1 < len(hs) {
				hs[i] = hs[len(hs)-1]
			}
			if len(hs) > 1 {
				c.handles[inode] = hs[:len(hs)-1]
			} else {
				delete(c.handles, inode)
			}
			if f.sourceFile != nil {
				f.sourceFile.release()
				f.sourceFile = nil
			}
			f.ctx.Cancel()
			break
		}
	}
}
