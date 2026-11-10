/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package chunk

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"sync/atomic"

	"github.com/aliyun/cachefs/pkg/digest"
	"github.com/aliyun/cachefs/pkg/utils"
	"golang.org/x/sys/unix"
)

var PageMemfd = utils.GetEnvBool("CFS_PAGE_MEMFD_ALLOC", false)
var pageStack = utils.GetEnvBool("CFS_PAGE_STACK", false)

// Page is a page with refcount
type Page struct {
	refs  int32
	mmap  bool
	Memfd *utils.Memfd
	dep   *Page
	Data  []byte
	orig  []byte
	stack []byte

	Hasher string
	Digest string
}

// NewPage create a new page.
func NewPage(data []byte) *Page {
	return &Page{refs: 1, Data: data}
}

// AllocPage allocates size bytes outside the Go heap, via memfd when PageMemfd
// is set and anonymous mmap otherwise, so cache data never pressures the
// runtime heap.
func AllocPage(size int, names ...string) (*Page, error) {
	if size <= 0 {
		panic("size of page should > 0")
	}

	var err error
	var page *Page
	typ := "Mmap"
	if PageMemfd {
		name := ""
		if len(names) > 0 {
			name = names[0]
		}
		typ = "Memfd"
		page, err = newMemfdPage(name, size)
	} else {
		page, err = newMmapPage(size)
	}
	if err != nil {
		logger.Warnf("%s allocate %v failed: %v", typ, size, err)
		return nil, fmt.Errorf("allocate %s page of %d bytes: %w", typ, size, err)
	}
	if pageStack {
		page.stack = debug.Stack()
	}
	runtime.SetFinalizer(page, func(p *Page) {
		refcnt := atomic.LoadInt32(&p.refs)
		if refcnt != 0 {
			logger.Errorf("refcount of page %p (%d bytes) is not zero: %d, created by: %s", p, cap(p.Data), refcnt, string(p.stack))
			if refcnt > 0 {
				p.Release()
			}
		}
	})
	return page, nil
}

// Allocate memory via mmap
func newMmapPage(size int) (*Page, error) {
	p, err := utils.Malloc(size)
	if err != nil {
		return nil, err
	}
	return &Page{refs: 1, mmap: true, Data: p, orig: p}, nil
}

// Allocate memory via memfd
func newMemfdPage(name string, size int) (*Page, error) {
	memfd, err := utils.MemfdAlloc(name, size)
	if err != nil {
		return nil, err
	}
	return &Page{refs: 1, Memfd: memfd, Data: memfd.Data()}, nil
}

func (p *Page) Slice(off, len int) *Page {
	p.Acquire()
	np := NewPage(p.Data[off : off+len])
	np.dep = p
	return np
}

// Acquire increase the refcount
func (p *Page) Acquire() {
	if pageStack {
		p.stack = append(p.stack, debug.Stack()...)
	}
	atomic.AddInt32(&p.refs, 1)
}

// Release decrease the refcount
func (p *Page) Release() {
	if pageStack {
		p.stack = append(p.stack, debug.Stack()...)
	}
	if atomic.AddInt32(&p.refs, -1) == 0 {
		if p.mmap {
			err := utils.Mfree(p.orig)
			if err != nil {
				logger.Warnf("utils.Mfree %p failed: %s", p.orig, err)
			}
		} else if p.Memfd != nil {
			fd := p.Memfd.Fd
			if err := p.Memfd.Free(); err != nil {
				logger.Warnf("Memfd.Free fd %d failed: %s", fd, err)
			}
		}
		if p.dep != nil {
			p.dep.Release()
			p.dep = nil
		}
		p.Memfd = nil
		p.Data = nil
		p.orig = nil
		p.Digest = ""
		p.Hasher = ""
	}
}

// Get existing or compute digest of the page
func (p *Page) GenDigest(hasher string) (string, error) {
	if p.Digest != "" {
		if p.Hasher == hasher {
			return p.Digest, nil
		} else {
			return "", fmt.Errorf("page digest has been computed by hasher %s", p.Hasher)
		}
	}
	digest, err := digest.GetDigest(hasher, p.Data)
	if err != nil {
		return "", err
	}
	p.Hasher = hasher
	p.Digest = digest
	return digest, nil
}

type pageReader struct {
	p   *Page
	off int
}

func NewPageReader(p *Page) *pageReader {
	p.Acquire()
	return &pageReader{p, 0}
}

func (r *pageReader) Read(buf []byte) (int, error) {
	n, err := r.ReadAt(buf, int64(r.off))
	r.off += n
	return n, err
}

func (r *pageReader) ReadAt(buf []byte, off int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if r.p == nil {
		return 0, errors.New("page is already released")
	}
	if int(off) == len(r.p.Data) {
		return 0, io.EOF
	}
	n := copy(buf, r.p.Data[off:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (r *pageReader) Close() error {
	if r.p != nil {
		r.p.Release()
		r.p = nil
	}
	return nil
}

func (r *pageReader) ExportFd() (int, *utils.Memfd, error) {
	if r.p == nil || r.p.Memfd == nil {
		return -1, nil, errors.New("page has no memfd backing")
	}
	fd, err := r.p.Memfd.ExportFd()
	if err != nil {
		return -1, nil, err
	}
	return fd, r.p.Memfd, nil
}

// osFileReader wraps a plain *os.File to implement ChunkReader.
type osFileReader struct {
	*os.File
}

func newOsFileReader(f *os.File) *osFileReader {
	return &osFileReader{f}
}

// ExportFd hands out a dup of the cache file's fd. It reports no *utils.Memfd
// because there is nothing to count: the data lives in a filesystem inode that
// never comes from the block pool, and file-backend eviction is an unlink, which
// leaves the dup'd fd holding the old inode alive and untouched.
func (r *osFileReader) ExportFd() (int, *utils.Memfd, error) {
	fd, err := unix.Dup(int(r.File.Fd()))
	if err != nil {
		return -1, nil, err
	}
	return fd, nil, nil
}
