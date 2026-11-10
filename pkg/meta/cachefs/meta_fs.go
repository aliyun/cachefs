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

package cachefsmeta

import (
	"io"
	"io/fs"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/cachefs/pkg/meta"
)

// MetaFS implements fs.FS over the in-memory inode tree held by MetaData.
// It is used in peer meta mode where all filesystem metadata has been
// pre-loaded from peers, allowing preload to walk the tree without accessing
// the source dir.
type MetaFS struct {
	m MetaData
}

// NewMetaFS returns an fs.FS backed by the in-memory inode tree of m.
func NewMetaFS(m MetaData) fs.FS {
	return &MetaFS{m: m}
}

// Open implements fs.FS.  name must be a clean slash-separated relative path
// as required by the fs.FS contract.
func (fsys *MetaFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}

	root := fsys.m.GetRoot()
	if root == nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}

	if name == "." {
		// Root has no parent Entry; synthesise one so metaFile can be uniform.
		rootEntry := &meta.Entry{Inode: root.Ino(), Name: []byte("."), Attr: root.GetAttr()}
		return &metaFile{m: fsys.m, inode: root, entry: rootEntry}, nil
	}

	// Walk path components through the in-memory children map.
	cur := root
	for _, seg := range strings.Split(name, "/") {
		child, ok := cur.Lookup(seg)
		if !ok {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		}
		cur = child
	}

	ent := &meta.Entry{Inode: cur.Ino(), Name: []byte(path.Base(name)), Attr: cur.GetAttr()}
	return &metaFile{m: fsys.m, inode: cur, entry: ent}, nil
}

// metaFile implements fs.File and fs.ReadDirFile.
type metaFile struct {
	m       MetaData
	inode   *Inode
	entry   *meta.Entry   // Entry for this file as seen by its parent; used by Stat()
	mu      sync.Mutex    // protects entries and dirIdx
	entries []fs.DirEntry // lazily built for directories
	dirIdx  int
}

func (f *metaFile) Stat() (fs.FileInfo, error) {
	return (&MetaDirEntry{E: f.entry, m: f.m, inodeType: f.inode.typ}).Info()
}

func (f *metaFile) Read([]byte) (int, error) {
	// MetaFS is walk-only; individual file data is never read through this FS.
	return 0, &fs.PathError{Op: "read", Path: f.inode.GetPath(), Err: fs.ErrInvalid}
}

func (f *metaFile) Close() error { return nil }

// ReadDir implements fs.ReadDirFile.
func (f *metaFile) ReadDir(n int) ([]fs.DirEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.entries == nil {
		f.entries = buildDirEntries(f.m, f.inode)
	}
	if n <= 0 {
		out := f.entries[f.dirIdx:]
		f.dirIdx = len(f.entries)
		return out, nil
	}
	if f.dirIdx >= len(f.entries) {
		return nil, io.EOF
	}
	end := f.dirIdx + n
	if end > len(f.entries) {
		end = len(f.entries)
	}
	out := f.entries[f.dirIdx:end]
	f.dirIdx = end
	if f.dirIdx >= len(f.entries) {
		return out, io.EOF
	}
	return out, nil
}

// buildDirEntries collects all children of dir into a DirEntry slice.
func buildDirEntries(m MetaData, dir *Inode) []fs.DirEntry {
	children := dir.Children()
	out := make([]fs.DirEntry, 0, len(children))
	for name, child := range children {
		ent := &meta.Entry{Inode: child.Ino(), Name: []byte(name), Attr: child.GetAttr()}
		out = append(out, &MetaDirEntry{E: ent, m: m, inodeType: child.typ})
	}
	return out
}

// MetaDirEntry wraps *meta.Entry to implement fs.DirEntry.
type MetaDirEntry struct {
	E         *meta.Entry
	m         MetaData
	inodeType uint8
}

func (e *MetaDirEntry) Name() string { return string(e.E.Name) }
func (e *MetaDirEntry) IsDir() bool {
	if e.E.Attr != nil {
		return e.E.Attr.Typ == meta.TypeDirectory
	}
	return e.inodeType == meta.TypeDirectory
}
func (e *MetaDirEntry) Type() fs.FileMode {
	if e.E.Attr != nil {
		return AttrToFileMode(e.E.Attr) & fs.ModeType
	}
	return AttrToFileMode(&meta.Attr{Typ: e.inodeType}) & fs.ModeType
}
func (e *MetaDirEntry) Info() (fs.FileInfo, error) {
	attr := e.E.Attr
	if attr == nil {
		if e.m == nil {
			return nil, fs.ErrNotExist
		}
		var loaded meta.Attr
		if eno := e.m.GetAttr(meta.Background, e.E.Inode, &loaded); eno != 0 {
			return nil, eno
		}
		attr = &loaded
	}
	return NewMetaFileInfo(e.E.Inode, string(e.E.Name), attr), nil
}

// MetaFileInfo implements fs.FileInfo.
type MetaFileInfo struct {
	Ino  meta.Ino
	name string
	Attr *meta.Attr
}

// NewMetaFileInfo constructs a MetaFileInfo for the given inode, name and attr.
func NewMetaFileInfo(ino meta.Ino, name string, attr *meta.Attr) *MetaFileInfo {
	return &MetaFileInfo{Ino: ino, name: name, Attr: attr}
}

func (fi *MetaFileInfo) Name() string      { return fi.name }
func (fi *MetaFileInfo) Size() int64       { return int64(fi.Attr.Length) }
func (fi *MetaFileInfo) IsDir() bool       { return fi.Attr.Typ == meta.TypeDirectory }
func (fi *MetaFileInfo) Mode() fs.FileMode { return AttrToFileMode(fi.Attr) }
func (fi *MetaFileInfo) ModTime() time.Time {
	return time.Unix(fi.Attr.Mtime, int64(fi.Attr.Mtimensec))
}
func (fi *MetaFileInfo) Sys() any {
	return makeSysStat(fi)
}

// AttrToFileMode converts a meta.Attr to fs.FileMode.
func AttrToFileMode(attr *meta.Attr) fs.FileMode {
	mode := fs.FileMode(attr.Mode) & 0777
	switch attr.Typ {
	case meta.TypeDirectory:
		mode |= fs.ModeDir
	case meta.TypeSymlink:
		mode |= fs.ModeSymlink
	case meta.TypeFIFO:
		mode |= fs.ModeNamedPipe
	case meta.TypeSocket:
		mode |= fs.ModeSocket
	case meta.TypeBlockDev:
		mode |= fs.ModeDevice
	case meta.TypeCharDev:
		mode |= fs.ModeDevice | fs.ModeCharDevice
	}
	return mode
}

// Compile-time interface checks.
var (
	_ fs.FS          = (*MetaFS)(nil)
	_ fs.File        = (*metaFile)(nil)
	_ fs.ReadDirFile = (*metaFile)(nil)
	_ fs.DirEntry    = (*MetaDirEntry)(nil)
	_ fs.FileInfo    = (*MetaFileInfo)(nil)
)
