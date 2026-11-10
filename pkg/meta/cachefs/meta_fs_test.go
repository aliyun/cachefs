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
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/meta"
	"github.com/stretchr/testify/assert"
)

// buildTree creates a small in-memory inode tree for tests:
//
//	. (root, dir)
//	├── dir1/          (dir)
//	│   ├── file1.txt  (regular, 100 bytes, Nlink=2)
//	│   └── link1      (hardlink of file1.txt, same ino)
//	├── file2.txt      (regular, 200 bytes)
//	└── sym1           (symlink)
//
// Because this test lives inside the cachefsmeta package it can write to
// the unexported children map directly, without any AddChild helper.
func buildTree() (*metaData, map[meta.Ino]*Inode) {
	mkAttr := func(typ uint8, size uint64, mode uint16) *meta.Attr {
		return &meta.Attr{Typ: typ, Mode: mode, Length: size, Nlink: 1, Mtime: 1000}
	}
	ino := func(n uint64) meta.Ino { return meta.Ino(n) }

	rootAttr := mkAttr(meta.TypeDirectory, 0, 0755)
	dir1Attr := mkAttr(meta.TypeDirectory, 0, 0755)
	file1Attr := &meta.Attr{Typ: meta.TypeFile, Mode: 0644, Length: 100, Nlink: 2, Mtime: 1000}
	file2Attr := mkAttr(meta.TypeFile, 200, 0644)
	sym1Attr := mkAttr(meta.TypeSymlink, 10, 0777)

	root := NewInode(ino(1), nil, "/", rootAttr.Typ, rootAttr)
	dir1 := NewInode(ino(2), root, "dir1", dir1Attr.Typ, dir1Attr)
	file1 := NewInode(ino(3), dir1, "file1.txt", file1Attr.Typ, file1Attr)
	file2 := NewInode(ino(4), root, "file2.txt", file2Attr.Typ, file2Attr)
	sym1 := NewInode(ino(5), root, "sym1", sym1Attr.Typ, sym1Attr)

	// Populate children maps directly (white-box access).
	root.children["dir1"] = dir1
	root.children["file2.txt"] = file2
	root.children["sym1"] = sym1
	dir1.children["file1.txt"] = file1
	dir1.children["link1"] = file1

	inoMap := map[meta.Ino]*Inode{
		ino(1): root,
		ino(2): dir1,
		ino(3): file1,
		ino(4): file2,
		ino(5): sym1,
	}

	m := &metaData{
		root:        root,
		inodeShards: newInodeShards(),
	}
	for _, cino := range inoMap {
		putTestInode(m, cino)
	}
	m.metaReady.Store(true)
	return m, inoMap
}

// MetaFS.Open tests
func TestMetaFSOpen(t *testing.T) {
	normalM, _ := buildTree()
	nilRootM := &metaData{inodeShards: newInodeShards()}

	tests := []struct {
		name string
		m    *metaData
		path string
		// wantErr: expected error type when non-nil; set pathErr to also check *fs.PathError.Err
		wantErr bool
		pathErr error
		// wantInfo: optional checks on Stat() when no error
		wantName  string
		wantSize  int64
		wantIsDir bool
	}{
		{
			name:    "absolute path is invalid",
			m:       normalM,
			path:    "/absolute",
			wantErr: true,
			pathErr: fs.ErrInvalid,
		},
		{
			name:      "root dot",
			m:         normalM,
			path:      ".",
			wantName:  ".",
			wantIsDir: true,
		},
		{
			name:    "nil root returns ErrNotExist",
			m:       nilRootM,
			path:    ".",
			wantErr: true,
			pathErr: fs.ErrNotExist,
		},
		{
			name:     "top-level file",
			m:        normalM,
			path:     "file2.txt",
			wantName: "file2.txt",
			wantSize: 200,
		},
		{
			name:     "nested file",
			m:        normalM,
			path:     "dir1/file1.txt",
			wantName: "file1.txt",
			wantSize: 100,
		},
		{
			name:    "non-existent top-level",
			m:       normalM,
			path:    "nonexistent",
			wantErr: true,
			pathErr: fs.ErrNotExist,
		},
		{
			name:    "non-existent nested",
			m:       normalM,
			path:    "dir1/nothere.txt",
			wantErr: true,
		},
		{
			name:    "dot-dot is invalid",
			m:       normalM,
			path:    "..",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fsys := NewMetaFS(tc.m)
			f, err := fsys.Open(tc.path)
			if tc.wantErr {
				if !assert.Error(t, err) {
					return
				}
				if tc.pathErr != nil {
					var pe *fs.PathError
					if assert.ErrorAs(t, err, &pe) {
						assert.Equal(t, tc.pathErr, pe.Err)
					}
				}
				return
			}
			if !assert.NoError(t, err) {
				return
			}
			defer f.Close()
			info, err := f.Stat()
			if !assert.NoError(t, err) {
				return
			}
			if tc.wantName != "" {
				assert.Equal(t, tc.wantName, info.Name())
			}
			if tc.wantSize != 0 {
				assert.Equal(t, tc.wantSize, info.Size())
			}
			assert.Equal(t, tc.wantIsDir, info.IsDir())
		})
	}
}

// metaFile.Read always returns ErrInvalid (MetaFS is walk-only)
func TestMetaFileRead_ReturnsInvalid(t *testing.T) {
	m, _ := buildTree()
	fsys := NewMetaFS(m)

	f, err := fsys.Open("file2.txt")
	if !assert.NoError(t, err) {
		return
	}
	defer f.Close()

	n, err := f.Read(make([]byte, 10))
	assert.Equal(t, 0, n)
	if !assert.Error(t, err) {
		return
	}
	var pe *fs.PathError
	if assert.ErrorAs(t, err, &pe) {
		assert.Equal(t, fs.ErrInvalid, pe.Err)
	}
}

// metaFile.ReadDir tests
func TestMetaFileReadDir(t *testing.T) {
	m, _ := buildTree()
	fsys := NewMetaFS(m)

	t.Run("all entries", func(t *testing.T) {
		dir, err := fsys.Open(".")
		if !assert.NoError(t, err) {
			return
		}
		defer dir.Close()
		rdf, ok := dir.(fs.ReadDirFile)
		if !assert.True(t, ok, "root should implement fs.ReadDirFile") {
			return
		}
		entries, err := rdf.ReadDir(-1)
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, entries, 3)
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		sort.Strings(names)
		assert.Equal(t, []string{"dir1", "file2.txt", "sym1"}, names)
	})

	t.Run("paged read", func(t *testing.T) {
		dir, err := fsys.Open("dir1")
		if !assert.NoError(t, err) {
			return
		}
		defer dir.Close()
		rdf := dir.(fs.ReadDirFile)
		// dir1 has 2 children: file1.txt and link1.
		first, err := rdf.ReadDir(1)
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, first, 1)
		// Reading the last entry returns it together with io.EOF per ReadDirFile contract.
		second, err := rdf.ReadDir(1)
		assert.Equal(t, io.EOF, err)
		assert.Len(t, second, 1)
		// Exhausted; subsequent call returns nil slice and io.EOF.
		_, err = rdf.ReadDir(1)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("n=0 reads all", func(t *testing.T) {
		dir, err := fsys.Open(".")
		if !assert.NoError(t, err) {
			return
		}
		defer dir.Close()
		rdf := dir.(fs.ReadDirFile)
		entries, err := rdf.ReadDir(0)
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, entries, 3)
	})

	t.Run("concurrent read does not panic", func(t *testing.T) {
		dir, err := fsys.Open(".")
		if !assert.NoError(t, err) {
			return
		}
		defer dir.Close()
		rdf := dir.(fs.ReadDirFile)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = rdf.ReadDir(-1)
			}()
		}
		wg.Wait()
	})
}

func TestBuildDirEntries(t *testing.T) {
	m, inodes := buildTree()
	entries := buildDirEntries(m, inodes[2])
	assert.Len(t, entries, 2)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
		dirEntry, ok := entry.(*MetaDirEntry)
		if !assert.True(t, ok) {
			continue
		}
		assert.Equal(t, meta.Ino(3), dirEntry.E.Inode)
		assert.Equal(t, inodes[3].GetAttr(), dirEntry.E.Attr)
		assert.Equal(t, inodes[3].typ, dirEntry.inodeType)
	}
	assert.ElementsMatch(t, []string{"file1.txt", "link1"}, names)
}

// MetaDirEntry tests
func TestMetaDirEntry(t *testing.T) {
	tests := []struct {
		name     string
		attr     *meta.Attr
		entry    string
		wantDir  bool
		wantType fs.FileMode
		wantSize int64
	}{
		{
			name:     "regular file",
			attr:     &meta.Attr{Typ: meta.TypeFile, Mode: 0644, Length: 512, Nlink: 1, Mtime: 2000},
			entry:    "foo.txt",
			wantDir:  false,
			wantType: fs.FileMode(0), // regular files have zero ModeType bits
			wantSize: 512,
		},
		{
			name:     "directory",
			attr:     &meta.Attr{Typ: meta.TypeDirectory, Mode: 0755, Nlink: 2},
			entry:    "subdir",
			wantDir:  true,
			wantType: fs.ModeDir,
		},
		{
			name:     "symlink",
			attr:     &meta.Attr{Typ: meta.TypeSymlink, Mode: 0777, Length: 8, Nlink: 1},
			entry:    "lnk",
			wantDir:  false,
			wantType: fs.ModeSymlink,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := &MetaDirEntry{E: &meta.Entry{Name: []byte(tc.entry), Attr: tc.attr}}
			assert.Equal(t, tc.entry, e.Name())
			assert.Equal(t, tc.wantDir, e.IsDir())
			assert.Equal(t, tc.wantType, e.Type())
			if tc.wantSize > 0 {
				info, err := e.Info()
				if !assert.NoError(t, err) {
					return
				}
				assert.Equal(t, tc.entry, info.Name())
				assert.Equal(t, tc.wantSize, info.Size())
				assert.Equal(t, tc.wantDir, info.IsDir())
			}
		})
	}
}

// MetaFileInfo tests
func TestMetaFileInfo(t *testing.T) {
	t.Run("Sys returns Stat_t with all fields", func(t *testing.T) {
		attr := &meta.Attr{Typ: meta.TypeFile, Mode: 0644, Length: 1024, Nlink: 3, Mtime: 3000, Mtimensec: 500}
		fi := NewMetaFileInfo(meta.Ino(42), "hard.txt", attr)
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !assert.True(t, ok, "Sys() must return *syscall.Stat_t") {
			return
		}
		assert.Equal(t, uint64(42), st.Ino)
		assert.Equal(t, uint64(3), uint64(st.Nlink))
		assert.Equal(t, int64(1024), st.Size)
		assert.Equal(t, uint32(0100644), st.Mode)
		assert.Equal(t, int64(3000), st.Mtim.Sec)
		assert.Equal(t, int64(500), st.Mtim.Nsec)
	})

	t.Run("fs.FileInfo fields", func(t *testing.T) {
		attr := &meta.Attr{Typ: meta.TypeFile, Mode: 0640, Length: 2048, Mtime: 5000, Mtimensec: 123}
		fi := NewMetaFileInfo(meta.Ino(7), "myfile", attr)
		assert.Equal(t, "myfile", fi.Name())
		assert.Equal(t, int64(2048), fi.Size())
		assert.False(t, fi.IsDir())
		assert.Equal(t, time.Unix(5000, 123), fi.ModTime())
		assert.Equal(t, fs.FileMode(0640), fi.Mode()&0777)
	})
}

// AttrToFileMode tests
func TestAttrToFileMode(t *testing.T) {
	cases := []struct {
		typ      uint8
		wantBits fs.FileMode
	}{
		{meta.TypeFile, 0},
		{meta.TypeDirectory, fs.ModeDir},
		{meta.TypeSymlink, fs.ModeSymlink},
		{meta.TypeFIFO, fs.ModeNamedPipe},
		{meta.TypeSocket, fs.ModeSocket},
		{meta.TypeBlockDev, fs.ModeDevice},
		{meta.TypeCharDev, fs.ModeDevice | fs.ModeCharDevice},
	}
	for _, tc := range cases {
		attr := &meta.Attr{Typ: tc.typ, Mode: 0644}
		got := AttrToFileMode(attr)
		assert.Equal(t, tc.wantBits, got&^fs.ModePerm, "typ=%d", tc.typ)
		assert.Equal(t, fs.FileMode(0644), got&fs.ModePerm, "typ=%d", tc.typ)
	}
}

// fs.WalkDir integration
func TestMetaFSWalkDir(t *testing.T) {
	m, _ := buildTree()
	fsys := NewMetaFS(m)

	t.Run("all paths", func(t *testing.T) {
		var visited []string
		err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			visited = append(visited, path)
			return nil
		})
		if !assert.NoError(t, err) {
			return
		}
		sort.Strings(visited)
		assert.Equal(t, []string{".", "dir1", "dir1/file1.txt", "dir1/link1", "file2.txt", "sym1"}, visited)
	})

	t.Run("hardlink dedup", func(t *testing.T) {
		// Simulate the walkFunc logic in cachefs_preload.go:
		// files with Nlink>1 sharing the same Ino are only processed once.
		fileSet := make(map[uint64]struct{})
		var regularFiles []string
		err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil || path == "." {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() || info.Size() == 0 {
				return nil
			}
			st, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return nil
			}
			if st.Nlink > 1 {
				if _, seen := fileSet[st.Ino]; seen {
					return nil
				}
				fileSet[st.Ino] = struct{}{}
			}
			regularFiles = append(regularFiles, path)
			return nil
		})
		if !assert.NoError(t, err) {
			return
		}
		// file2.txt (Nlink=1) + only one of {file1.txt, link1} (Nlink=2, same ino).
		assert.Len(t, regularFiles, 2)
		assert.Contains(t, regularFiles, "file2.txt")
	})

	t.Run("sub path", func(t *testing.T) {
		var visited []string
		err := fs.WalkDir(fsys, "dir1", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			visited = append(visited, path)
			return nil
		})
		if !assert.NoError(t, err) {
			return
		}
		sort.Strings(visited)
		assert.Equal(t, []string{"dir1", "dir1/file1.txt", "dir1/link1"}, visited)
	})
}
