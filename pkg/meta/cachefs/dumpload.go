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

package cachefsmeta

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aliyun/cachefs/pkg/meta"
)

type dumpedInode struct {
	Inode    uint64
	Name     []byte
	Path     string
	AltKey   string
	Attr     *meta.Attr
	Children []*dumpedInode
}

type dumpedCachefs struct {
	RootInode  *dumpedInode
	Symlinks   map[uint64][]byte
	InoAltMap  map[string]uint64
	NextInode  uint64
	TotalSize  uint64
	TotalInode uint64
}

// dump filesystem meta info
func (m *metaData) dumpMeta() (*dumpedCachefs, error) {
	if !m.metaReady.Load() {
		return nil, errors.New("meta info not ready")
	}
	if cached := m.dumpedMeta.Load(); cached != nil {
		return cached, nil
	}
	dumped := &dumpedCachefs{
		NextInode:  m.nextIno.Load(),
		TotalSize:  m.totalSize,
		TotalInode: m.totalInode,
	}
	dumped.Symlinks = m.dumpSymlinks()
	dumpedRoot, err := m.dumpInode("/", m.root)
	if err != nil {
		return nil, err
	}
	dumped.InoAltMap = m.dumpInoAltMap()
	dumped.RootInode = dumpedRoot
	m.dumpedMeta.Store(dumped)
	return dumped, nil
}

func (m *metaData) dumpInoAltMap() map[string]uint64 {
	dumped := make(map[string]uint64)
	for i := range m.inodeShards {
		shard := &m.inodeShards[i]
		shard.Lock()
		for ino, cino := range shard.inodes {
			if cino.altKey != "" {
				dumped[cino.altKey] = uint64(ino)
			}
		}
		shard.Unlock()
	}
	return dumped
}

func (m *metaData) dumpSymlinks() map[uint64][]byte {
	m.symLock.Lock()
	defer m.symLock.Unlock()
	dumped := make(map[uint64][]byte, len(m.symlinks))
	for ino, name := range m.symlinks {
		dumped[uint64(ino)] = name
	}
	return dumped
}

func (m *metaData) dumpInode(name string, cino *Inode) (*dumpedInode, error) {
	// Dump/restore is only available with ReaddirTimeout < 0, so attr sweeper is not
	// started, which ensures GetAttr() returns valid attr info.
	dumped := &dumpedInode{
		Inode:  uint64(cino.inode),
		Name:   []byte(name),
		Path:   cino.GetPath(),
		AltKey: cino.altKey,
		Attr:   cino.GetAttr(),
	}
	if cino.typ != meta.TypeDirectory {
		return dumped, nil
	}

	cino.childLock.Lock()
	defer cino.childLock.Unlock()
	for name, child := range cino.children {
		dumpedChild, err := m.dumpInode(name, child)
		if err != nil {
			return nil, err
		}
		dumped.Children = append(dumped.Children, dumpedChild)
	}
	return dumped, nil
}

func (m *metaData) loadMeta(dumped *dumpedCachefs) error {
	cacheSize := m.config.CacheSize
	totalSize := dumped.TotalSize
	if m.config.MetaConsumer() &&
		m.config.CacheSize < dumped.TotalSize {
		return fmt.Errorf("require at least %dMiB(%d) cache size, have %dMiB(%d)",
			totalSize>>20, totalSize, cacheSize>>20, cacheSize)
	}

	m.nextIno.Store(dumped.NextInode)
	m.totalSize = dumped.TotalSize
	m.totalInode = dumped.TotalInode
	m.loadSymlinks(dumped.Symlinks)

	err := m.loadInode(dumped.RootInode, nil)
	if err != nil {
		return err
	}
	m.loadInoAltMap(dumped.InoAltMap)
	m.metaReady.Store(true)
	return nil
}

// assume `inoMap` already loaded
func (m *metaData) loadInoAltMap(dumped map[string]uint64) {
	for altKey, ino := range dumped {
		cino, eno := m.GetInode(meta.Ino(ino))
		if eno != 0 {
			logger.Warnf("Inode %d not found, referenced by alt key %s", ino, altKey)
			continue
		}
		m.inoAltMap.Store(altKey, cino)
	}
}

func (m *metaData) loadSymlinks(dumped map[uint64][]byte) {
	m.symLock.Lock()
	defer m.symLock.Unlock()
	for ino, name := range dumped {
		m.symlinks[meta.Ino(ino)] = name
	}
}

func (m *metaData) loadInode(dumped *dumpedInode, parent *Inode) error {
	name := dumped.Path
	if parent != nil {
		name = filepath.Base(dumped.Path)
	}
	cino := NewInode(meta.Ino(dumped.Inode), parent, name, dumped.Attr.Typ, dumped.Attr)
	cino.altKey = dumped.AltKey
	shard := m.inodeShard(cino.inode)
	shard.Lock()
	ino, ok := shard.inodes[cino.inode]
	if ok {
		shard.Unlock()

		// Hardlinks share the same inode
		if ino.typ != meta.TypeDirectory {
			return nil
		}

		// Dir doesn't support hardlink
		return fmt.Errorf("Duplicated inode %d, previous \"%s\" parent %d, current \"%s\" parent %d",
			cino.inode, ino.GetPath(), ino.GetAttr().Parent, cino.GetPath(), cino.GetAttr().Parent)
	}
	shard.inodes[cino.inode] = cino
	shard.Unlock()
	m.cachedAttrs.Add(1)
	if cino.typ != meta.TypeDirectory {
		return nil
	}

	cino.readdir = true
	cino.readAt = time.Now()
	if cino.inode == meta.RootInode {
		m.root = cino
	}

	seen := make(map[string]meta.Ino, len(dumped.Children))
	for _, d := range dumped.Children {
		name := string(d.Name)
		previous, ok := seen[name]
		if ok {
			return fmt.Errorf("Duplicate child entry for %d:\"%s\", previous inode %d, current inode %d",
				cino.inode, cino.GetPath(), previous, d.Inode)
		}
		seen[name] = meta.Ino(d.Inode)
	}

	for _, d := range dumped.Children {
		if err := m.loadInode(d, cino); err != nil {
			return err
		}
		child, eno := m.GetInode(meta.Ino(d.Inode))
		if eno != 0 {
			return fmt.Errorf("get inode %d: %s", d.Inode, eno)
		}
		cino.childLock.Lock()
		cino.children[string(d.Name)] = child
		cino.childLock.Unlock()
	}

	// A nil parent marks the root of the directory tree, so this block runs once after the
	// whole tree is loaded. A hardlink inode records only the first looked-up path; other
	// hardlink paths reuse it through altKey without recording their own paths. For example,
	// /dir2/b was looked up first but /dir1/a is loaded first from the dump, the recursive load
	// initially assigns dir1 as the parent. This pass resets the parent to dir2 so GetPath
	// still returns /dir2/b.
	if parent == nil {
		// Map every directory's full path to its inode so the parent directory of each
		// recorded hardlink path can be found.
		dirs := make(map[string]meta.Ino)
		var collectDirs func(*dumpedInode)
		collectDirs = func(d *dumpedInode) {
			if d.Attr.Typ == meta.TypeDirectory {
				dirs[d.Path] = meta.Ino(d.Inode)
			}
			for _, child := range d.Children {
				collectDirs(child)
			}
		}
		collectDirs(dumped)

		// Ordinary inodes keep their dump-tree parent. For hardlinks, use the parent
		// directory of Path (the first looked-up path), not the parent of the current
		// dump-tree entry or attr.Parent.
		var restoreHardlinkParents func(*dumpedInode)
		restoreHardlinkParents = func(d *dumpedInode) {
			if d.AltKey != "" {
				ino, _ := m.GetInode(meta.Ino(d.Inode))
				parent, _ := m.GetInode(dirs[filepath.Dir(d.Path)])
				ino.parent = parent
			}
			for _, child := range d.Children {
				restoreHardlinkParents(child)
			}
		}
		restoreHardlinkParents(dumped)
	}
	return nil
}

// Walk filesystem tree from the given node
func (m *metaData) walkDir(rootIno meta.Ino) syscall.Errno {
	root, eno := m.GetInode(rootIno)
	if eno != 0 {
		return eno
	}
	if root.typ != meta.TypeDirectory {
		logger.Warnf("Meta walkdir: inode %d path \"%s\" is not a dir", root.inode, root.GetPath())
		return syscall.ENOTDIR
	}

	var entries []*meta.Entry
	ctx := meta.NewContext(uint32(os.Getpid()), 0, []uint32{0})
	eno = m.Readdir(ctx, rootIno, &entries)
	if eno != 0 {
		return eno
	}
	for _, entry := range entries {
		if bytes.Equal(entry.Name, []byte(".")) ||
			bytes.Equal(entry.Name, []byte("..")) {
			continue
		}
		attr := entry.Attr
		if attr.Typ != meta.TypeDirectory {
			continue
		}
		err := m.walkDir(entry.Inode)
		if err != 0 {
			return err
		}
	}
	return 0
}

// Go through all symlinks and cache their targets. Assume all symlink inodes are in inode map.
func (m *metaData) cacheSymlinks() {
	symlinks := []meta.Ino{}
	for i := range m.inodeShards {
		shard := &m.inodeShards[i]
		shard.Lock()
		for inode, cino := range shard.inodes {
			if cino.typ == meta.TypeSymlink {
				symlinks = append(symlinks, inode)
			}
		}
		shard.Unlock()
	}
	linkTarget := &[]byte{}
	for _, inode := range symlinks {
		_ = m.ReadLink(meta.Background, inode, linkTarget)
	}
}

func (m *metaData) updateTotalSize() {
	m.totalSize = 0
	for i := range m.inodeShards {
		shard := &m.inodeShards[i]
		shard.Lock()
		for _, cino := range shard.inodes {
			if cino.typ == meta.TypeFile {
				m.totalSize += cino.GetAttr().Length
			}
		}
		shard.Unlock()
	}
}
