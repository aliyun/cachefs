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
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/aliyun/cachefs/pkg/meta"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type MockRpcService struct{}

func (r *MockRpcService) GetServer() *grpc.Server { return grpc.NewServer() }
func (r *MockRpcService) Addr() string            { return "" }
func (r *MockRpcService) Serve()                  {}
func (r *MockRpcService) Stop()                   {}

// setupTestDirTree creates a complex temporary directory tree under root
// with various file types and up to 4 levels of depth.
// It is intended for use in unit tests that require realistic filesystem structures.
func setupTestDirTree(t *testing.T, root string) {
	t.Helper()

	// File contents for regular files
	content := []byte("test content\n")

	// Major/minor numbers for device files (arbitrary)
	major, minor := uint32(1), uint32(3) // typically used for /dev/null, /dev/zero

	cases := []struct {
		path    string
		mode    os.FileMode
		data    []byte // for regular files
		target  string // for symlink/hardlink
		devType uint32 // for device files: makedev(major, minor)
		isDir   bool
		create  func() error
	}{
		// regular file, dir, symlink
		{"file.txt", 0644, content, "", 0, false, nil},
		{"dir1", 0755, nil, "", 0, true, nil},
		{"symlink-to-file.txt", 0777, nil, "file.txt", 0, false, nil},

		{"dir1/file2.txt", 0644, content, "", 0, false, nil},
		{"dir1/subdir", 0755, nil, "", 0, true, nil},
		{"dir1/symlink-to-subdir", 0777, nil, "subdir", 0, false, nil},

		{"dir1/subdir/block-dev", 0660, nil, "", makedev(major, minor), false, nil},
		{"dir1/subdir/char-dev", 0660, nil, "", makedev(1, 5), false, nil},
		{"dir1/subdir/fifo", 0666, nil, "", 0, false, nil}, // FIFO (named pipe)

		{"dir1/subdir/fifo-hardlink", 0666, nil, "dir1/subdir/fifo", 0, false, nil}, // hard link to fifo
		{"dir1/subdir/socket.sock", 0666, nil, "", 0, false, nil},                   // placeholder for socket

		// Another branch
		{"dir2", 0755, nil, "", 0, true, nil},
		{"dir2/reg.txt", 0644, content, "", 0, false, nil},

		{"dir2/reg-hardlink", 0644, nil, "dir2/reg.txt", 0, false, nil},
	}

	for _, tc := range cases {
		fullPath := filepath.Join(root, tc.path)
		parent := filepath.Dir(fullPath)

		// Ensure parent exists
		if err := os.MkdirAll(parent, 0755); err != nil {
			t.Errorf("failed to create parent dir %s: %v", parent, err)
			return
		}

		switch {
		case tc.isDir:
			if err := os.MkdirAll(fullPath, tc.mode); err != nil {
				t.Errorf("failed to create dir %s: %v", fullPath, err)
				return
			}

		case tc.path == "symlink-to-file.txt" || tc.path == "dir1/symlink-to-subdir":
			if err := os.Symlink(tc.target, fullPath); err != nil {
				t.Errorf("failed to create symlink %s -> %s: %v", fullPath, tc.target, err)
				return
			}

		case strings.HasSuffix(tc.path, "-hardlink"):
			targetPath := filepath.Join(root, tc.target)
			if err := os.Link(targetPath, fullPath); err != nil {
				t.Errorf("failed to create hard link: %v", err)
				return
			}

		case strings.Contains(tc.path, "block-dev"):
			if err := makeBlockDev(fullPath, tc.devType); err != nil {
				t.Logf("warning: failed to create block special file %s: %v (requires root or permission)", fullPath, err)
			}

		case strings.Contains(tc.path, "char-dev"):
			if err := makeCharDev(fullPath, tc.devType); err != nil {
				t.Logf("warning: failed to create char special file %s: %v (requires root or permission)", fullPath, err)
			}

		case strings.Contains(tc.path, "fifo"):
			if err := makeFIFO(fullPath, tc.mode); err != nil {
				t.Logf("warning: failed to create FIFO %s: %v (platform support)", fullPath, err)
			}

		case strings.HasSuffix(tc.path, ".sock"):
			// Create a Unix socket placeholder by binding
			_ = os.Remove(fullPath) // clean up first
			listener, err := net.Listen("unix", fullPath)
			if err != nil {
				t.Logf("warning: cannot create socket %s: %v (permission or OS support)", fullPath, err)
			} else {
				listener.Close()
			}

		default:
			// Regular file
			if err := os.WriteFile(fullPath, tc.data, tc.mode); err != nil {
				t.Errorf("failed to write file %s: %v", fullPath, err)
				return
			}
		}
	}
}

func makedev(major, minor uint32) uint32 {
	return (major << 8) | (minor & 0xff) | ((major & 0xfff00) << 12) | ((minor & 0xfffff00) << 12)
}

func makeBlockDev(path string, dev uint32) error {
	return syscall.Mknod(path, uint32(os.ModeDevice|os.ModeCharDevice|0660), int(dev))
}

func makeCharDev(path string, dev uint32) error {
	return syscall.Mknod(path, uint32(os.ModeCharDevice|0660), int(dev))
}

func makeFIFO(path string, mode os.FileMode) error {
	return syscall.Mkfifo(path, uint32(mode))
}

func TestDumpLoadSymlinks(t *testing.T) {
	cm := &metaData{
		symlinks: map[meta.Ino][]byte{
			3: []byte("testfile"),
		},
	}

	dumped := cm.dumpSymlinks()
	assert.Equal(t, map[uint64][]byte{3: []byte("testfile")}, dumped, "dump symlinks")

	loadedCm := &metaData{symlinks: make(map[meta.Ino][]byte)}
	loadedCm.loadSymlinks(dumped)
	assert.Equal(t, cm.symlinks, loadedCm.symlinks, "load symlinks")
}

func TestDumpLoadInode(t *testing.T) {
	tmpdir := t.TempDir()
	setupTestDirTree(t, tmpdir)

	config := &MetaConfig{
		SourceDirs: []string{tmpdir},
	}
	cm := NewMetaData(config, nil, nil, nil)
	err := cm.Init()
	if err != nil {
		t.Errorf("Init cachefs: %s", err)
		return
	}
	cfsMeta, ok := cm.(*metaData)
	if !ok {
		t.Error("Cast MetaData to *metaData failed")
		return
	}
	eno := cfsMeta.walkDir(meta.RootInode)
	if eno != 0 {
		t.Errorf("walk dir failed: %s", eno)
		return
	}

	dumped, err := cfsMeta.dumpInode("/", cfsMeta.root)
	if err != nil {
		t.Errorf("dump root failed: %s", err)
		return
	}

	cm2 := NewMetaData(config, nil, nil, nil)
	cfsMeta2, ok := cm2.(*metaData)
	if !ok {
		t.Error("Cast MetaData to *metaData failed")
		return
	}
	err = cfsMeta2.loadInode(dumped, nil)
	if err != nil {
		t.Errorf("load root failed: %s", err)
		return
	}

	// readAt is not compared, reset to same time
	originalInodes := testInodeMap(cfsMeta)
	loadedInodes := testInodeMap(cfsMeta2)
	for ino, original := range originalInodes {
		loaded := loadedInodes[ino]
		loaded.readAt = original.readAt
		assert.Equal(t, original.GetPath(), loaded.GetPath(), "compare path of inode %d", ino)
		if ino == meta.RootInode {
			assert.Nil(t, loaded.parent)
		} else if assert.NotNil(t, original.parent) && assert.NotNil(t, loaded.parent) {
			assert.Equal(t, original.parent.inode, loaded.parent.inode)
		}
	}
	assert.Equal(t, testInodeStates(cfsMeta), testInodeStates(cfsMeta2), "compare cachefs inode map")

	newDumpedDir := func(ino meta.Ino, name string, children ...*dumpedInode) *dumpedInode {
		path := "/" + name
		if name == "/" {
			path = "/"
		}
		return &dumpedInode{
			Inode:    uint64(ino),
			Name:     []byte(name),
			Path:     path,
			Attr:     &meta.Attr{Typ: meta.TypeDirectory},
			Children: children,
		}
	}
	hardlink := &dumpedInode{
		Inode:  4,
		Name:   []byte("file"),
		Path:   "/dir1/file",
		AltKey: "dev:file",
		Attr:   &meta.Attr{Typ: meta.TypeFile, Parent: 3},
	}
	alias := *hardlink
	alias.Name = []byte("alias")
	// Load /dir2/alias first. Since the recorded hardlink path is /dir1/file,
	// the restore pass must reset the parent to dir1 regardless of attr.Parent.
	dumpHardlinks := newDumpedDir(meta.RootInode, "/",
		newDumpedDir(3, "dir2", &alias),
		newDumpedDir(2, "dir1", hardlink),
	)
	cfsMeta3 := NewMetaData(config, nil, nil, nil).(*metaData)
	err = cfsMeta3.loadInode(dumpHardlinks, nil)
	if assert.NoError(t, err) {
		loadedHardlink, _ := cfsMeta3.GetInode(4)
		expectedParent, _ := cfsMeta3.GetInode(2)
		assert.Equal(t, expectedParent, loadedHardlink.parent)
		assert.Equal(t, "/dir1/file", loadedHardlink.GetPath())
	}
}

func TestDumpLoadMeta(t *testing.T) {
	tmpdir := t.TempDir()
	setupTestDirTree(t, tmpdir)

	serf := &MockSerfService{}
	rpc := &MockRpcService{}
	config := &MetaConfig{
		SourceDirs:     []string{tmpdir},
		ReaddirTimeout: -1,
	}
	cm := NewMetaData(config, rpc, serf, nil)
	err := cm.Init()
	if err != nil {
		t.Errorf("Init cachefs: %s", err)
		return
	}
	_, ok := cm.(*metaData)
	if !ok {
		t.Error("Cast MetaData to *metaData failed")
		return
	}

	// Not meta provider, should fail
	eno := cm.SourceReady(true)
	if eno != syscall.ENOTSUP {
		t.Errorf("SourceReady should return syscall.ENOTSUP, but got %d", eno)
	}

	// Set sourceless, SourceReady check should pass, but cache size check should fail
	config.Sourceless = true
	eno = cm.SourceReady(true)
	assert.Equal(t, syscall.ENOSPC, eno, "SourceReady ENOSPC")

	config.Sourceless = true
	config.CacheSize = 99999999
	for _, timeout := range []int{-1, 0, 1} {
		t.Logf("Test with readdir timeout: %d", timeout)
		config.ReaddirTimeout = timeout
		loopConfig := *config
		cm := NewMetaData(&loopConfig, rpc, serf, nil)
		err := cm.Init()
		if err != nil {
			t.Errorf("Init cachefs: %s", err)
			continue
		}
		cfsMeta, ok := cm.(*metaData)
		if !ok {
			t.Error("Cast MetaData to *metaData failed")
			continue
		}
		eno = cm.SourceReady(true)
		if eno != 0 {
			t.Errorf("SourceReady prepare failed: %s", eno)
			continue
		}
		eno = cm.SourceReady(false)
		if eno != 0 {
			t.Errorf("SourceReady failed: %s", eno)
			continue
		}

		dumped, err := cfsMeta.dumpMeta()
		if err != nil {
			t.Errorf("dumpMeta failed: %s", err)
			continue
		}
		cm2 := NewMetaData(config, rpc, serf, nil)
		cfsMeta2, ok := cm2.(*metaData)
		if !ok {
			t.Error("Cast MetaData to *metaData failed")
			continue
		}

		err = cfsMeta2.loadMeta(dumped)
		if err != nil {
			t.Errorf("loadMeta failed: %s", err)
		}

		assert.Equal(t, cfsMeta.nextIno.Load(), cfsMeta2.nextIno.Load(), "check nextIno")
		assert.Equal(t, cfsMeta.totalSize, cfsMeta2.totalSize, "check totalSize")
		assert.Equal(t, cfsMeta.totalInode, cfsMeta2.totalInode, "check totalInode")
		assert.Equal(t, cfsMeta.symlinks, cfsMeta2.symlinks, "check symlinks")

		// readAt is not compared, reset to same time
		originalInodes := testInodeMap(cfsMeta)
		loadedInodes := testInodeMap(cfsMeta2)
		for ino, original := range originalInodes {
			loaded := loadedInodes[ino]
			loaded.readAt = original.readAt
		}
		assert.Equal(t, testInodeStates(cfsMeta), testInodeStates(cfsMeta2), "check cachefs meta")
	}
}

func TestDumpLoadInoAltMap(t *testing.T) {
	cm := &metaData{inodeShards: newInodeShards()}

	// Add inodes with and without altKeys
	cino1 := NewInode(1, nil, "one", meta.TypeFile, &meta.Attr{Typ: meta.TypeFile})
	cino1.altKey = "alt-key-1"
	cino2 := NewInode(2, nil, "two", meta.TypeDirectory, &meta.Attr{Typ: meta.TypeDirectory})
	cino3 := NewInode(3, nil, "three", meta.TypeSymlink, &meta.Attr{Typ: meta.TypeSymlink})
	cino3.altKey = "alt-key-3"
	putTestInode(cm, cino1)
	putTestInode(cm, cino2)
	putTestInode(cm, cino3)

	// Test dumping altKey map
	dumped := cm.dumpInoAltMap()
	assert.Len(t, dumped, 2) // Only 2 entries with altKeys
	assert.Equal(t, uint64(1), dumped["alt-key-1"])
	assert.Equal(t, uint64(3), dumped["alt-key-3"])

	// Test with empty inoMap
	resetTestInodes(cm)
	dumped = cm.dumpInoAltMap()
	assert.Empty(t, dumped)

	// Test with all inodes having no altKeys
	putTestInode(cm, cino2)
	dumped = cm.dumpInoAltMap()
	assert.Empty(t, dumped)

	// Test loadInoAltMap functionality
	resetTestInodes(cm)
	putTestInode(cm, cino1)
	putTestInode(cm, cino2)
	putTestInode(cm, cino3)

	// Load altKey mappings
	dumpedMap := map[string]uint64{
		"alt-key-1": 1,
		"alt-key-3": 3,
		"alt-key-4": 4, // This inode doesn't exist
	}
	cm.loadInoAltMap(dumpedMap)
	assert.Equal(t, 2, testAltMapLen(cm))
	value, ok := cm.inoAltMap.Load("alt-key-1")
	assert.True(t, ok)
	assert.Equal(t, cino1, value)
	value, ok = cm.inoAltMap.Load("alt-key-3")
	assert.True(t, ok)
	assert.Equal(t, cino3, value)

	// Test with empty dumped map
	resetTestInodes(cm)
	cm.loadInoAltMap(make(map[string]uint64))
	assert.Zero(t, testAltMapLen(cm))
	dumpedMap = map[string]uint64{
		"alt-key-5": 5,
		"alt-key-6": 6,
	}
	cm.loadInoAltMap(dumpedMap)
	assert.Zero(t, testAltMapLen(cm))
}

func TestDumpMetaCache(t *testing.T) {
	t.Run("not ready returns error", func(t *testing.T) {
		m, _ := buildTree()
		// metaReady was set to true by buildTree; reset it.
		m.metaReady.Store(false)
		_, err := m.dumpMeta()
		if !assert.Error(t, err) {
			return
		}
		assert.Contains(t, err.Error(), "not ready")
		assert.Nil(t, m.dumpedMeta.Load(), "dumpedMeta must stay nil on error")
	})

	t.Run("first call builds and stores cache", func(t *testing.T) {
		m, _ := buildTree()
		// dumpedMeta must be empty before the first call.
		assert.Nil(t, m.dumpedMeta.Load())

		dumped, err := m.dumpMeta()
		if !assert.NoError(t, err) {
			return
		}
		assert.NotNil(t, dumped)
		// The result must have been stored atomically.
		assert.Same(t, dumped, m.dumpedMeta.Load(),
			"dumpedMeta must point to the returned value after first call")
	})

	t.Run("second call returns cached pointer", func(t *testing.T) {
		m, _ := buildTree()

		first, err := m.dumpMeta()
		if !assert.NoError(t, err) {
			return
		}
		second, err := m.dumpMeta()
		if !assert.NoError(t, err) {
			return
		}
		// Must be the same pointer — no rebuild on repeated calls.
		assert.Same(t, first, second, "dumpMeta must return the cached pointer on the second call")
	})

	t.Run("pre-stored cache is returned immediately", func(t *testing.T) {
		m, _ := buildTree()
		sentinel := &dumpedCachefs{NextInode: 42}
		m.dumpedMeta.Store(sentinel)

		result, err := m.dumpMeta()
		if !assert.NoError(t, err) {
			return
		}
		assert.Same(t, sentinel, result,
			"dumpMeta must return the pre-stored cache without rebuilding")
	})
}
