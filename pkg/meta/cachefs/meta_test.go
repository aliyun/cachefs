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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/meta"
	"github.com/aliyun/cachefs/pkg/srcmgr"
	"github.com/stretchr/testify/assert"
)

func testInodeMap(m *metaData) map[meta.Ino]*Inode {
	inodes := make(map[meta.Ino]*Inode)
	for i := range m.inodeShards {
		for ino, cino := range m.inodeShards[i].inodes {
			inodes[ino] = cino
		}
	}
	return inodes
}

type testInodeState struct {
	parent   meta.Ino
	name     string
	typ      uint8
	altKey   string
	attr     *meta.Attr
	children map[string]meta.Ino
	readdir  bool
	readAt   time.Time
}

func testInodeStates(m *metaData) map[meta.Ino]testInodeState {
	states := make(map[meta.Ino]testInodeState)
	for ino, cino := range testInodeMap(m) {
		state := testInodeState{
			name:     cino.name,
			typ:      cino.typ,
			altKey:   cino.altKey,
			attr:     cino.GetAttr(),
			children: make(map[string]meta.Ino),
			readdir:  cino.readdir,
			readAt:   cino.readAt,
		}
		if cino.parent != nil {
			state.parent = cino.parent.inode
		}
		for name, child := range cino.children {
			state.children[name] = child.inode
		}
		states[ino] = state
	}
	return states
}

func putTestInode(m *metaData, cino *Inode) {
	shard := m.inodeShard(cino.inode)
	if shard.inodes == nil {
		shard.inodes = make(map[meta.Ino]*Inode)
	}
	shard.inodes[cino.inode] = cino
}

func resetTestInodes(m *metaData) {
	m.inodeShards = newInodeShards()
	m.inoAltMap.Range(func(key, _ any) bool {
		m.inoAltMap.Delete(key)
		return true
	})
}

func testAltMapLen(m *metaData) int {
	count := 0
	m.inoAltMap.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func TestNewInodeShards(t *testing.T) {
	shards := newInodeShards()
	assert.Len(t, shards, inodeShardCount)
	for i := range shards {
		assert.NotNil(t, shards[i].inodes)
		assert.Empty(t, shards[i].inodes)
	}
}

func TestInodeShard(t *testing.T) {
	m := &metaData{inodeShards: newInodeShards()}
	for ino := meta.Ino(0); ino < 2*inodeShardCount; ino++ {
		expected := &m.inodeShards[uint64(ino)&(inodeShardCount-1)]
		assert.Same(t, expected, m.inodeShard(ino))
	}
}

func TestInodeCount(t *testing.T) {
	m := &metaData{inodeShards: newInodeShards()}
	assert.Zero(t, m.inodeCount())
	for _, ino := range []meta.Ino{1, 2, 65} {
		putTestInode(m, NewInode(ino, nil, "file", meta.TypeFile, nil))
	}
	assert.Equal(t, uint64(3), m.inodeCount())
}

func TestNewMetaData(t *testing.T) {
	conf := &MetaConfig{SourceDirs: []string{"/source"}}
	m := NewMetaData(conf, nil, nil, nil)
	cfsMeta, ok := m.(*metaData)
	if !ok {
		t.Error("Create default cachefs meta failed, type assertion failed")
	} else {
		assert.Equal(t, map[string]struct{}{}, cfsMeta.sourceSubPath)
		assert.Equal(t, uint64(meta.RootInode), cfsMeta.nextIno.Load())
	}

	tests := []struct {
		name    string
		subpath string
		expect  map[string]struct{}
	}{
		{"tc1", "/", map[string]struct{}{}},
		{"tc2", "/a", map[string]struct{}{"/a": {}}},
		{"tc3", "/a /b", map[string]struct{}{"/a": {}, "/b": {}}},
		{"tc4", "/a b", map[string]struct{}{"/a": {}}},
		{"tc5", "/a /b /a", map[string]struct{}{"/a": {}, "/b": {}}},
	}

	for _, tc := range tests {
		conf := &MetaConfig{
			SourceDirs: []string{"/source"},
			SubPathConfig: SourceSubPathConfig{
				Paths: tc.subpath,
			},
		}
		m := NewMetaData(conf, nil, nil, nil)
		cfsMeta, ok := m.(*metaData)
		assert.Equal(t, true, ok, tc.name)
		assert.Equal(t, tc.expect, cfsMeta.sourceSubPath, tc.name)
	}

	// Peer meta enabled but serf/rpc are nil: a warning is logged and meta is returned
	// with peer meta initialization skipped (serfGroup and serfUser remain unset).
	peerMetaConf := &MetaConfig{
		SourceDirs:      []string{"/source"},
		PeerMetaTimeout: 30,
	}
	m = NewMetaData(peerMetaConf, nil, nil, nil)
	cfsMeta, ok = m.(*metaData)
	if !ok {
		t.Error("nil-services peer meta: type assertion failed")
	} else {
		assert.Equal(t, "", cfsMeta.serfGroup, "serfGroup should be empty when serf service is nil")
		assert.Nil(t, cfsMeta.serfUser, "serfUser should be nil when serf service is nil")
	}
}

func TestSubpathEnabled(t *testing.T) {
	tests := []struct {
		name   string
		paths  string
		file   string
		cmd    string
		expect bool
	}{
		{
			name:   "both empty",
			paths:  "",
			file:   "",
			expect: false,
		},
		{
			name:   "paths not empty",
			paths:  "/a /b",
			file:   "",
			expect: true,
		},
		{
			name:   "file not empty",
			paths:  "",
			file:   "/some/file",
			expect: true,
		},
		{
			name:   "both not empty",
			paths:  "/a /b",
			file:   "/some/file",
			expect: true,
		},
		{
			name:   "only whitespace in paths",
			paths:  "   ",
			file:   "",
			expect: true, // len("   ") > 0 is true
		},
		{
			name:   "cmd only",
			cmd:    "echo /a",
			expect: true,
		},
		{
			name:   "all empty",
			expect: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf := &MetaConfig{
				SubPathConfig: SourceSubPathConfig{
					Paths: tc.paths,
					File:  tc.file,
					Cmd:   tc.cmd,
				},
			}
			result := conf.SubpathEnabled()
			if result != tc.expect {
				t.Errorf("SubpathEnabled() = %v, expect %v", result, tc.expect)
				return
			}
		})
	}
}

func TestIsAllowedSubPath(t *testing.T) {
	tests := []struct {
		name    string
		subpath string
		path    string
		expect  bool
	}{
		{"tc1", "", "/a", true},
		{"tc1", "/", "/a", true},
		{"tc1", "/ /b", "/a", true},
		{"tc2", "/a", "/a", true},
		{"tc3", "/a", "/b", false},
		{"tc4", "/b", "/a", false},
		{"tc5", "/a /b", "/a", true},
		{"tc6", "/a /b", "/b", true},
		{"tc6", "/a /b", "/c", false},
		{"tc7", "/a/b", "/a", true},
		{"tc8", "/a/b", "/b", false},
		{"tc9", "/a/b", "/a/b", true},
		{"tc10", "/a/b", "/a/b/c", true},
		{"tc11", "/a/b/c", "/a", true},
		{"tc12", "/a/b/c", "/b", false},
		{"tc13", "/a/b/c", "/a/b", true},
		{"tc14", "/a/b/c", "/a/b/c", true},
		{"tc15", "/ab/c", "/ab", true},
		{"tc16", "/ab/c", "/a", false},
		{"tc17", "/ab/c", "/ab/c", true},
		{"tc18", "/ab/c", "/ab/c/d", true},
		{"tc19", "/ab/cd", "/ab/c", false},
		{"tc20", "/ab/cd/ef", "/ab/cd", true},
		{"tc21", "/ab/cd/ef", "/ab/cd/e", false},
		{"tc22", "/ab/cd/ef", "/ab/cd/ef/g", true},
		{"tc23", "//ab/cd/ef", "/ab/cd/ef/g", true},
		{"tc24", "/a/b", "/a/bc", false},
		{"tc25", "/a/b /a/bc/d", "/a/bc", true},
		{"tc26", "/a/b /a/bc/d", "/a/bc/de", false},
		{"trailing-slash", "/a/b/c/", "/a/b/c/d", true},
		{"trailing-slash-exact", "/a/b/c/", "/a/b/c", true},
		{"trailing-slash-ancestor", "/a/b/c/", "/a/b", true},
		{"dotdot", "/a/b/../c", "/a/c/d", true},
	}

	for _, tc := range tests {
		conf := &MetaConfig{
			SourceDirs: []string{"/source"},
			SubPathConfig: SourceSubPathConfig{
				Paths: tc.subpath,
			},
		}
		m := NewMetaData(conf, nil, nil, nil)
		cfsMeta, ok := m.(*metaData)
		assert.Equal(t, true, ok, tc.name)
		assert.Equal(t, tc.expect, cfsMeta.isAllowedSubPath(tc.path), tc.name)
	}
}

func TestGenSourceSubpath(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect map[string]struct{}
	}{
		{"empty", "", map[string]struct{}{}},
		{"onePath", "/path1", map[string]struct{}{"/path1": {}}},
		{"twoPaths", "/path1 /path2", map[string]struct{}{"/path1": {}, "/path2": {}}},
		{"tabSeparated", "/path1\t/path2", map[string]struct{}{"/path1": {}, "/path2": {}}},
		{"withWhiteSpace", " /path1\t /path2 \n/path3   ", map[string]struct{}{"/path1": {}, "/path2": {}, "/path3": {}}},
		{"trailingSlash", "/a/b/c/", map[string]struct{}{"/a/b/c": {}}},
		{"doubleSlash", "//a/b", map[string]struct{}{"/a/b": {}}},
		{"dotdot", "/a/b/../c", map[string]struct{}{"/a/c": {}}},
		{"dedupAfterClean", "/a /a/", map[string]struct{}{"/a": {}}},
		{"doubleSlashRootClears", "/a //", map[string]struct{}{}},
	}

	for _, tc := range tests {
		conf := &SourceSubPathConfig{
			Paths: tc.input,
			File:  "",
		}
		output, _, _, err := genSourceSubpaths(conf)
		assert.NoError(t, err, tc.name)
		assert.Equal(t, tc.expect, output, tc.name)
	}
}

func TestGenSourceSubpathFile(t *testing.T) {
	type testcase struct {
		name    string
		input   string
		content string
		err     bool
		expect  []string
	}
	tests := []testcase{
		{"nosuchfile", "nosuchfile", "", true, []string{}},
		{"empty", "", "", false, []string{}},
		{"emptyfile", "emptyfile.txt", "", false, []string{}},
		{"onlynewline", "onlynewline.txt", "\n", false, []string{}},
		{"onePath", "onepath.txt", "path1", false, []string{"path1"}},
		{"twoPaths", "twopaths.txt", "path1\npath2", false, []string{"path1", "path2"}},
		{"tabSeparated", "tabseperated.txt", "path1\tpath2", false, []string{"path1\tpath2"}},
		{"comment", "comment.txt", "#path1\npath2", false, []string{"path2"}},
		{"emptyline", "emptyline.txt", "path1\n\npath2\n", false, []string{"path1", "path2"}},
	}

	dir := t.TempDir()

	// Prepare test files
	for _, tc := range tests {
		fname := tc.input
		if !strings.HasSuffix(fname, "txt") {
			continue
		}
		path := filepath.Join(dir, fname)
		err := os.WriteFile(path, []byte(tc.content), 0644)
		if err != nil {
			t.Errorf("Write test file: %s", err)
			return
		}
		// update input to right path
		tc.input = path
	}

	// Run tests
	for _, tc := range tests {
		conf := &SourceSubPathConfig{
			Paths: "",
			File:  tc.input,
		}
		output, _, _, err := genSourceSubpaths(conf)
		if tc.err {
			assert.Error(t, err, tc.name)
			return
		}
		assert.NoError(t, err, tc.name)
		assert.Equal(t, tc.expect, output, tc.name)
	}
}

// cmdOut must be non-empty only in Cmd mode: callers rely on cmdOut != "" to decide
// whether to log the command result. Even an empty-output command yields "<empty>",
// while File/Paths/empty modes must return "".
func TestGenSourceSubpathsCmdOut(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "subpaths.txt")
	if err := os.WriteFile(f, []byte("/from/file\n"), 0644); err != nil {
		t.Fatalf("write subpath file: %s", err)
	}
	cases := []struct {
		name     string
		conf     SourceSubPathConfig
		nonEmpty bool
	}{
		{"empty", SourceSubPathConfig{}, false},
		{"paths", SourceSubPathConfig{Paths: "/from/static"}, false},
		{"file", SourceSubPathConfig{File: f}, false},
		{"cmd", SourceSubPathConfig{Cmd: "printf /from/cmd\\n"}, true},
		{"cmd empty output", SourceSubPathConfig{Cmd: "true"}, true},
	}
	for _, tc := range cases {
		conf := tc.conf
		_, cmdOut, _, err := genSourceSubpaths(&conf)
		assert.NoError(t, err, tc.name)
		if tc.nonEmpty {
			assert.NotEmpty(t, cmdOut, tc.name)
		} else {
			assert.Empty(t, cmdOut, tc.name)
		}
	}
}

func TestSubPathEqual(t *testing.T) {
	tests := []struct {
		name    string
		a       map[string]struct{}
		b       map[string]struct{}
		want    bool
		removed []string
	}{
		{
			name:    "same set, different order",
			a:       map[string]struct{}{"a": {}, "b": {}},
			b:       map[string]struct{}{"b": {}, "a": {}},
			want:    true,
			removed: []string{},
		},
		{
			name:    "different set",
			a:       map[string]struct{}{"a": {}, "b": {}},
			b:       map[string]struct{}{"a": {}, "c": {}},
			want:    false,
			removed: []string{"b/"},
		},
		{
			name:    "different len",
			a:       map[string]struct{}{"a": {}, "b": {}},
			b:       map[string]struct{}{"a": {}},
			want:    false,
			removed: []string{"b/"},
		},

		{
			name:    "empty map and nil map",
			a:       nil,
			b:       map[string]struct{}{},
			want:    true,
			removed: []string{},
		},
		{
			name:    "two empty maps",
			a:       map[string]struct{}{},
			b:       map[string]struct{}{},
			want:    true,
			removed: []string{},
		},
		{
			name:    "nil and non-empty map",
			a:       nil,
			b:       map[string]struct{}{"a": {}},
			want:    false,
			removed: []string{},
		},
		{
			name:    "empty and non-empty map",
			a:       map[string]struct{}{},
			b:       map[string]struct{}{"a": {}},
			want:    false,
			removed: []string{},
		},
		{
			name:    "partial same",
			a:       map[string]struct{}{"a": {}, "b": {}},
			b:       map[string]struct{}{"a": {}, "c": {}},
			want:    false,
			removed: []string{"b/"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, removed := subPathEqual(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("subPathEqual() = %v, want %v", got, tc.want)
			}
			assert.Equal(t, tc.removed, removed, tc.name)
		})
	}
}

func TestReloadSubpathFile(t *testing.T) {
	// Prepare a test source dir, which contains several dirs and files
	sourceDir := t.TempDir()
	dirA := filepath.Join(sourceDir, "dirA")
	if err := os.Mkdir(dirA, 0755); err != nil {
		t.Errorf("Mkdir: %s", err)
		return
	}
	dirB := filepath.Join(sourceDir, "dirB")
	if err := os.Mkdir(dirB, 0755); err != nil {
		t.Errorf("Mkdir: %s", err)
		return
	}

	// Prepare subpath list file
	tmpDir := t.TempDir()
	subPathFile := filepath.Join(tmpDir, "subpath.txt")
	if err := os.WriteFile(subPathFile, []byte("/dirA"), 0644); err != nil {
		t.Errorf("Write subpath file: %s", err)
		return
	}

	// Create cachefs meta
	metaConf := &MetaConfig{
		Name:       "testcachefs",
		SourceDirs: []string{sourceDir},
		SubPathConfig: SourceSubPathConfig{
			Paths:  "",
			File:   subPathFile,
			Reload: 1, // reload every 1 second
		},
		ReaddirTimeout: 0,
	}
	cfsMeta := NewMetaData(metaConf, nil, nil, nil)
	assert.Equal(t, true, cfsMeta != nil, "create cachefs meta")
	if err := cfsMeta.Init(); err != nil {
		t.Errorf("Init meta: %s", err)
		return
	}

	// We should only see dirA for now
	var inodes []*meta.Entry
	if err := cfsMeta.Readdir(meta.Background, meta.RootInode, &inodes); err != 0 {
		t.Errorf("Readdir: %s", err)
		return
	}
	// 3 entries, dirA and "." and ".."
	assert.Equal(t, 3, len(inodes), "first readdir")
	for _, ent := range inodes {
		name := string(ent.Name)
		if name == "." || name == ".." {
			continue
		}
		if name != "dirA" {
			t.Errorf("Readdir expect only dirA, but found %s", name)
			return
		}
	}

	// Update subpath list, and wait for reload
	if err := os.WriteFile(subPathFile, []byte("/dirA\n/dirB"), 0644); err != nil {
		t.Errorf("Write subpath file: %s", err)
		return
	}
	time.Sleep(1200 * time.Millisecond)

	// We should see dirA and dirB
	inodes = []*meta.Entry{}
	if err := cfsMeta.Readdir(meta.Background, meta.RootInode, &inodes); err != 0 {
		t.Errorf("Readdir: %s", err)
		return
	}
	// 4 entries, dirA dirB and "." and ".."
	foundA := false
	foundB := false
	assert.Equal(t, 4, len(inodes), "second readdir")
	for _, ent := range inodes {
		name := string(ent.Name)
		if name == "." || name == ".." {
			continue
		}
		if name == "dirA" {
			foundA = true
		}
		if name == "dirB" {
			foundB = true
		}
	}
	if !foundA || !foundB {
		t.Errorf("Readdir expect dirA and dirB, but found A %v, found B %v", foundA, foundB)
		return
	}

	// Update subpath list, remove dirA
	if err := os.WriteFile(subPathFile, []byte("/dirB"), 0644); err != nil {
		t.Errorf("Write subpath file: %s", err)
		return
	}
	time.Sleep(1200 * time.Millisecond)

	// We should see dirB
	inodes = []*meta.Entry{}
	if err := cfsMeta.Readdir(meta.Background, meta.RootInode, &inodes); err != 0 {
		t.Errorf("Readdir: %s", err)
		return
	}
	// 3 entries, dirB and "." and ".."
	assert.Equal(t, 3, len(inodes), "third readdir")
	for _, ent := range inodes {
		name := string(ent.Name)
		if name == "." || name == ".." {
			continue
		}
		if name != "dirB" {
			t.Errorf("Readdir expect only dirB, but found %s", name)
			return
		}
	}
}

// TestReloadSubpathInvalidatesSubdirCache verifies that a subpath reload
// invalidates the cached readdir of nested directories, not just the root.
// It uses a large ReaddirTimeout so a subdir's own cache would never expire
// on its own: only the reload-triggered invalidation can drop the stale,
// now-disallowed child.
func TestReloadSubpathInvalidatesSubdirCache(t *testing.T) {
	assert := assert.New(t)

	// source layout: dirA/{sub1,sub2}
	sourceDir := t.TempDir()
	dirA := filepath.Join(sourceDir, "dirA")
	assert.NoError(os.MkdirAll(filepath.Join(dirA, "sub1"), 0755))
	assert.NoError(os.MkdirAll(filepath.Join(dirA, "sub2"), 0755))

	tmpDir := t.TempDir()
	subPathFile := filepath.Join(tmpDir, "subpath.txt")
	assert.NoError(os.WriteFile(subPathFile, []byte("/dirA"), 0644))

	metaConf := &MetaConfig{
		Name:       "testcachefs",
		SourceDirs: []string{sourceDir},
		SubPathConfig: SourceSubPathConfig{
			File:   subPathFile,
			Reload: 1, // reload every 1 second
		},
		ReaddirTimeout: 3600, // large: subdir cache must not expire on its own
	}
	cfsMeta := NewMetaData(metaConf, nil, nil, nil)
	assert.NotNil(cfsMeta)
	assert.NoError(cfsMeta.Init())

	// List child names of an inode, excluding "." and "..".
	listNames := func(ino meta.Ino) []string {
		var entries []*meta.Entry
		if err := cfsMeta.Readdir(meta.Background, ino, &entries); err != 0 {
			t.Fatalf("Readdir(%d): %s", ino, err)
		}
		names := []string{}
		for _, e := range entries {
			if n := string(e.Name); n != "." && n != ".." {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		return names
	}

	// Resolve dirA's inode.
	var dirAIno meta.Ino
	var attr meta.Attr
	if err := cfsMeta.Lookup(meta.Background, meta.RootInode, "dirA", &dirAIno, &attr); err != 0 {
		t.Fatalf("Lookup dirA: %s", err)
	}

	// Prime dirA's cache: both children are allowed under "/dirA".
	assert.Equal([]string{"sub1", "sub2"}, listNames(dirAIno), "before reload")

	// Narrow the subpath to "/dirA/sub1"; sub2 is no longer allowed.
	assert.NoError(os.WriteFile(subPathFile, []byte("/dirA/sub1"), 0644))
	time.Sleep(1200 * time.Millisecond)

	// dirA's non-expired cache must have been invalidated by the reload, so
	// the stale sub2 is dropped. Without subtree invalidation (root-only), the
	// cached dirA readdir would still return sub2 here.
	assert.Equal([]string{"sub1"}, listNames(dirAIno), "after reload")
}

func TestGetRemovedKeys(t *testing.T) {
	tests := []struct {
		name     string
		oldSet   map[string]struct{}
		newSet   map[string]struct{}
		expected []string
	}{
		{
			name: "some keys removed",
			oldSet: map[string]struct{}{
				"a": {},
				"b": {},
			},
			newSet: map[string]struct{}{
				"b": {},
			},
			expected: []string{"a/"},
		},
		{
			name:     "empty old set",
			oldSet:   map[string]struct{}{},
			newSet:   map[string]struct{}{"a": {}},
			expected: []string{},
		},
		{
			name: "empty new set",
			oldSet: map[string]struct{}{
				"a": {},
				"b": {},
			},
			newSet:   map[string]struct{}{},
			expected: []string{"a/", "b/"},
		},
		{
			name: "exact match",
			oldSet: map[string]struct{}{
				"a": {},
				"b": {},
			},
			newSet: map[string]struct{}{
				"a": {},
				"b": {},
			},
			expected: []string{},
		},
		{
			name:     "both sets empty",
			oldSet:   map[string]struct{}{},
			newSet:   map[string]struct{}{},
			expected: []string{},
		},
		{
			name:     "nil old set",
			oldSet:   nil,
			newSet:   map[string]struct{}{"a": {}},
			expected: []string{},
		},
		{
			name: "nil new set",
			oldSet: map[string]struct{}{
				"a": {},
				"b": {},
			},
			newSet:   nil,
			expected: []string{"a/", "b/"}, // nil newSet treated as empty set
		},
		{
			name: "case sensitivity check",
			oldSet: map[string]struct{}{
				"Apple":  {},
				"Banana": {},
			},
			newSet: map[string]struct{}{
				"apple": {},
			},
			expected: []string{"Apple/", "Banana/"},
		},
	}

	for _, tc := range tests {
		result := getRemovedKeys(tc.oldSet, tc.newSet)
		// Sort results and expected values for stable comparison (since map iteration order
		// is random in Go)
		sort.Strings(result)
		sort.Strings(tc.expected)
		assert.Equal(t, tc.expected, result, tc.name)
	}
}

func TestNewInode(t *testing.T) {
	ino := meta.Ino(100)
	attr := &meta.Attr{
		Length: 1024,
		Mtime:  time.Now().Unix(),
	}
	root := NewInode(meta.RootInode, nil, "/", meta.TypeDirectory, nil)
	dir := NewInode(2, root, "dir", meta.TypeDirectory, nil)
	tests := []struct {
		name     string
		parent   *Inode
		nodeName string
		wantPath string
	}{
		{"root", nil, "/", "/"},
		{"child of root", root, "file", "/file"},
		{"nested", dir, "file", "/dir/file"},
		{"empty name", nil, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewInode(ino, tt.parent, tt.nodeName, attr.Typ, attr)
			assert.Equal(t, ino, node.inode)
			assert.Same(t, tt.parent, node.parent)
			assert.Equal(t, tt.wantPath, node.GetPath())
			assert.Equal(t, attr, node.GetAttr())
			assert.Empty(t, node.children)
		})
	}
	file := NewInode(3, nil, "file", meta.TypeFile, &meta.Attr{Typ: meta.TypeFile})
	assert.Nil(t, file.children)
	directory := NewInode(4, nil, "dir", meta.TypeDirectory, &meta.Attr{Typ: meta.TypeDirectory})
	assert.NotNil(t, directory.children)
	assert.Empty(t, directory.children)
}

func TestLookup(t *testing.T) {
	source := t.TempDir()
	if !assert.NoError(t, os.Mkdir(filepath.Join(source, "dir"), 0755)) ||
		!assert.NoError(t, os.WriteFile(filepath.Join(source, "file"), []byte("data"), 0644)) ||
		!assert.NoError(t, os.WriteFile(filepath.Join(source, "dir", "file"), []byte("data"), 0644)) ||
		!assert.NoError(t, os.Symlink("dir", filepath.Join(source, "link"))) {
		return
	}
	m := NewMetaData(&MetaConfig{SourceDirs: []string{source}, ReaddirTimeout: -1}, nil, nil, nil).(*metaData)
	if !assert.NoError(t, m.Init()) {
		return
	}

	for _, tt := range []struct {
		name      string
		directory bool
	}{
		{name: "dir", directory: true},
		{name: "file"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var ino meta.Ino
			var attr meta.Attr
			if !assert.Zero(t, m.doLookup(meta.Background, meta.RootInode, tt.name, &ino, &attr)) {
				return
			}
			child, eno := m.GetInode(ino)
			if !assert.Zero(t, eno) || !assert.NotNil(t, child) {
				return
			}
			if tt.directory {
				assert.NotNil(t, child.children)
			} else {
				assert.Nil(t, child.children)
			}
		})
	}

	t.Run("non-directory parent", func(t *testing.T) {
		var ino meta.Ino
		var attr meta.Attr
		if !assert.Zero(t, m.Lookup(meta.Background, meta.RootInode, "link", &ino, &attr)) {
			return
		}
		assert.Equal(t, syscall.ENOTDIR, m.Lookup(meta.Background, ino, "file", &ino, &attr))
	})
}

func TestInodeBuildPath(t *testing.T) {
	root := NewInode(meta.RootInode, nil, "/", meta.TypeDirectory, nil)
	dir := NewInode(2, root, "dir", meta.TypeDirectory, nil)
	subdir := NewInode(3, dir, "subdir", meta.TypeDirectory, nil)
	file := NewInode(4, subdir, "file", meta.TypeFile, nil)
	prefixedRoot := NewInode(5, nil, "/stag_src", meta.TypeDirectory, nil)
	prefixedDir := NewInode(6, prefixedRoot, "dir", meta.TypeDirectory, nil)

	tests := []struct {
		name string
		node *Inode
		want string
	}{
		{"root", root, "/"},
		{"nested path", file, "/dir/subdir/file"},
		{"prefixed root", prefixedRoot, "/stag_src"},
		{"prefixed child", prefixedDir, "/stag_src/dir"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.node.buildPath())
		})
	}
}

func TestGetAttr(t *testing.T) {
	attr := &meta.Attr{}
	node := NewInode(meta.Ino(1), nil, "", attr.Typ, attr)
	assert.Equal(t, attr, node.GetAttr())
}

func TestCachedAttr(t *testing.T) {
	m := &metaData{config: &MetaConfig{ReaddirTimeout: 60}}
	cino := NewInode(1, nil, "file", meta.TypeFile, &meta.Attr{Typ: meta.TypeFile, Length: 42})
	attr := m.cachedAttr(cino)
	if assert.NotNil(t, attr) {
		assert.Equal(t, uint64(42), attr.Length)
	}

	cino.attrCache.Load().loadedAt = time.Now().Add(-2 * time.Minute)
	assert.Nil(t, m.cachedAttr(cino))
	m.config.ReaddirTimeout = -1
	assert.NotNil(t, m.cachedAttr(cino))
	cino.attrCache.Store(nil)
	assert.Nil(t, m.cachedAttr(cino))
}

func TestLoadAttr(t *testing.T) {
	source := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(source, "file"), []byte("data"), 0644))
	m := NewMetaData(&MetaConfig{SourceDirs: []string{source}, ReaddirTimeout: -1}, nil, nil, nil).(*metaData)
	root := NewInode(meta.RootInode, nil, "/", meta.TypeDirectory, nil)

	attr, eno := m.loadAttr(meta.Background, root)
	assert.Zero(t, eno)
	if assert.NotNil(t, attr) {
		assert.EqualValues(t, meta.TypeDirectory, attr.Typ)
		assert.Equal(t, meta.RootInode, attr.Parent)
	}

	file := NewInode(2, root, "file", meta.TypeFile, nil)
	attr, eno = m.loadAttr(meta.Background, file)
	assert.Zero(t, eno)
	if assert.NotNil(t, attr) {
		assert.Equal(t, uint64(4), attr.Length)
		assert.Equal(t, meta.RootInode, attr.Parent)
	}
	assert.Equal(t, int64(2), m.cachedAttrs.Load())
}

func TestAttrReload(t *testing.T) {
	source := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(source, "file"), []byte("data"), 0644))
	m := NewMetaData(&MetaConfig{SourceDirs: []string{source}, AttrCleanup: true, ReaddirTimeout: -1}, nil, nil, nil).(*metaData)
	root := NewInode(meta.RootInode, nil, "/", meta.TypeDirectory, &meta.Attr{Typ: meta.TypeDirectory})
	root.attrCache.Load().loadedAt = time.Now().Add(time.Hour)
	file := NewInode(2, root, "file", meta.TypeFile, nil)
	putTestInode(m, root)
	putTestInode(m, file)
	m.cachedAttrs.Add(1)

	attr, eno := m.ensureAttr(meta.Background, file)
	assert.Zero(t, eno)
	if assert.NotNil(t, attr) {
		assert.Equal(t, uint64(4), attr.Length)
		assert.EqualValues(t, meta.TypeFile, attr.Typ)
		assert.Equal(t, meta.RootInode, attr.Parent)
	}
	assert.Equal(t, int64(2), m.cachedAttrs.Load())

	file.attrCache.Load().loadedAt = time.Now().Add(-time.Second)
	m.config.ReaddirTimeout = 0
	assert.Equal(t, 1, m.doSweepAttrs())
	assert.Equal(t, int64(1), m.cachedAttrs.Load())

	var reloaded meta.Attr
	assert.Zero(t, m.GetAttr(meta.Background, file.inode, &reloaded))
	assert.Equal(t, uint64(4), reloaded.Length)
	assert.Equal(t, int64(2), m.cachedAttrs.Load())
}

func TestReaddirSkipsAttrReloadFailure(t *testing.T) {
	source := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(source, "good"), []byte("data"), 0644))
	m := NewMetaData(&MetaConfig{SourceDirs: []string{source}, ReaddirTimeout: 60}, nil, nil, nil).(*metaData)
	root := NewInode(meta.RootInode, nil, "/", meta.TypeDirectory, &meta.Attr{Typ: meta.TypeDirectory, Parent: meta.RootInode})
	root.readdir = true
	root.readAt = time.Now()
	good := NewInode(2, root, "good", meta.TypeFile, nil)
	missing := NewInode(3, root, "missing", meta.TypeFile, nil)
	root.children["good"] = good
	root.children["missing"] = missing
	for _, cino := range []*Inode{root, good, missing} {
		putTestInode(m, cino)
	}

	var entries []*meta.Entry
	assert.Zero(t, m.Readdir(meta.Background, meta.RootInode, &entries))
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, string(entry.Name))
	}
	assert.ElementsMatch(t, []string{".", "..", "good"}, names)
}

func TestIsStaleAttr(t *testing.T) {
	now := time.Now()
	for _, tt := range []struct {
		name    string
		cached  *inodeAttr
		timeout time.Duration
		want    bool
	}{
		{name: "missing", want: true},
		{name: "fresh", cached: &inodeAttr{loadedAt: now.Add(-5 * time.Second)}, timeout: 10 * time.Second},
		{name: "expired", cached: &inodeAttr{loadedAt: now.Add(-15 * time.Second)}, timeout: 10 * time.Second, want: true},
		{name: "zero timeout", cached: &inodeAttr{loadedAt: now.Add(-time.Second)}, want: true},
		{name: "negative timeout", cached: &inodeAttr{loadedAt: now.Add(-24 * time.Hour)}, timeout: -1},
		{name: "future load time", cached: &inodeAttr{loadedAt: now.Add(time.Hour)}, timeout: time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isStaleAttr(tt.cached, tt.timeout))
		})
	}
}

func TestMetaConfig(t *testing.T) {
	tests := []struct {
		name           string
		sourceDir      string
		sourceless     bool
		expectProvider bool
		expectConsumer bool
	}{
		{
			name:           "non-empty SourceDir, Sourceless=true -> is sender, not receiver",
			sourceDir:      "/mnt/source",
			sourceless:     true,
			expectProvider: true,
			expectConsumer: false,
		},
		{
			name:           "empty SourceDir, Sourceless=true -> is receiver, not sender",
			sourceDir:      "",
			sourceless:     true,
			expectProvider: false,
			expectConsumer: true,
		},
		{
			name:           "non-empty SourceDir, Sourceless=false -> neither sender nor receiver",
			sourceDir:      "/mnt/source",
			sourceless:     false,
			expectProvider: false,
			expectConsumer: false,
		},
		{
			name:           "empty SourceDir, Sourceless=false -> neither sender nor receiver",
			sourceDir:      "",
			sourceless:     false,
			expectProvider: false,
			expectConsumer: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &MetaConfig{
				Sourceless: tt.sourceless,
			}
			if tt.sourceDir != "" {
				mc.SourceDirs = []string{tt.sourceDir}
			}

			sender := mc.MetaProvider()
			if sender != tt.expectProvider {
				t.Errorf("MetaProvider() = %v, want %v", sender, tt.expectProvider)
			}

			receiver := mc.MetaConsumer()
			if receiver != tt.expectConsumer {
				t.Errorf("MetaConsumer() = %v, want %v", receiver, tt.expectConsumer)
			}
		})
	}
}

func TestMetaConfigPeerMeta(t *testing.T) {
	tests := []struct {
		name         string
		timeout      int
		mode         string
		wantPeerMeta bool
		wantAutoMode bool
	}{
		{
			name:         "timeout=0 -> PeerMeta disabled",
			timeout:      0,
			wantPeerMeta: false,
			wantAutoMode: false,
		},
		{
			name:         "timeout>0, mode empty -> auto mode",
			timeout:      30,
			mode:         "",
			wantPeerMeta: true,
			wantAutoMode: true,
		},
		{
			name:         "timeout>0, mode=auto -> auto mode",
			timeout:      30,
			mode:         "auto",
			wantPeerMeta: true,
			wantAutoMode: true,
		},
		{
			name:         "timeout>0, mode=manual -> not auto mode",
			timeout:      30,
			mode:         "manual",
			wantPeerMeta: true,
			wantAutoMode: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &MetaConfig{
				PeerMetaTimeout: tt.timeout,
				PeerMetaMode:    tt.mode,
			}
			if mc.PeerMeta() != tt.wantPeerMeta {
				t.Errorf("PeerMeta() = %v, want %v", mc.PeerMeta(), tt.wantPeerMeta)
			}
			if mc.PeerMetaAutoMode() != tt.wantAutoMode {
				t.Errorf("PeerMetaAutoMode() = %v, want %v", mc.PeerMetaAutoMode(), tt.wantAutoMode)
			}
		})
	}
}

func TestRunSourceOp(t *testing.T) {
	newMeta := func(dirs ...string) *metaData {
		return &metaData{config: &MetaConfig{
			SourceDirs:    dirs,
			SourceManager: srcmgr.NewSourceManager(dirs, nil),
		}}
	}

	t.Run("infra error fails over to next replica", func(t *testing.T) {
		m := newMeta("/a", "/b")
		var calls []string
		eno := m.runSourceOp("test", "/", func(dir string) syscall.Errno {
			calls = append(calls, dir)
			if dir == "/a" {
				return syscall.EIO
			}
			return 0
		})
		assert.Equal(t, syscall.Errno(0), eno)
		assert.Equal(t, []string{"/a", "/b"}, calls)
	})

	t.Run("semantic error returns as-is without failover", func(t *testing.T) {
		m := newMeta("/a", "/b")
		var calls []string
		eno := m.runSourceOp("test", "/", func(dir string) syscall.Errno {
			calls = append(calls, dir)
			return syscall.ENOENT
		})
		assert.Equal(t, syscall.ENOENT, eno)
		assert.Equal(t, []string{"/a"}, calls)
	})

	t.Run("exhaustion returns last error", func(t *testing.T) {
		m := newMeta("/a", "/b")
		var calls []string
		eno := m.runSourceOp("test", "/", func(dir string) syscall.Errno {
			calls = append(calls, dir)
			return syscall.ESTALE
		})
		assert.Equal(t, syscall.ESTALE, eno)
		assert.Equal(t, []string{"/a", "/b"}, calls, "every replica must be tried before giving up")
	})

	t.Run("new op starts at current alive replica", func(t *testing.T) {
		m := newMeta("/a", "/b", "/c")
		// Fail replica /a over to /b through the production failover path.
		eno := m.runSourceOp("test", "/", func(dir string) syscall.Errno {
			if dir == "/a" {
				return syscall.EIO
			}
			return 0
		})
		assert.Equal(t, syscall.Errno(0), eno)

		var calls []string
		eno = m.runSourceOp("test", "/", func(dir string) syscall.Errno {
			calls = append(calls, dir)
			return 0
		})
		assert.Equal(t, syscall.Errno(0), eno)
		assert.Equal(t, []string{"/b"}, calls)
	})

	t.Run("nil manager degenerates to single attempt", func(t *testing.T) {
		m := &metaData{config: &MetaConfig{SourceDirs: []string{"/a"}}}
		var calls []string
		eno := m.runSourceOp("test", "/", func(dir string) syscall.Errno {
			calls = append(calls, dir)
			return syscall.EIO
		})
		assert.Equal(t, syscall.EIO, eno)
		assert.Equal(t, []string{"/a"}, calls, "an unwired manager falls back to the first source dir")
	})

	t.Run("empty replica list fails without calling op", func(t *testing.T) {
		m := newMeta()
		called := false
		eno := m.runSourceOp("test", "/", func(dir string) syscall.Errno {
			called = true
			return 0
		})
		assert.Equal(t, syscall.ENOENT, eno)
		assert.False(t, called, "op must never run with an empty dir")
	})

	t.Run("nil manager with no source dirs fails without calling op", func(t *testing.T) {
		m := &metaData{config: &MetaConfig{}}
		called := false
		eno := m.runSourceOp("test", "/", func(dir string) syscall.Errno {
			called = true
			return 0
		})
		assert.Equal(t, syscall.ENOENT, eno)
		assert.False(t, called, "op must never run with an empty dir")
	})
}

func TestStatFS(t *testing.T) {
	newMeta := func(dirs ...string) *metaData {
		m := &metaData{config: &MetaConfig{
			SourceDirs:    dirs,
			SourceManager: srcmgr.NewSourceManager(dirs, nil),
		}}
		m.InitMetrics(nil)
		return m
	}

	t.Run("semantic error does not fail over", func(t *testing.T) {
		good := t.TempDir()
		missing := filepath.Join(good, "nonexistent")
		m := newMeta(missing, good)
		var total, avail, itotal, iavail uint64
		// A missing replica dir fails statfs with ENOENT: a semantic error
		// that must be returned as-is, without failing the replica over.
		eno := m.StatFS(nil, &total, &avail, &itotal, &iavail)
		assert.Equal(t, syscall.ENOENT, eno)
		assert.Equal(t, missing, m.config.SourceManager.CurrentAliveDir(),
			"a semantic statfs error must not mark the replica failed")
		// The next call starts at the same replica and fails the same way.
		eno = m.StatFS(nil, &total, &avail, &itotal, &iavail)
		assert.Equal(t, syscall.ENOENT, eno)
		assert.Equal(t, missing, m.config.SourceManager.CurrentAliveDir())
	})

	t.Run("success on a live replica", func(t *testing.T) {
		m := newMeta(t.TempDir())
		var total, avail, itotal, iavail uint64
		eno := m.StatFS(nil, &total, &avail, &itotal, &iavail)
		assert.Equal(t, syscall.Errno(0), eno)
		assert.NotZero(t, total)
	})
}
