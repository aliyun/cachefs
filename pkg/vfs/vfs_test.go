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

package vfs

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/meta"
	cachefsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestInodeToCacheTarget(t *testing.T) {
	testAttr := &Attr{
		Length:    1024,
		Mtime:     1640995200,
		Mtimensec: 123456789,
	}
	fs := &Cachefs{
		Conf: &Config{
			SafeCacheKey: false,
		},
	}

	tests := []struct {
		name         string
		safeCacheKey bool
		path         string
		attr         *Attr
		expected     string
	}{
		{
			name:         "SafeCacheKey_false",
			safeCacheKey: false,
			path:         "/test/file.txt",
			attr:         testAttr,
			expected:     "/test/file.txt",
		},
		{
			name:         "SafeCacheKey_true",
			safeCacheKey: true,
			path:         "/test/file.txt",
			attr:         testAttr,
			expected:     "/test/file.txt:1024-1640995200-123456789",
		},
		{
			name:         "SafeCacheKey_true_empty_path",
			safeCacheKey: true,
			path:         "",
			attr:         testAttr,
			expected:     ":1024-1640995200-123456789",
		},
		{
			name:         "SafeCacheKey_true_zero_values",
			safeCacheKey: true,
			path:         "/test/file.txt",
			attr: &Attr{
				Length:    0,
				Mtime:     0,
				Mtimensec: 0,
			},
			expected: "/test/file.txt:0-0-0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs.Conf.SafeCacheKey = tt.safeCacheKey
			cino := cachefsmeta.NewInode(3, nil, tt.path, tt.attr.Typ, tt.attr)
			result := fs.inodeToCacheTarget(cino, tt.attr)
			if result != tt.expected {
				t.Errorf("inodeToCacheTarget() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestEncodeCacheTarget(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			SafeCacheKey: false,
		},
	}

	tests := []struct {
		name         string
		safeCacheKey bool
		path         string
		length       uint64
		mtime        int64
		mtimensec    uint32
		expected     string
	}{
		{
			name:         "SafeCacheKey_false",
			safeCacheKey: false,
			path:         "/test/file.txt",
			length:       1024,
			mtime:        1640995200,
			mtimensec:    123456789,
			expected:     "/test/file.txt",
		},
		{
			name:         "SafeCacheKey_true",
			safeCacheKey: true,
			path:         "/test/file.txt",
			length:       1024,
			mtime:        1640995200,
			mtimensec:    123456789,
			expected:     "/test/file.txt:1024-1640995200-123456789",
		},
		{
			name:         "SafeCacheKey_true_empty_path",
			safeCacheKey: true,
			path:         "",
			length:       1024,
			mtime:        1640995200,
			mtimensec:    123456789,
			expected:     ":1024-1640995200-123456789",
		},
		{
			name:         "SafeCacheKey_true_zero_values",
			safeCacheKey: true,
			path:         "/test/file.txt",
			length:       0,
			mtime:        0,
			mtimensec:    0,
			expected:     "/test/file.txt:0-0-0",
		},
		{
			name:         "SafeCacheKey_true_large_values",
			safeCacheKey: true,
			path:         "/test/large.file",
			length:       18446744073709551615, // max uint64
			mtime:        9223372036854775807,  // max int64
			mtimensec:    4294967295,           // max uint32
			expected:     "/test/large.file:18446744073709551615-9223372036854775807-4294967295",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs.Conf.SafeCacheKey = tt.safeCacheKey
			result := fs.encodeCacheTarget(tt.path, tt.length, tt.mtime, tt.mtimensec)
			if result != tt.expected {
				t.Errorf("encodeCacheTarget() = %v, expected %v", result, tt.expected)
				return
			}
		})
	}
}

func TestIsFuseLayerMode(t *testing.T) {
	tests := []struct {
		name   string
		conf   *Config
		expect bool
	}{
		{"no daemon socket", &Config{}, false},
		{"with daemon socket", &Config{DaemonSocket: "test"}, true},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expect, tc.conf.IsFuseLayerMode(), tc.name)
	}
}

func TestGetBlockIndex(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize: 4 << 20,
			},
		},
	}

	tests := []struct {
		name          string
		offset        uint64
		size          uint64
		expectedStart uint32
		expectedEnd   uint32
	}{
		{
			name:          "single block",
			offset:        0,
			size:          1024,
			expectedStart: 0,
			expectedEnd:   1,
		},
		{
			name:          "multiple blocks",
			offset:        0,
			size:          8 << 20,
			expectedStart: 0,
			expectedEnd:   2,
		},
		{
			name:          "offset in middle",
			offset:        2 << 20,
			size:          4 << 20,
			expectedStart: 0,
			expectedEnd:   2,
		},
		{
			name:          "cross block boundary",
			offset:        (4 << 20) - 1024,
			size:          2048,
			expectedStart: 0,
			expectedEnd:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sblk, eblk := fs.getBlockIndex(tt.offset, tt.size)
			assert.Equal(t, tt.expectedStart, sblk)
			assert.Equal(t, tt.expectedEnd, eblk)
		})
	}
}

func TestBytesInBlock(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			Store: &chunk.StoreConfig{
				BlockSize: 4 << 20,
			},
		},
	}

	blockSize := uint64(4 << 20)

	tests := []struct {
		name     string
		target   string
		offset   uint64
		size     uint64
		blknum   uint32
		expected uint32
	}{
		{
			name:     "full block",
			target:   "test",
			offset:   0,
			size:     blockSize,
			blknum:   0,
			expected: uint32(blockSize),
		},
		{
			name:     "partial block at start",
			target:   "test",
			offset:   0,
			size:     1024,
			blknum:   0,
			expected: 1024,
		},
		{
			name:     "partial block with offset",
			target:   "test",
			offset:   1024,
			size:     2048,
			blknum:   0,
			expected: 2048,
		},
		{
			name:     "cross block boundary",
			target:   "test",
			offset:   blockSize - 512,
			size:     1024,
			blknum:   0,
			expected: 512,
		},
		{
			name:     "second block",
			target:   "test",
			offset:   blockSize - 512,
			size:     1024,
			blknum:   1,
			expected: 512,
		},
		{
			name:     "out of range",
			target:   "test",
			offset:   0,
			size:     1024,
			blknum:   2,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fs.bytesInBlock(tt.target, tt.offset, tt.size, tt.blknum)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSetMountedAndWaitMount(t *testing.T) {
	fs := &Cachefs{
		mountCh: make(chan struct{}),
	}

	done := make(chan bool)
	go func() {
		fs.WaitMount()
		done <- true
	}()

	fs.SetMounted()
	<-done

	assert.Nil(t, fs.mountCh)
}

func TestSetMountedTwice(t *testing.T) {
	fs := &Cachefs{
		mountCh: make(chan struct{}),
	}

	fs.SetMounted()
	assert.Nil(t, fs.mountCh)

	fs.SetMounted()
	assert.Nil(t, fs.mountCh)
}

func TestWaitMountAlreadyMounted(t *testing.T) {
	fs := &Cachefs{
		mountCh: nil,
	}

	fs.WaitMount()
}

func TestReadOnlyOperations(t *testing.T) {
	fs := &Cachefs{}
	ctx := NewLogContext(meta.NewContext(1, 1000, []uint32{1000}))

	tests := []struct {
		name     string
		testFunc func() syscall.Errno
	}{
		{
			name: "SetAttr",
			testFunc: func() syscall.Errno {
				_, err := fs.SetAttr(ctx, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
				return err
			},
		},
		{
			name: "Mknod",
			testFunc: func() syscall.Errno {
				_, err := fs.Mknod(ctx, 1, "test", 0, 0, 0)
				return err
			},
		},
		{
			name: "Mkdir",
			testFunc: func() syscall.Errno {
				_, err := fs.Mkdir(ctx, 1, "test", 0, 0)
				return err
			},
		},
		{
			name: "Unlink",
			testFunc: func() syscall.Errno {
				return fs.Unlink(ctx, 1, "test")
			},
		},
		{
			name: "Rmdir",
			testFunc: func() syscall.Errno {
				return fs.Rmdir(ctx, 1, "test")
			},
		},
		{
			name: "Rename",
			testFunc: func() syscall.Errno {
				return fs.Rename(ctx, 1, "old", 2, "new", 0)
			},
		},
		{
			name: "Link",
			testFunc: func() syscall.Errno {
				_, err := fs.Link(ctx, 1, 2, "test")
				return err
			},
		},
		{
			name: "Symlink",
			testFunc: func() syscall.Errno {
				_, err := fs.Symlink(ctx, "/path", 1, "test")
				return err
			},
		},
		{
			name: "SetXattr",
			testFunc: func() syscall.Errno {
				return fs.SetXattr(ctx, 1, "key", []byte("value"), 0)
			},
		},
		{
			name: "RemoveXattr",
			testFunc: func() syscall.Errno {
				return fs.RemoveXattr(ctx, 1, "key")
			},
		},
		{
			name: "Create",
			testFunc: func() syscall.Errno {
				_, _, err := fs.Create(ctx, 1, "test", 0, 0, 0)
				return err
			},
		},
		{
			name: "Fsync",
			testFunc: func() syscall.Errno {
				return fs.Fsync(ctx, 1, 0, 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.testFunc()
			assert.Equal(t, syscall.EROFS, err)
		})
	}
}

func TestUnsupportedOperations(t *testing.T) {
	fs := &Cachefs{}
	ctx := NewLogContext(meta.NewContext(1, 1000, []uint32{1000}))

	t.Run("GetXattr", func(t *testing.T) {
		_, err := fs.GetXattr(ctx, 1, "key", 100)
		assert.Equal(t, syscall.ENOTSUP, err)
	})

	t.Run("ListXattr", func(t *testing.T) {
		_, err := fs.ListXattr(ctx, 1, 100)
		assert.Equal(t, syscall.ENOTSUP, err)
	})

	t.Run("Fallocate", func(t *testing.T) {
		err := fs.Fallocate(ctx, 1, 0, 0, 1024, 0)
		assert.Equal(t, syscall.ENOTSUP, err)
	})

	t.Run("CopyFileRange", func(t *testing.T) {
		_, err := fs.CopyFileRange(ctx, 1, 0, 0, 2, 0, 0, 1024, 0)
		assert.Equal(t, syscall.ENOTSUP, err)
	})

	t.Run("Getlk", func(t *testing.T) {
		var start, len uint64
		var typ, pid uint32
		err := fs.Getlk(ctx, 1, 0, 0, &start, &len, &typ, &pid)
		assert.Equal(t, syscall.ENOTSUP, err)
	})

	t.Run("Setlk", func(t *testing.T) {
		err := fs.Setlk(ctx, 1, 0, 0, 0, 1024, 0, 0, false)
		assert.Equal(t, syscall.ENOTSUP, err)
	})

	t.Run("Flock", func(t *testing.T) {
		err := fs.Flock(ctx, 1, 0, 0, 0, false)
		assert.Equal(t, syscall.ENOTSUP, err)
	})
}

func TestFlush(t *testing.T) {
	fs := &Cachefs{}
	ctx := NewLogContext(meta.NewContext(1, 1000, []uint32{1000}))

	err := fs.Flush(ctx, 1, 0, 0)
	assert.Equal(t, syscall.Errno(0), err)
}

func TestReleasedir(t *testing.T) {
	fs := &Cachefs{
		handles: make(map[Ino][]*handle),
	}
	ctx := NewLogContext(meta.NewContext(1, 1000, []uint32{1000}))

	result := fs.Releasedir(ctx, 1, 0)
	assert.Equal(t, 0, result)
}

func TestCachefs_PendingCache(t *testing.T) {
	fs := &Cachefs{
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}

	target := "test-target"
	blk := uint32(10)

	t.Run("AddAndCheckPendingCache", func(t *testing.T) {
		// Initially no pending cache
		cached := fs.checkPendingCache(target, blk)
		if cached {
			t.Error("Expected no pending cache, but got cached=true")
			return
		}

		// Add pending cache
		fs.addPendingCache(target, blk)
		key := chunk.CacheKey{Target: target, Blk: blk}
		if !fs.pendingOps.Exists(key.String()) {
			t.Error("Expected pending cache to be added")
			return
		}
	})

	t.Run("RemovePendingCache", func(t *testing.T) {
		target2 := "test-target-2"
		blk2 := uint32(20)

		// Add pending cache
		fs.addPendingCache(target2, blk2)
		key := chunk.CacheKey{Target: target2, Blk: blk2}

		// Remove pending cache
		fs.removePendingCache(target2, blk2)
		if fs.pendingOps.Exists(key.String()) {
			t.Error("Expected pending cache to be removed")
			return
		}

		// Remove non-existent key should not panic
		fs.removePendingCache("non-existent", 999)
	})

	t.Run("CheckPendingCacheTimeout", func(t *testing.T) {
		target3 := "test-target-3"
		blk3 := uint32(30)

		// Add pending cache but never remove it
		fs.addPendingCache(target3, blk3)

		start := time.Now()
		cached := fs.checkPendingCache(target3, blk3)
		duration := time.Since(start)

		// Should timeout and return false
		if cached {
			t.Error("Expected timeout with cached=false, but got cached=true")
			return
		}

		// Should take approximately 500ms (timeout duration)
		if duration < 400*time.Millisecond || duration > 600*time.Millisecond {
			t.Errorf("Expected timeout around 500ms, got %v", duration)
			return
		}

		// Clean up
		fs.removePendingCache(target3, blk3)
	})

	t.Run("CheckPendingCacheSignaled", func(t *testing.T) {
		target4 := "test-target-4"
		blk4 := uint32(40)

		// Add pending cache
		fs.addPendingCache(target4, blk4)

		// Signal after 100ms
		go func() {
			time.Sleep(100 * time.Millisecond)
			fs.removePendingCache(target4, blk4)
		}()

		start := time.Now()
		cached := fs.checkPendingCache(target4, blk4)
		duration := time.Since(start)

		// Should be signaled and return true
		if !cached {
			t.Error("Expected to be signaled with cached=true, but got cached=false")
			return
		}

		// Should take approximately 100ms (signal time)
		if duration < 50*time.Millisecond || duration > 200*time.Millisecond {
			t.Errorf("Expected signal around 100ms, got %v", duration)
			return
		}
	})
}

func TestCachefs_ConcurrentPendingCache(t *testing.T) {
	fs := &Cachefs{
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}

	target := "test-target-concurrent"
	blk := uint32(100)

	// Add pending cache to simulate first reader is adding data to cache
	fs.addPendingCache(target, blk)

	// Start multiple concurrent checkers
	numCheckers := 5
	results := make(chan bool, numCheckers)
	startTime := time.Now()

	for i := 0; i < numCheckers; i++ {
		go func() {
			cached := fs.checkPendingCache(target, blk)
			results <- cached
		}()
	}

	// Remove pending cache after 150ms to simulate cache operation completion
	go func() {
		time.Sleep(150 * time.Millisecond)
		fs.removePendingCache(target, blk)
	}()

	// Collect results
	for i := 0; i < numCheckers; i++ {
		select {
		case cached := <-results:
			// All should be signaled and return true
			if !cached {
				t.Errorf("Checker %d: Expected to be signaled with cached=true, but got cached=false", i)
				return
			}
		case <-time.After(500 * time.Millisecond):
			t.Errorf("Checker %d: timed out waiting for result", i)
			return
		}
	}

	duration := time.Since(startTime)
	// All checkers should complete around the same time (150ms)
	if duration < 100*time.Millisecond || duration > 250*time.Millisecond {
		t.Errorf("Expected all checkers to complete around 150ms, got %v", duration)
		return
	}
}

func TestCachefs_MultiplePendingCaches(t *testing.T) {
	fs := &Cachefs{
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}

	// Test multiple different targets/blocks can coexist in pending cache
	targets := []struct {
		target string
		blk    uint32
	}{
		{"target1", 1},
		{"target2", 2},
		{"target3", 3},
	}

	// Add all to pending cache
	for _, tc := range targets {
		fs.addPendingCache(tc.target, tc.blk)
	}

	// Verify all exist
	if fs.pendingOps.Count() != len(targets) {
		t.Errorf("Expected %d pending caches, got %d", len(targets), fs.pendingOps.Count())
		return
	}

	// Remove one by one and verify others still exist
	for i, tc := range targets {
		fs.removePendingCache(tc.target, tc.blk)
		expectedLen := len(targets) - i - 1
		if fs.pendingOps.Count() != expectedLen {
			t.Errorf("After removing %d items, expected %d pending caches, got %d", i+1, expectedLen, fs.pendingOps.Count())
			return
		}
	}
}

func TestParsePinPaths(t *testing.T) {
	newCachefs := func(conf PinPathConfig) *Cachefs {
		return &Cachefs{Conf: &Config{PinPath: conf}}
	}

	t.Run("empty config", func(t *testing.T) {
		fs := newCachefs(PinPathConfig{})
		result, _, _, _ := fs.parsePinPaths()
		if len(result) != 0 {
			t.Errorf("expected empty, got %v", result)
		}
	})

	t.Run("paths from static config", func(t *testing.T) {
		fs := newCachefs(PinPathConfig{Paths: "/models/llama /data/embeddings"})
		result, _, _, _ := fs.parsePinPaths()
		expected := []string{"/models/llama/", "/data/embeddings/"}
		if len(result) != len(expected) {
			t.Fatalf("expected %v, got %v", expected, result)
		}
		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("result[%d] = %q, want %q", i, result[i], expected[i])
			}
		}
	})

	t.Run("path.Clean normalizes trailing slash", func(t *testing.T) {
		fs := newCachefs(PinPathConfig{Paths: "/models/llama/ /data//embeddings/"})
		result, _, _, _ := fs.parsePinPaths()
		expected := []string{"/models/llama/", "/data/embeddings/"}
		if len(result) != len(expected) {
			t.Fatalf("expected %v, got %v", expected, result)
		}
		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("result[%d] = %q, want %q", i, result[i], expected[i])
			}
		}
	})

	t.Run("root path rejected", func(t *testing.T) {
		fs := newCachefs(PinPathConfig{Paths: "/ /models/llama"})
		result, _, _, _ := fs.parsePinPaths()
		if len(result) != 1 || result[0] != "/models/llama/" {
			t.Errorf("expected [\"/models/llama/\"], got %v", result)
		}
	})

	t.Run("non-slash-prefix rejected", func(t *testing.T) {
		fs := newCachefs(PinPathConfig{Paths: "relative/path /valid/path"})
		result, _, _, _ := fs.parsePinPaths()
		if len(result) != 1 || result[0] != "/valid/path/" {
			t.Errorf("expected [\"/valid/path/\"], got %v", result)
		}
	})

	t.Run("file overrides paths", func(t *testing.T) {
		dir := t.TempDir()
		f := dir + "/pins.txt"
		if err := os.WriteFile(f, []byte("/from/file/\n"), 0644); err != nil {
			t.Errorf("write pin file: %s", err)
			return
		}
		fs := newCachefs(PinPathConfig{Paths: "/from/static", File: f})
		result, _, _, _ := fs.parsePinPaths()
		if len(result) != 1 || result[0] != "/from/file/" {
			t.Errorf("expected [\"/from/file/\"], got %v", result)
		}
	})

	t.Run("nonexistent file returns error", func(t *testing.T) {
		fs := newCachefs(PinPathConfig{File: "/nonexistent/path"})
		result, _, _, err := fs.parsePinPaths()
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
	})

	t.Run("cmd overrides file and paths", func(t *testing.T) {
		fs := newCachefs(PinPathConfig{
			Paths: "/from/static",
			Cmd:   "printf /from/cmd/\\n",
		})
		result, _, _, _ := fs.parsePinPaths()
		if len(result) != 1 || result[0] != "/from/cmd/" {
			t.Errorf("expected [\"/from/cmd/\"], got %v", result)
		}
	})

	// cmdOut must be non-empty only in Cmd mode: callers rely on cmdOut != "" to decide
	// whether to log the command result. Even an empty-output command yields "<empty>",
	// while File/Paths/empty modes must return "".
	t.Run("cmdOut only set in cmd mode", func(t *testing.T) {
		dir := t.TempDir()
		f := dir + "/pins.txt"
		if err := os.WriteFile(f, []byte("/from/file/\n"), 0644); err != nil {
			t.Fatalf("write pin file: %s", err)
		}
		cases := []struct {
			name     string
			conf     PinPathConfig
			nonEmpty bool
		}{
			{"empty", PinPathConfig{}, false},
			{"paths", PinPathConfig{Paths: "/from/static"}, false},
			{"file", PinPathConfig{File: f}, false},
			{"cmd", PinPathConfig{Cmd: "printf /from/cmd/\\n"}, true},
			{"cmd empty output", PinPathConfig{Cmd: "true"}, true},
		}
		for _, tc := range cases {
			_, cmdOut, _, err := newCachefs(tc.conf).parsePinPaths()
			assert.NoError(t, err, tc.name)
			if tc.nonEmpty {
				assert.NotEmpty(t, cmdOut, tc.name)
			} else {
				assert.Empty(t, cmdOut, tc.name)
			}
		}
	})
}

func TestInitPinPathFuseLayerMode(t *testing.T) {
	fs := &Cachefs{
		Conf: &Config{
			DaemonSocket: "/tmp/test.sock",
			PinPath:      PinPathConfig{Paths: "/models"},
		},
		stopCh: make(chan struct{}),
	}
	fs.initPinPath()
	if fs.pinPaths != nil {
		t.Errorf("expected pinPaths to be nil in fuse layer mode, got %v", fs.pinPaths)
	}
}
