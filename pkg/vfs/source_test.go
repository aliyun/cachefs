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
	"math/rand"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/aliyun/cachefs/pkg/srcmgr"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSourceStore(t *testing.T) {
	fs := &Cachefs{}
	dir := "/test/dir"
	bs := uint32(4 << 20)

	s := newSourceStore(fs, []string{dir}, bs)
	assert.NotNil(t, s)

	src, ok := s.(*source)
	assert.True(t, ok)
	assert.Equal(t, fs, src.fs)
	assert.Equal(t, dir, src.sm.CurrentAliveDir())
	assert.Equal(t, 1, src.sm.Len())
	assert.Equal(t, bs, src.bs)
	assert.NotNil(t, src.files)
}

func TestNewSourceFile(t *testing.T) {
	fs := &Cachefs{}
	dir := "/test/dir"
	bs := uint32(4 << 20)
	s := newSourceStore(fs, []string{dir}, bs).(*source)

	target := "file.txt"
	length := uint64(1024)

	sf := s.newSourceFile(target, length)
	assert.NotNil(t, sf)

	srcFile, ok := sf.(*sourceFile)
	assert.True(t, ok)
	assert.Equal(t, target, srcFile.target)
	assert.Equal(t, length, srcFile.length)
	assert.Equal(t, bs, srcFile.bs)
	assert.Equal(t, uint32(1), srcFile.refs)
	assert.Equal(t, s, srcFile.source)
}

func TestSourceFileAcquireRelease(t *testing.T) {
	fs := &Cachefs{}
	dir := "/test"
	s := newSourceStore(fs, []string{dir}, 4<<20).(*source)

	target := "file.txt"
	length := uint64(1024)

	sf := s.newSourceFile(target, length).(*sourceFile)
	assert.Equal(t, uint32(1), sf.refs)

	sf.acquire()
	assert.Equal(t, uint32(2), sf.refs)

	sf.acquire()
	assert.Equal(t, uint32(3), sf.refs)

	sf.release()
	assert.Equal(t, uint32(2), sf.refs)

	sf.release()
	assert.Equal(t, uint32(1), sf.refs)

	sf.release()
	assert.Equal(t, uint32(0), sf.refs)
}

func TestSourceFileReadAt(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	content := make([]byte, 8<<20)
	for i := range content {
		content[i] = byte(i % 256)
	}
	err := os.WriteFile(testFile, content, 0644)
	assert.NoError(t, err)

	fs := &Cachefs{}
	s := newSourceStore(fs, []string{tmpDir}, 4<<20).(*source)

	sf := s.newSourceFile("test.txt", uint64(len(content))).(*sourceFile)

	tests := []struct {
		name      string
		bufSize   int
		offset    int64
		expectErr bool
	}{
		{
			name:      "read first block",
			bufSize:   4 << 20,
			offset:    0,
			expectErr: false,
		},
		{
			name:      "read second block",
			bufSize:   4 << 20,
			offset:    4 << 20,
			expectErr: false,
		},
		{
			name:      "read partial block",
			bufSize:   1024,
			offset:    0,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, tt.bufSize)
			n, err := sf.ReadAt(buf, tt.offset)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.bufSize, n)
				for i := 0; i < n; i++ {
					expected := byte((int(tt.offset) + i) % 256)
					assert.Equal(t, expected, buf[i], "mismatch at offset %d", i)
				}
			}
		})
	}

	sf.release()
}

func TestSourceFileReadAtNonExistent(t *testing.T) {
	tmpDir := t.TempDir()

	fs := &Cachefs{}
	s := newSourceStore(fs, []string{tmpDir}, 4<<20).(*source)

	sf := s.newSourceFile("nonexistent.txt", 1024).(*sourceFile)

	buf := make([]byte, 1024)
	_, err := sf.ReadAt(buf, 0)
	assert.Error(t, err)

	sf.release()
}

func TestSourceFileReadAtSizeChanged(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	content := make([]byte, 1024)
	err := os.WriteFile(testFile, content, 0644)
	assert.NoError(t, err)

	fs := &Cachefs{}
	s := newSourceStore(fs, []string{tmpDir}, 4<<20).(*source)

	sf := s.newSourceFile("test.txt", 2048).(*sourceFile)

	buf := make([]byte, 1024)
	_, err = sf.ReadAt(buf, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "size changed")

	sf.release()
}

func TestSourceFileMultipleReferences(t *testing.T) {
	fs := &Cachefs{}
	dir := "/test"
	s := newSourceStore(fs, []string{dir}, 4<<20).(*source)

	target := "file.txt"
	length := uint64(1024)

	sf1 := s.newSourceFile(target, length).(*sourceFile)
	assert.Equal(t, uint32(1), sf1.refs)

	sf2 := s.newSourceFile(target, length).(*sourceFile)
	assert.Equal(t, sf1, sf2)
	assert.Equal(t, uint32(2), sf2.refs)

	sf1.release()
	assert.Equal(t, uint32(1), sf1.refs)

	sf2.release()
	assert.Equal(t, uint32(0), sf2.refs)
}

const testBlockSize = 128 << 10 // 131072, a multiple of utils.DirectIOAlignSize(4096)

// directIOWorks probes whether the filesystem backing dir supports O_DIRECT for
// both open and an aligned read (tmpfs on older kernels does not).
func directIOWorks(t *testing.T, dir string) bool {
	t.Helper()
	p := filepath.Join(dir, ".diocheck")
	require.NoError(t, os.WriteFile(p, make([]byte, utils.DirectIOAlignSize), 0o600))
	defer os.Remove(p)
	f, err := utils.OpenDirect(p, os.O_RDONLY, 0)
	if err != nil {
		return false
	}
	defer f.Close()
	buf := utils.AllocAlignedBuf(utils.DirectIOAlignSize)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return false
	}
	return true
}

// newTestSource builds a source directly (bypassing newSourceStore so no global
// gauge state is mutated) for exercising sourceFile.ReadAt.
func newTestSource(dir string, bs uint32, directIO bool) *source {
	s := &source{
		sm:       srcmgr.NewSourceManager([]string{dir}, nil),
		bs:       bs,
		files:    make(map[string]*sourceFile),
		directIO: directIO,
		dios:     map[string]*utils.DioGate{dir: utils.NewDioGate("source reads " + dir)},
	}
	return s
}

func writeRandomFile(t *testing.T, dir, name string, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	rand.New(rand.NewSource(int64(size) + 1)).Read(data)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o600))
	return data
}

// readAllBlocks reads a source file block by block via sourceFile.ReadAt and
// returns the concatenated bytes. Buffers are aligned to mimic production pages.
func readAllBlocks(t *testing.T, s *source, name string, length uint64) ([]byte, *sourceFile) {
	t.Helper()
	sfStore := s.newSourceFile(name, length)
	sf := sfStore.(*sourceFile)
	bs := int(s.bs)
	out := make([]byte, 0, length)
	for off := 0; off < int(length); off += bs {
		blklen := bs
		if remain := int(length) - off; remain < bs {
			blklen = remain
		}
		buf := utils.AllocAlignedBuf(blklen)
		n, err := sf.ReadAt(buf, int64(off))
		require.NoError(t, err)
		require.Equal(t, blklen, n)
		out = append(out, buf[:n]...)
	}
	return out, sf
}

func TestBufAligned(t *testing.T) {
	require.True(t, utils.BufAligned(utils.AllocAlignedBuf(utils.DirectIOAlignSize)))
	require.True(t, utils.BufAligned(utils.AllocAlignedBuf(testBlockSize)))
	// A one-byte-shifted slice of an aligned buffer is misaligned.
	big := utils.AllocAlignedBuf(utils.DirectIOAlignSize * 2)
	require.False(t, utils.BufAligned(big[1:]))
	// Start address aligned but length not a multiple of the alignment size.
	require.False(t, utils.BufAligned(big[:utils.DirectIOAlignSize+100]))
	// An empty slice must not panic and must return false.
	require.False(t, utils.BufAligned(nil))
	require.False(t, utils.BufAligned([]byte{}))
}

func TestVerifySize(t *testing.T) {
	dir := t.TempDir()
	data := writeRandomFile(t, dir, "f", utils.DirectIOAlignSize)
	open := func() *os.File {
		f, err := os.Open(filepath.Join(dir, "f"))
		require.NoError(t, err)
		t.Cleanup(func() { f.Close() })
		return f
	}

	// plaintext, correct length -> ok
	s := newTestSource(dir, utils.DirectIOAlignSize, false)
	sf := s.newSourceFile("f", uint64(len(data))).(*sourceFile)
	require.NoError(t, sf.verifySize(open()))
	sf.release()

	// plaintext, wrong length -> error
	s = newTestSource(dir, utils.DirectIOAlignSize, false)
	sf = s.newSourceFile("f", uint64(len(data)+1)).(*sourceFile)
	err := sf.verifySize(open())
	require.Error(t, err)
	require.Contains(t, err.Error(), "size changed")
	sf.release()
}

func TestGetBufferedFile(t *testing.T) {
	dir := t.TempDir()
	writeRandomFile(t, dir, "f", utils.DirectIOAlignSize)

	// cached and reused across calls
	s := newTestSource(dir, utils.DirectIOAlignSize, false)
	sf := s.newSourceFile("f", utils.DirectIOAlignSize).(*sourceFile)
	f1, err := sf.getBufferedFile(dir)
	require.NoError(t, err)
	require.NotNil(t, f1)
	f2, err := sf.getBufferedFile(dir)
	require.NoError(t, err)
	require.Same(t, f1, f2, "buffered file should be cached and reused")
	require.Same(t, f1, sf.buffered.f)
	sf.release()
	require.Nil(t, sf.buffered, "release must close and clear the handle")

	// size mismatch -> error, nothing cached
	s = newTestSource(dir, utils.DirectIOAlignSize, false)
	sf = s.newSourceFile("f", utils.DirectIOAlignSize*2).(*sourceFile)
	_, err = sf.getBufferedFile(dir)
	require.Error(t, err)
	require.Nil(t, sf.buffered)
	sf.release()
}

func TestGetDirectFile(t *testing.T) {
	dir := t.TempDir()
	writeRandomFile(t, dir, "f", testBlockSize)
	s := newTestSource(dir, testBlockSize, true)
	sf := s.newSourceFile("f", testBlockSize).(*sourceFile)
	defer sf.release()

	f1, err := sf.getDirectFile(dir)
	if err != nil {
		// Filesystem doesn't support O_DIRECT: must be EINVAL and must have
		// permanently disabled direct IO without caching a handle.
		require.ErrorIs(t, err, syscall.EINVAL)
		require.True(t, s.dios[dir].Disabled())
		require.Nil(t, sf.direct)
		return
	}
	require.NotNil(t, f1)
	f2, err := sf.getDirectFile(dir)
	require.NoError(t, err)
	require.Same(t, f1, f2, "direct file should be cached and reused")
	require.Same(t, f1, sf.direct.f)
	require.False(t, s.dios[dir].Disabled())
}

func TestSourceFileReadCorrectness(t *testing.T) {
	bs := testBlockSize
	sizes := []int{1, 100, bs - 1, bs, bs + 1, bs * 2, bs*2 + 1234, bs * 3}
	for _, directIO := range []bool{false, true} {
		for _, size := range sizes {
			dir := t.TempDir()
			data := writeRandomFile(t, dir, "f", size)
			s := newTestSource(dir, uint32(bs), directIO)
			got, sf := readAllBlocks(t, s, "f", uint64(size))
			require.Equal(t, data, got, "directIO=%v size=%d", directIO, size)
			sf.release()
		}
	}
}

func TestSourceFileForceDirectIO(t *testing.T) {
	dir := t.TempDir()
	if !directIOWorks(t, dir) {
		t.Skip("source filesystem does not support O_DIRECT, skipping forced direct IO test")
	}
	bs := testBlockSize
	// A multi-block file with a partial tail: exercises both O_DIRECT full-block
	// reads and the buffered tail read.
	size := bs*3 + 777
	data := writeRandomFile(t, dir, "f", size)

	s := newTestSource(dir, uint32(bs), true)
	sourceDirectIOEnabledPerSource.WithLabelValues(dir).Set(1)

	got, sf := readAllBlocks(t, s, "f", uint64(size))
	defer sf.release()
	require.Equal(t, data, got)

	// The O_DIRECT path must have been taken (full-block reads opened directFile)
	// and must not have been permanently downgraded.
	require.NotNil(t, sf.direct, "expected O_DIRECT file to be opened for full-block reads")
	require.NotNil(t, sf.buffered, "expected buffered file to be opened for the partial tail")
	require.False(t, s.dios[dir].Disabled(), "direct IO should not have been downgraded")
	require.Equal(t, float64(1), testutil.ToFloat64(sourceDirectIOEnabledPerSource.WithLabelValues(dir)))
}

// TestSourceDirectIOEnabled verifies that --source-direct-io turns on O_DIRECT for
// source reads and publishes that state on the per-source gauge. Cache pages are
// always mmap/memfd backed, hence page-aligned, so nothing else gates it.
func TestSourceDirectIOEnabled(t *testing.T) {
	fs := &Cachefs{Conf: &Config{SourceDirectIO: true}}
	s := newSourceStore(fs, []string{t.TempDir()}, testBlockSize).(*source)
	dir := s.sm.Dirs()[0]

	require.True(t, s.directIO)
	require.True(t, s.directIOEnabled(dir))
	require.Equal(t, float64(1),
		testutil.ToFloat64(sourceDirectIOEnabledPerSource.WithLabelValues(dir)))
}

func TestDisableDirectIODowngrade(t *testing.T) {
	dir := t.TempDir()
	s := newTestSource(dir, testBlockSize, true)
	sourceDirectIOEnabledPerSource.WithLabelValues(dir).Set(1)

	require.True(t, s.directIOEnabled(dir))

	s.disableDirectIO(dir, syscall.EINVAL)

	require.False(t, s.directIOEnabled(dir))
	require.True(t, s.dios[dir].Disabled())
	require.Equal(t, float64(0), testutil.ToFloat64(sourceDirectIOEnabledPerSource.WithLabelValues(dir)))

	// Idempotent: a second call must not panic and keeps state.
	s.disableDirectIO(dir, syscall.EINVAL)
	require.False(t, s.directIOEnabled(dir))
}

// newTestSourceDirs builds a multi-replica source directly for exercising the
// fallback loop in sourceFile.ReadAt.
func newTestSourceDirs(dirs []string, bs uint32) *source {
	s := &source{
		sm:    srcmgr.NewSourceManager(dirs, nil),
		bs:    bs,
		files: make(map[string]*sourceFile),
		dios:  make(map[string]*utils.DioGate, len(dirs)),
	}
	for _, dir := range dirs {
		s.dios[dir] = utils.NewDioGate("source reads " + dir)
	}
	return s
}

func TestReadAtFallbackOnInfraError(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	data := writeRandomFile(t, dir1, "f", testBlockSize)
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "f"), data, 0o600))

	s := newTestSourceDirs([]string{dir1, dir2}, testBlockSize)
	sf := s.newSourceFile("f", testBlockSize).(*sourceFile)
	defer sf.release()

	var injected []string
	sourceReadFaultInjector = func(dir string, realRead func() (int, error)) (int, error) {
		injected = append(injected, dir)
		if dir == dir1 {
			return 0, syscall.EIO
		}
		return realRead()
	}
	defer func() { sourceReadFaultInjector = nil }()

	errorsBefore := testutil.ToFloat64(sourceReadErrorsPerSource.WithLabelValues(dir1))
	readsBefore := testutil.ToFloat64(sourceReadsPerSource.WithLabelValues(dir2))

	buf := make([]byte, testBlockSize)
	n, err := sf.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, testBlockSize, n)
	require.Equal(t, data, buf)
	require.Equal(t, []string{dir1, dir2}, injected, "first attempt on replica dir1, then failover to dir2")
	require.Equal(t, dir2, sf.buffered.dir, "file must be rebound to the fallback replica")
	require.Equal(t, float64(1), testutil.ToFloat64(sourceReadErrorsPerSource.WithLabelValues(dir1))-errorsBefore,
		"failed attempt on replica dir1 must be counted even though the read was rescued")
	require.Equal(t, float64(1), testutil.ToFloat64(sourceReadsPerSource.WithLabelValues(dir2))-readsBefore)
}

func TestReadAtNoFallbackOnENOENT(t *testing.T) {
	dir1 := t.TempDir() // replica missing the file
	dir2 := t.TempDir()
	writeRandomFile(t, dir2, "f", testBlockSize)

	s := newTestSourceDirs([]string{dir1, dir2}, testBlockSize)
	sf := s.newSourceFile("f", testBlockSize).(*sourceFile)
	defer sf.release()

	_, err := sf.ReadAt(make([]byte, testBlockSize), 0)
	require.ErrorIs(t, err, os.ErrNotExist, "ENOENT is semantic and must surface as-is")
}

func TestReadAtInfraErrorExhausted(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	s := newTestSourceDirs([]string{dir1, dir2}, testBlockSize)
	sf := s.newSourceFile("f", testBlockSize).(*sourceFile)
	defer sf.release()

	var injected []string
	sourceReadFaultInjector = func(dir string, realRead func() (int, error)) (int, error) {
		injected = append(injected, dir)
		return 0, syscall.ESTALE
	}
	defer func() { sourceReadFaultInjector = nil }()

	_, err := sf.ReadAt(make([]byte, testBlockSize), 0)
	require.ErrorIs(t, err, syscall.ESTALE, "when all replicas fail, the last error is returned as-is")
	require.Equal(t, []string{dir1, dir2}, injected, "every replica must be tried before giving up")
}

func TestReadAtLastReplicaServesWhenAllDead(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	data := writeRandomFile(t, dir1, "f", testBlockSize)
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "f"), data, 0o600))

	// Fail every replica through the production read path: the read returns
	// the last error and marks both replicas failed.
	s := newTestSourceDirs([]string{dir1, dir2}, testBlockSize)
	sourceReadFaultInjector = func(dir string, realRead func() (int, error)) (int, error) {
		return 0, syscall.EIO
	}
	defer func() { sourceReadFaultInjector = nil }()
	sf := s.newSourceFile("f", testBlockSize).(*sourceFile)
	defer sf.release()
	_, err := sf.ReadAt(make([]byte, testBlockSize), 0)
	require.ErrorIs(t, err, syscall.EIO)

	sourceReadFaultInjector = nil
	buf := make([]byte, testBlockSize)
	n, err := sf.ReadAt(buf, 0)
	require.NoError(t, err, "the last replica keeps serving even when marked failed")
	require.Equal(t, testBlockSize, n)
	require.Equal(t, data, buf)
}

func TestReadAtFollowsLiveReplicaAfterFailure(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	data := writeRandomFile(t, dir1, "f", testBlockSize)
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "f"), data, 0o600))
	writeRandomFile(t, dir1, "probe", testBlockSize)
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "probe"), data, 0o600))
	defer func() { sourceReadFaultInjector = nil }()
	eioOnDir1 := func(dir string, realRead func() (int, error)) (int, error) {
		if dir == dir1 {
			return 0, syscall.EIO
		}
		return realRead()
	}

	// Replica dir1 fails before the file is read: a probe read on another file
	// takes the error, fails over to dir2, and marks dir1 failed through the
	// production read path.
	s := newTestSourceDirs([]string{dir1, dir2}, testBlockSize)
	sourceReadFaultInjector = eioOnDir1
	probe := s.newSourceFile("probe", testBlockSize).(*sourceFile)
	_, err := probe.ReadAt(make([]byte, testBlockSize), 0)
	require.NoError(t, err, "the probe read must fail over to dir2")
	probe.release()

	// A fresh file must start on the live replica and never attempt dir1.
	var attempted []string
	sourceReadFaultInjector = func(dir string, realRead func() (int, error)) (int, error) {
		attempted = append(attempted, dir)
		return realRead()
	}
	sf := s.newSourceFile("f", testBlockSize).(*sourceFile)
	buf := make([]byte, testBlockSize)
	n, err := sf.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, testBlockSize, n)
	require.Equal(t, data, buf)
	require.Equal(t, []string{dir2}, attempted, "must never attempt the failed replica")
	require.Equal(t, dir2, sf.buffered.dir, "must start on the live replica")
	sf.release()

	// Replica dir1 fails while the file is bound to it and holding an open
	// handle: the read takes the error, fails over, and must rebind to the
	// live replica and retire the failed replica's handle.
	s = newTestSourceDirs([]string{dir1, dir2}, testBlockSize)
	sf = s.newSourceFile("f", testBlockSize).(*sourceFile)
	defer sf.release()
	buf = make([]byte, testBlockSize)
	n, err = sf.ReadAt(buf, 0)
	require.NoError(t, err)
	require.NotNil(t, sf.buffered)
	require.Equal(t, dir1, sf.buffered.dir)
	failedHandle := sf.buffered.f

	sourceReadFaultInjector = eioOnDir1
	buf = make([]byte, testBlockSize)
	n, err = sf.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, testBlockSize, n)
	require.Equal(t, data, buf)
	require.Contains(t, sf.staleFiles, failedHandle,
		"the failed replica's handle must be retired, not reused")
	require.Equal(t, dir2, sf.buffered.dir, "a read must rebind to the live replica")
	sourceReadFaultInjector = nil
}

func TestParseSourceFaultSpec(t *testing.T) {
	t.Run("valid specs", func(t *testing.T) {
		for spec, want := range map[string]struct {
			eno syscall.Errno
			dir string
		}{
			"eio:/a":          {syscall.EIO, "/a"},
			"EIO:/b":          {syscall.EIO, "/b"},
			"estale:/a":       {syscall.ESTALE, "/a"},
			"enotconn:/a":     {syscall.ENOTCONN, "/a"},
			"euclean:/a":      {syscall.EUCLEAN, "/a"},
			"eremoteio:/a":    {syscall.EREMOTEIO, "/a"},
			"eio:*":           {syscall.EIO, "*"},
			"eio:/with:colon": {syscall.EIO, "/with:colon"},
		} {
			eno, dir, err := parseSourceFaultSpec(spec)
			require.NoError(t, err, spec)
			require.Equal(t, want.eno, eno, spec)
			require.Equal(t, want.dir, dir, spec)
		}
	})

	t.Run("invalid specs", func(t *testing.T) {
		for _, spec := range []string{
			"", "eio", "eio:", ":/a", "enoent:/a", "eperm:/a", // not whitelisted
		} {
			_, _, err := parseSourceFaultSpec(spec)
			require.Error(t, err, spec)
		}
	})
}

func TestSourceFaultInjectorFor(t *testing.T) {
	realRead := func() (int, error) { return 7, nil }

	inj := sourceFaultInjectorFor(syscall.EIO, "/b")
	n, err := inj("/a", realRead)
	require.NoError(t, err, "non-matching replica delegates to the real read")
	require.Equal(t, 7, n)
	_, err = inj("/b", realRead)
	require.ErrorIs(t, err, syscall.EIO)

	inj = sourceFaultInjectorFor(syscall.ESTALE, "*")
	for _, dir := range []string{"/a", "/b", "/c"} {
		_, err = inj(dir, realRead)
		require.ErrorIs(t, err, syscall.ESTALE, "* must fail every replica")
	}
}

func TestGetBufferedFileRetiresMismatchedReplica(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	data := writeRandomFile(t, dir1, "f", testBlockSize)
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "f"), data, 0o600))

	s := newTestSourceDirs([]string{dir1, dir2}, testBlockSize)
	sf := s.newSourceFile("f", testBlockSize).(*sourceFile)
	defer sf.release()

	f0, err := sf.getBufferedFile(dir1)
	require.NoError(t, err)

	// The mismatched handle must never be served for replica dir2: it is retired
	// and a fresh handle on replica dir2 is opened instead.
	f1, err := sf.getBufferedFile(dir2)
	require.NoError(t, err)
	require.NotSame(t, f0, f1)
	require.Equal(t, dir2, sf.buffered.dir)
	require.Contains(t, sf.staleFiles, f0)

	// A handle matching the requested replica is reused as before.
	f1again, err := sf.getBufferedFile(dir2)
	require.NoError(t, err)
	require.Same(t, f1, f1again)

	buf := make([]byte, testBlockSize)
	n, err := f1.ReadAt(buf, 0)
	require.NoError(t, err, "the new handle must read from replica dir2")
	require.Equal(t, data, buf[:n])
}

func TestRebindSourceIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	writeRandomFile(t, dir, "f", testBlockSize)

	s := newTestSourceDirs([]string{dir}, testBlockSize)
	sf := s.newSourceFile("f", testBlockSize).(*sourceFile)
	defer sf.release()

	buf := make([]byte, testBlockSize)
	for i := 0; i < 3; i++ {
		_, err := sf.ReadAt(buf, 0)
		require.NoError(t, err)
	}
	require.Empty(t, sf.staleFiles,
		"repeated reads on an unchanged replica must not retire and reopen handles")
}

func TestRetireUnless(t *testing.T) {
	dir := t.TempDir()
	writeRandomFile(t, dir, "f", testBlockSize)

	s := newTestSource(dir, testBlockSize, false)
	sf := s.newSourceFile("f", testBlockSize).(*sourceFile)
	defer sf.release()

	open := func() *os.File {
		f, err := os.Open(filepath.Join(dir, "f"))
		require.NoError(t, err)
		return f
	}

	t.Run("nil handle stays nil", func(t *testing.T) {
		sf.Lock()
		require.Nil(t, sf.retireUnless(nil, dir))
		sf.Unlock()
		require.Empty(t, sf.staleFiles)
	})

	t.Run("matching dir keeps handle", func(t *testing.T) {
		f := open()
		defer f.Close()
		h := &boundFile{dir: dir, f: f}
		sf.Lock()
		require.Same(t, h, sf.retireUnless(h, dir))
		sf.Unlock()
		require.Empty(t, sf.staleFiles)
	})

	t.Run("mismatched dir retires handle to stale list", func(t *testing.T) {
		// Both mismatch directions retire, and retirements accumulate.
		f1, f2 := open(), open()
		sf.Lock()
		require.Nil(t, sf.retireUnless(&boundFile{dir: dir, f: f1}, "/other"))
		require.Nil(t, sf.retireUnless(&boundFile{dir: "/other", f: f2}, dir))
		sf.Unlock()
		require.Equal(t, []*os.File{f1, f2}, sf.staleFiles)
	})
}

func TestReadAtFailoverRetiresUntouchedHandleSlot(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	data := writeRandomFile(t, dir1, "f", testBlockSize)
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "f"), data, 0o600))

	s := newTestSourceDirs([]string{dir1, dir2}, testBlockSize)
	sf := s.newSourceFile("f", testBlockSize).(*sourceFile)
	defer sf.release()

	// Park a handle in the O_DIRECT slot, bound to replica dir1. The read below is
	// buffered-only (direct IO is off), so retiring this one proves the
	// failover rebinds eagerly instead of only fixing the slot it happens to use.
	direct0, err := os.Open(filepath.Join(dir1, "f"))
	require.NoError(t, err)
	sf.direct = &boundFile{dir: dir1, f: direct0}
	buffered0, err := sf.getBufferedFile(dir1)
	require.NoError(t, err)

	sourceReadFaultInjector = func(dir string, realRead func() (int, error)) (int, error) {
		if dir == dir1 {
			return 0, syscall.EIO
		}
		return realRead()
	}
	defer func() { sourceReadFaultInjector = nil }()

	buf := make([]byte, testBlockSize)
	n, err := sf.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, testBlockSize, n)
	require.Equal(t, data, buf)
	require.Contains(t, sf.staleFiles, buffered0)
	require.Contains(t, sf.staleFiles, direct0, "the slot this read never touched must be retired too")
	require.Nil(t, sf.direct)
	require.Equal(t, dir2, sf.buffered.dir)
}
