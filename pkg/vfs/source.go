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

package vfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aliyun/cachefs/pkg/srcmgr"
	"github.com/aliyun/cachefs/pkg/utils"
)

// sourceReadFaultInjector, when set, wraps readAtDir: it may replace the IO
// with its own return values or defer to realRead. Test-only hook for driving
// the fallback loop without a faulty filesystem.
var sourceReadFaultInjector func(dir string, realRead func() (int, error)) (int, error)

var sourceFaultErrnos = map[string]syscall.Errno{
	"eio":       syscall.EIO,
	"estale":    syscall.ESTALE,
	"enotconn":  syscall.ENOTCONN,
	"euclean":   syscall.EUCLEAN,
	"eremoteio": syscall.EREMOTEIO,
}

// CFS_SOURCE_READ_FAULT=errno:dir installs sourceReadFaultInjector at startup:
// reads from the replica dir (or "*" for all replicas) fail with errno, which
// must be one of the infrastructure errnos that trigger fallback. Metadata
// operations are not affected. Testing purpose only.
func init() {
	spec := os.Getenv("CFS_SOURCE_READ_FAULT")
	if spec == "" {
		return
	}
	eno, dir, err := parseSourceFaultSpec(spec)
	if err != nil {
		logger.Fatalf("Invalid CFS_SOURCE_READ_FAULT %q: %s", spec, err)
	}
	logger.Warnf("Env \"CFS_SOURCE_READ_FAULT\" is set to %q, will inject source read faults, this is for testing purpose only", spec)
	sourceReadFaultInjector = sourceFaultInjectorFor(eno, dir)
}

// sourceFaultInjectorFor fails reads from the replica dir (or every replica
// when dir is "*") with eno, and delegates all other reads to the real IO.
func sourceFaultInjectorFor(eno syscall.Errno, dir string) func(string, func() (int, error)) (int, error) {
	return func(rdir string, realRead func() (int, error)) (int, error) {
		if dir == "*" || rdir == dir {
			return 0, eno
		}
		return realRead()
	}
}

// parseSourceFaultSpec parses "errno:dir"; dir "*" means all replicas. The
// errno name never contains ":", so a replica path containing one survives
// the split intact.
func parseSourceFaultSpec(spec string) (syscall.Errno, string, error) {
	name, dir, ok := strings.Cut(spec, ":")
	if !ok {
		return 0, "", fmt.Errorf("expect errno:dir, e.g. eio:/mnt/source or estale:*")
	}
	eno, ok := sourceFaultErrnos[strings.ToLower(name)]
	if !ok {
		return 0, "", fmt.Errorf("errno %q not in fallback whitelist", name)
	}
	if dir == "" {
		return 0, "", fmt.Errorf("empty replica dir")
	}
	return eno, dir, nil
}

type sourceStore interface {
	newSourceFile(target string, length uint64) sourceFileStore
}

type sourceFileStore interface {
	acquire()
	release()
	io.ReaderAt
}

type source struct {
	sync.Mutex
	fs    *Cachefs
	sm    *srcmgr.Manager
	files map[string]*sourceFile
	bs    uint32

	// directIO is set once at construction and read-only afterwards: whether
	// O_DIRECT should be used for reading full plaintext blocks from source.
	directIO bool

	// dios permanently disables O_DIRECT per replica once that replica's
	// filesystem rejects it, so future reads on that replica fall back to
	// buffered IO; other replicas are unaffected. Keyed by replica dir.
	dios map[string]*utils.DioGate
}

func newSourceStore(fs *Cachefs, dirs []string, bs uint32) sourceStore {
	s := &source{
		fs:    fs,
		bs:    bs,
		files: make(map[string]*sourceFile),
	}
	// fs.Conf is always set in production; the fallback guards tests that
	// construct a source with a bare &Cachefs{} or a config without a manager.
	if fs.Conf != nil && fs.Conf.Meta != nil && fs.Conf.Meta.SourceManager != nil {
		s.sm = fs.Conf.Meta.SourceManager
	} else {
		s.sm = srcmgr.NewSourceManager(dirs, nil)
	}
	dirs = s.sm.Dirs()
	s.dios = make(map[string]*utils.DioGate, len(dirs))
	for _, dir := range dirs {
		s.dios[dir] = utils.NewDioGate("source reads " + dir)
		sourceDirectIOEnabledPerSource.WithLabelValues(dir).Set(0)
	}
	if fs.Conf != nil && fs.Conf.SourceDirectIO {
		s.directIO = true
		for _, dir := range dirs {
			sourceDirectIOEnabledPerSource.WithLabelValues(dir).Set(1)
		}
		logger.Infof("Source direct IO (O_DIRECT) enabled for source reads from %v", dirs)
	}
	return s
}

// directIOEnabled reports whether O_DIRECT should currently be attempted on
// the replica dir. It is a plain field read plus, only when direct IO is
// configured, a single relaxed atomic load, so it is negligible on the read
// hot path.
func (s *source) directIOEnabled(dir string) bool {
	if !s.directIO {
		return false
	}
	g, ok := s.dios[dir]
	return ok && !g.Disabled()
}

// disableDirectIO permanently falls the replica dir back to buffered IO and
// drops its source_directio_enabled child to 0. The gate logs exactly once
// (lock-free).
func (s *source) disableDirectIO(dir string, cause error) {
	if g, ok := s.dios[dir]; ok {
		g.Disable(cause)
	}
	sourceDirectIOEnabledPerSource.WithLabelValues(dir).Set(0)
}

func (s *source) newSourceFile(target string, length uint64) sourceFileStore {
	s.Lock()
	defer s.Unlock()
	sf, ok := s.files[target]
	if ok {
		sf.doAcquire(true)
		return sf
	}
	sf = &sourceFile{
		target: target,
		length: length,
		bs:     s.bs,
		refs:   1,
		source: s,
	}
	s.files[target] = sf
	return sf
}

// boundFile is an open source file handle tagged with the replica dir it was
// opened on. Cached handles are only reused for reads on the same replica:
// a handle slot may briefly hold a handle opened on the old replica (an open
// racing with rebindSource), and reading it would attribute the dead
// replica's errors to the healthy one and get that replica killed.
type boundFile struct {
	dir string
	f   *os.File
}

type sourceFile struct {
	// only protects the file handles, refs is protected by source
	sync.Mutex
	target string // path relative to the source dir
	length uint64 // source file length
	bs     uint32
	refs   uint32

	direct     *boundFile // opened with O_DIRECT on replica direct.dir
	buffered   *boundFile // opened in buffer IO on replica buffered.dir
	staleFiles []*os.File // handles retired by rebindSource, closed on release
	source     *source
}

func (sf *sourceFile) doAcquire(locked bool) {
	if !locked {
		sf.source.Lock()
		defer sf.source.Unlock()
	}
	sf.refs++
}

func (sf *sourceFile) acquire() {
	sf.doAcquire(false)
}

func (sf *sourceFile) release() {
	sf.source.Lock()
	defer sf.source.Unlock()
	sf.refs--
	if sf.refs == 0 {
		delete(sf.source.files, sf.target)
		sf.Lock()
		if sf.direct != nil {
			if err := sf.direct.f.Close(); err != nil {
				logger.Warnf("Close source direct file: %s", err)
			}
		}
		if sf.buffered != nil {
			if err := sf.buffered.f.Close(); err != nil {
				logger.Warnf("Close source buffered file: %s", err)
			}
		}
		for _, f := range sf.staleFiles {
			if err := f.Close(); err != nil {
				logger.Warnf("Close source stale file: %s", err)
			}
		}
		sf.direct, sf.buffered, sf.staleFiles = nil, nil, nil
		sf.Unlock()
	}
}

// rebindSource retires every handle not opened on replica nextDir; new handles are opened lazily on
// the next read. Retired handles stay open until release so in-flight reads on them complete
// safely. Idempotence comes from each handle's own replica tag, so callers may invoke this
// unconditionally before every read attempt.
//
// Since retired handles are only closed at release, the number of rebinds over a file's lifetime
// must stay bounded. It does today only because failures are permanent, which makes the current
// alive replica (and so nextDir) monotonic; a recovery model that lets it move backwards would
// rebind on most reads and hold fds until the file is released.
func (sf *sourceFile) rebindSource(nextDir string) {
	sf.Lock()
	defer sf.Unlock()
	sf.direct = sf.retireUnless(sf.direct, nextDir)
	sf.buffered = sf.retireUnless(sf.buffered, nextDir)
}

// retireUnless returns h unchanged when it is absent or already bound to nextDir, and otherwise
// moves it to the stale list and returns nil. Callers must hold sf's lock.
func (sf *sourceFile) retireUnless(h *boundFile, nextDir string) *boundFile {
	if h == nil || h.dir == nextDir {
		return h
	}
	sf.staleFiles = append(sf.staleFiles, h.f)
	return nil
}

// verifySize validates the on-disk source file size matches the expected
// length.
func (sf *sourceFile) verifySize(f *os.File) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if sf.length != uint64(st.Size()) {
		return fmt.Errorf("source file %s size changed, expect %v got %v", f.Name(), sf.length, st.Size())
	}
	return nil
}

// getBufferedFile opens & returns the buffered file handle on replica dir.
// The cached handle is reused only when it belongs to dir; a mismatched
// handle (stored by an open that raced with rebindSource) is retired, since
// replica failures are permanent and it can never become wanted again.
func (sf *sourceFile) getBufferedFile(dir string) (*os.File, error) {
	sf.Lock()
	if sf.buffered != nil {
		if sf.buffered.dir == dir {
			f := sf.buffered.f
			sf.Unlock()
			return f, nil
		}
		sf.staleFiles = append(sf.staleFiles, sf.buffered.f)
		sf.buffered = nil
	}
	sf.Unlock()

	nf, err := os.Open(path.Join(dir, sf.target))
	if err != nil {
		return nil, err
	}
	if err := sf.verifySize(nf); err != nil {
		nf.Close()
		return nil, err
	}

	sf.Lock()
	if sf.buffered != nil {
		if sf.buffered.dir == dir {
			f := sf.buffered.f
			sf.Unlock()
			nf.Close()
			return f, nil
		}
		sf.staleFiles = append(sf.staleFiles, sf.buffered.f)
	}
	sf.buffered = &boundFile{dir: dir, f: nf}
	sf.Unlock()
	return nf, nil
}

// getDirectFile opens & returns the O_DIRECT file handle on replica dir, with
// the same replica-identity rules as getBufferedFile. If the replica's
// filesystem does not support O_DIRECT (EINVAL), it disables direct IO for
// that replica and returns the error so the caller can fall back to buffered.
func (sf *sourceFile) getDirectFile(dir string) (*os.File, error) {
	sf.Lock()
	if sf.direct != nil {
		if sf.direct.dir == dir {
			f := sf.direct.f
			sf.Unlock()
			return f, nil
		}
		sf.staleFiles = append(sf.staleFiles, sf.direct.f)
		sf.direct = nil
	}
	sf.Unlock()

	nf, err := utils.OpenDirect(path.Join(dir, sf.target), os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, syscall.EINVAL) {
			sf.source.disableDirectIO(dir, err)
		}
		return nil, err
	}
	if err := sf.verifySize(nf); err != nil {
		nf.Close()
		return nil, err
	}

	sf.Lock()
	if sf.direct != nil {
		if sf.direct.dir == dir {
			f := sf.direct.f
			sf.Unlock()
			nf.Close()
			return f, nil
		}
		sf.staleFiles = append(sf.staleFiles, sf.direct.f)
	}
	sf.direct = &boundFile{dir: dir, f: nf}
	sf.Unlock()
	return nf, nil
}

// Read a block or less to given buffer, off must be block-aligned
func (sf *sourceFile) ReadAt(buf []byte, off int64) (int, error) {
	if off%int64(sf.bs) != 0 {
		logger.Fatalf("BUG: Read source offset not block size aligned: off %d block size %d", off, sf.bs)
	}
	if len(buf) > int(sf.bs) {
		logger.Fatalf("BUG: Read source len larger than block size: len %d block size %d", len(buf), sf.bs)
	}

	// An empty replica list must not reach RunOp: it degenerates to op(""),
	// which would open sf.target relative to the working directory.
	if sf.source.sm.Len() == 0 {
		return 0, errors.New("no source dir available")
	}
	var n int
	err := sf.source.sm.RunOp("read", sf.target, func(dir string) error {
		sf.rebindSource(dir)
		start := time.Now()
		var err error
		n, err = sf.readAtDir(dir, buf, off)
		if err != nil {
			// Every failed attempt is counted on the replica that produced it,
			// including failures rescued by a lower-priority replica.
			sourceReadErrorsPerSource.WithLabelValues(dir).Inc()
			return err
		}
		sourceReadsPerSource.WithLabelValues(dir).Inc()
		sourceReadBytesPerSource.WithLabelValues(dir).Add(float64(n))
		sourceReadHistPerSource.WithLabelValues(dir).Observe(time.Since(start).Seconds())
		return nil
	})
	return n, err
}

// readAtDir reads a full or partial block from replica dir.
func (sf *sourceFile) readAtDir(dir string, buf []byte, off int64) (int, error) {
	if sourceReadFaultInjector != nil {
		return sourceReadFaultInjector(dir, func() (int, error) {
			return sf.doReadAtDir(dir, buf, off)
		})
	}
	return sf.doReadAtDir(dir, buf, off)
}

// doReadAtDir performs the actual read from replica dir. O_DIRECT
// rejections (EINVAL) are handled in place by permanently downgrading that
// replica to buffered IO; all other errors propagate to the caller for
// infrastructure-error classification.
func (sf *sourceFile) doReadAtDir(dir string, buf []byte, off int64) (int, error) {
	// Plaintext: a full block (len == block size) has an aligned offset and length
	// and can use O_DIRECT when the buffer is also aligned; a partial block (the last
	// block or a small file) is read with buffered IO directly into buf, no copy.
	if sf.source.directIOEnabled(dir) && len(buf) == int(sf.bs) && utils.BufAligned(buf) {
		f, err := sf.getDirectFile(dir)
		if err == nil {
			n, rerr := f.ReadAt(buf, off)
			if rerr == nil {
				return n, nil
			}
			if !errors.Is(rerr, syscall.EINVAL) {
				// A real read error, not an alignment rejection; surface it.
				return n, rerr
			}
			// O_DIRECT open succeeded but the read was rejected (e.g. device
			// logical block size larger than our 4096 alignment). This is
			// structural for this replica, so permanently disable and fall
			// through to buffered.
			sf.source.disableDirectIO(dir, rerr)
		} else if !errors.Is(err, syscall.EINVAL) {
			// A real open error (not "unsupported"); surface it rather than masking.
			return 0, err
		}
	}

	// O_DIRECT unsupported at open or read time, fall through to buffered.
	f, err := sf.getBufferedFile(dir)
	if err != nil {
		return 0, err
	}
	return f.ReadAt(buf, off)
}
