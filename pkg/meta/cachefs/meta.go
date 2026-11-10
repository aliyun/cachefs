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

package cachefsmeta

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aliyun/cachefs/pkg/meta"
	"github.com/aliyun/cachefs/pkg/metric"
	pb "github.com/aliyun/cachefs/pkg/rpc/meta"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	ss "github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/srcmgr"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/eryugey/serf/serf"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var logger = utils.GetLogger("cachefs")

const (
	CmdPreload      = 1024
	CmdClear        = 1025
	CmdSourceReady  = 1026
	CmdMetaReady    = 1027
	inodeShardCount = 64
)

type inodeShard struct {
	sync.Mutex
	inodes map[meta.Ino]*Inode
}

func newInodeShards() []inodeShard {
	shards := make([]inodeShard, inodeShardCount)
	for i := range shards {
		shards[i].inodes = make(map[meta.Ino]*Inode)
	}
	return shards
}

// Fault injection regarding to metadata operation timeout, used in tests
var (
	injectTimeoutFault bool = os.Getenv("CFS_READDIR_FAULT") == "true"
	injectRandomFault  bool = os.Getenv("CFS_READDIR_FAULT") == "random"
	injectReaddirFault bool = injectTimeoutFault || injectRandomFault
)

type MetaData interface {
	metric.MetricsInitializer
	Init() error
	GetInode(meta.Ino) (*Inode, syscall.Errno)
	GetRoot() *Inode // Return the root inode for in-memory tree walk
	GetPath(path string) string
	SourceReady(bool) syscall.Errno // Indicate source dir is ready in sourceless mode
	MetaReady() syscall.Errno       // Indicate source dir meta is fully cached and can be used as meta source
	IsMetaReady() bool              // Report whether in-memory metadata is fully loaded
	Stop()

	Lookup(meta.Context, meta.Ino, string, *meta.Ino, *meta.Attr) syscall.Errno
	GetAttr(meta.Context, meta.Ino, *meta.Attr) syscall.Errno
	ReadLink(meta.Context, meta.Ino, *[]byte) syscall.Errno
	Open(meta.Context, meta.Ino, uint32, *meta.Attr) syscall.Errno
	Opendir(meta.Context, meta.Ino, *meta.Attr) syscall.Errno
	Close(meta.Context, meta.Ino) syscall.Errno
	StatFS(meta.Context, *uint64, *uint64, *uint64, *uint64) syscall.Errno
	Readdir(meta.Context, meta.Ino, *[]*meta.Entry) syscall.Errno
}

// Source subpath config
type SourceSubPathConfig struct {
	Paths  string // user-specified subpaths within source dir, separated by space
	File   string // file path that contains source subpaths, overrides `Paths`
	Cmd    string // command to run to get source subpaths, overrides `File` and `Paths`
	Reload int    // Seconds between reloading subpaths from `Cmd` or `File`
}

// Config for cachefs metadata
type MetaConfig struct {
	Name            string // cachefs mount name
	SourceDirs      []string
	Sourceless      bool
	PeerMetaTimeout int    // enable load filesystem meta from peer with timeout in second
	PeerMetaMode    string // "auto" or "manual"
	SubPathConfig   SourceSubPathConfig
	MountPoint      string
	CacheSize       uint64
	AttrCleanup     bool // clean up expired inode attributes
	ReaddirTimeout  int  // timeout second, -1 means no timeout
	MetaOpTimeout   int  // metadata operation timeout, 0 means no timeout
	FuseCache       bool // use fuse page cache or not

	// Callbacks on subpath change, trigger cache reload
	ReloadCache func() `json:"-"`
	// evict cache associated with removed subpaths
	RemoveCache func(remove, keep []string) (int64, int64) `json:"-"`
	// SourceManager tracks permanent failure state of SourceDirs replicas,
	// shared with the vfs data path; nil in tests that don't exercise fallback.
	SourceManager *srcmgr.Manager `json:"-"`
}

// MetaProvider scans & caches filesystem meta data from the source dir on notification, and
// broadcasts meta info to MetaConsumer; the source dir can be empty on start up. Sourceless
// mode is validated to a single source dir, so this never involves multiple replicas.
func (mc *MetaConfig) MetaProvider() bool {
	return len(mc.SourceDirs) > 0 && mc.Sourceless
}

// MetaConsumer receives filesystem meta data from MetaProvider or other MetaConsumer that has
// received full meta data, and only finish mount after receiving meta data.
func (mc *MetaConfig) MetaConsumer() bool {
	return len(mc.SourceDirs) == 0 && mc.Sourceless
}

// Peer meta mode is enabled if PeerMetaTimeout is greater than 0, and we'll try loading
// filesystem meta data from other peers first within the given timeout, and fallback to
// load meta from source dir on timeout.
func (mc *MetaConfig) PeerMeta() bool {
	return mc.PeerMetaTimeout > 0
}

// PeerMetaAutoMode reports whether meta ready should be set automatically after mount. Returns true
// when peer meta is enabled and PeerMetaMode is "auto".
func (mc *MetaConfig) PeerMetaAutoMode() bool {
	return mc.PeerMeta() && (mc.PeerMetaMode == "" || mc.PeerMetaMode == "auto")
}

// Source dir subpath is enabled if any of `Paths`, `File`, or `Cmd` is set.
func (mc *MetaConfig) SubpathEnabled() bool {
	sbc := mc.SubPathConfig
	return len(sbc.Paths) > 0 || len(sbc.File) > 0 || len(sbc.Cmd) > 0
}

// inodeAttr is an immutable cached attribute snapshot.
type inodeAttr struct {
	attr     meta.Attr
	loadedAt time.Time
}

type Inode struct {
	inode  meta.Ino
	parent *Inode
	typ    uint8

	// Basename within the parent directory. GetPath joins the parent chain to
	// build the source-relative path, which locates the source file under a
	// source dir replica and is also the base of the cache key.
	// For example:
	// source path: $SOURCE_DIR/path/to/file
	// name: file, with parents "to" and "path" under the root
	name string

	// only used for dump/load hardlink entries
	altKey string

	attrCache atomic.Pointer[inodeAttr]
	children  map[string]*Inode // name -> child inode; nil for non-directories
	childLock sync.Mutex
	readdir   bool
	readAt    time.Time
}

type metaData struct {
	config *MetaConfig

	// inode management
	inodeShards []inodeShard
	inoAltMap   sync.Map // alternative "dev:ino" -> *Inode
	nextIno     atomic.Uint64
	cachedAttrs atomic.Int64
	root        *Inode
	stopCh      chan struct{}

	// used by peer meta mode or sourceless mode
	metaLock      sync.Mutex
	metaSource    map[string]struct{}
	metaSourceCh  chan struct{}
	metaReady     atomic.Bool
	sourcePrepare atomic.Bool
	metaProvider  string
	serfUser      *ss.SerfUser
	serf          ss.SerfService
	serfGroup     string
	serfReadyCh   chan struct{}
	localAddr     string
	rpc           rpcsvc.RpcService
	clientMu      sync.Mutex
	clients       map[string]*metaClient
	totalSize     uint64
	totalInode    uint64
	dumpedMeta    atomic.Pointer[dumpedCachefs]

	// Only subpaths in `sourceSubPath` are accessible if specified
	sourceSubPath   map[string]struct{}
	sourceSubPathMu sync.RWMutex

	// go-fuse doesn't support cache symlinks, so cache in fuse server,
	// the assumption is there aren't many symlinks
	symLock  sync.Mutex
	symlinks map[meta.Ino][]byte

	// metrics
	opDist        prometheus.Histogram
	loadMetaCount prometheus.Counter
}

func NewMetaData(conf *MetaConfig, rpc rpcsvc.RpcService, serfSvc ss.SerfService, reg prometheus.Registerer) MetaData {
	opTimeout := time.Duration(conf.MetaOpTimeout) * time.Second
	if conf.MetaOpTimeout != 0 {
		logger.Infof("Filesystem metadata operation on source dir timeout time %s, will return cached meta on timeout and error", opTimeout)
	}
	if injectReaddirFault {
		logger.Warnf("Env \"CFS_READDIR_FAULT\" is set to \"%s\", will inject readdir timeout and/or error, this is for testing purpose only",
			os.Getenv("CFS_READDIR_FAULT"))
	}

	if len(conf.SourceDirs) == 0 && !conf.Sourceless {
		logger.Fatal("Missing source dir in non-sourceless mode")
	}

	subPaths, cmdOut, cmdDur, err := genSourceSubpaths(&conf.SubPathConfig)
	if err != nil {
		// error detail already logged in genSourceSubpaths/ReadPathsFromCmd
		logger.Fatal("Generate source subpath list failed, aborting")
	}
	// cmdOut is non-empty only in Cmd mode
	if cmdOut != "" {
		logger.Infof("Source subpath command %q succeeded in %s, output: %s", conf.SubPathConfig.Cmd, cmdDur, cmdOut)
	}
	m := &metaData{
		config:        conf,
		inodeShards:   newInodeShards(),
		symlinks:      make(map[meta.Ino][]byte),
		serf:          serfSvc,
		rpc:           rpc,
		sourceSubPath: subPaths,
		metaSource:    make(map[string]struct{}),
		metaReady:     atomic.Bool{},
		sourcePrepare: atomic.Bool{},
		metaSourceCh:  make(chan struct{}, 1),
		serfReadyCh:   make(chan struct{}),
		stopCh:        make(chan struct{}),
		clients:       make(map[string]*metaClient),
	}
	m.nextIno.Store(uint64(meta.RootInode))
	m.startAttrSweeper()
	m.InitMetrics(reg)
	go m.reloadSubpathFile()

	// Serf service is only used in sourceless or peer meta mode
	if !conf.Sourceless && !conf.PeerMeta() {
		return m
	}

	// Sourceless or peer meta require serf and rpc services
	if m.serf == nil || m.rpc == nil {
		if conf.Sourceless {
			logger.Fatal("Sourceless mode requires serf and rpc services")
		} else {
			logger.Warn("Peer meta mode requires serf and rpc services, skip peer meta.")
			return m
		}
	}
	m.serfGroup = m.serf.Group()
	m.localAddr = m.rpc.Addr()

	// Register LoadMeta server to rpc service
	pb.RegisterCachefsMetaServer(m.rpc.GetServer(), m)

	// Register and setup serf service for sourceless mode
	m.serfUser = &ss.SerfUser{
		Name:               "meta",
		UserEvents:         []string{announceEvent, dumpEvent},
		UserEventCoalescer: newUserEventCoalescer(m.localAddr, m.serf),
		Filter:             newMetaEventFilter(m.serf),
		MemberLeaveCh:      make(chan []ss.SerfMember, 128),
		UserEventCh:        make(chan serf.UserEvent, 128),
		ReadyCh:            make(chan struct{}),
		JoinedCh:           make(chan struct{}, 16),
	}
	if conf.MetaProvider() {
		// We're meta and data provider, can be used as seeder
		m.serfUser.Role = ss.SerfRoleSeeder
		logger.Info("Run in sourceless meta provider mode")
	} else if conf.MetaConsumer() {
		logger.Info("Run in sourceless meta consumer mode")
	} else if conf.PeerMeta() {
		logger.Info("Run with source dir peer meta enabled")
	}
	if err := m.serf.RegisterUser(m.serfUser); err != nil {
		logger.Fatalf("Register serf user: %s", err)
	}

	go m.handleSerfUserEvent()
	go m.requestMetaSource()
	go m.handleSerfMemberLeaveEvent()
	go func() {
		m.serfUser.WaitReady()
		close(m.serfReadyCh)
	}()
	go m.gcClients()
	return m
}

func NewInode(ino meta.Ino, parent *Inode, name string, typ uint8, attr *meta.Attr) *Inode {
	cino := &Inode{
		inode:  ino,
		parent: parent,
		name:   name,
		typ:    typ,
	}
	if cino.typ == meta.TypeDirectory {
		cino.children = make(map[string]*Inode)
	}
	if attr != nil {
		cino.attrCache.Store(&inodeAttr{attr: *attr, loadedAt: time.Now()})
	}
	return cino
}

func (m *metaData) fileInfoToAttr(fi os.FileInfo, attr *meta.Attr) string {
	attr.Typ = meta.FileInfoToType(fi)
	attr.Mode = uint16(fi.Mode().Perm())
	attr.Length = uint64(fi.Size())

	// Set default time stamp
	modTime := fi.ModTime()
	attr.Atime = modTime.Unix()
	attr.Mtime = modTime.Unix()
	attr.Ctime = modTime.Unix()
	attr.Atimensec = uint32(modTime.UnixNano())
	attr.Mtimensec = uint32(modTime.UnixNano())
	attr.Ctimensec = uint32(modTime.UnixNano())

	// read only cachefs could keep cache
	attr.KeepCache = m.config.FuseCache
	attr.Full = true

	st, ok := fi.Sys().(*syscall.Stat_t)
	if ok {
		attr.Uid = st.Uid
		attr.Gid = st.Gid
		attr.Rdev = uint32(st.Rdev)
		atime, atimensec := st.Atim.Unix()
		mtime, mtimensec := st.Mtim.Unix()
		ctime, ctimensec := st.Ctim.Unix()
		attr.Atime = atime
		attr.Atimensec = uint32(atimensec)
		attr.Mtime = mtime
		attr.Mtimensec = uint32(mtimensec)
		attr.Ctime = ctime
		attr.Ctimensec = uint32(ctimensec)
		attr.Nlink = uint32(st.Nlink)

		return fmt.Sprintf("%v:%v", st.Dev, st.Ino)
	} else {
		logger.Warnf("stat of file \"%s\" cannot be converted to syscall.Stat_t, use \"0:0\" as dev and inode", fi.Name())
		return "0:0"
	}
}

func (m *metaData) isAllowedSubPath(onDiskPath string) bool {
	m.sourceSubPathMu.RLock()
	defer m.sourceSubPathMu.RUnlock()
	if len(m.sourceSubPath) == 0 {
		return true
	}
	for dir := range m.sourceSubPath {
		subdir := dir + string(os.PathSeparator)
		check := onDiskPath + string(os.PathSeparator)
		// e.g. Check onDiskPath "/dira/dirb", and subdir "/dira"
		if strings.HasPrefix(check, subdir) {
			return true
		}
		// e.g. Check onDiskPath "/dira/dirb", and subdir "/dira/dirb/dirc"
		if strings.HasPrefix(subdir, check) {
			return true
		}
	}
	return false
}

func (m *metaData) Init() error {
	// no source dir in sourceless meta consumer mode, wait for meta provider and load from it
	if m.config.MetaConsumer() {
		return m.waitAndLoadMeta()
	}

	// Try load filesystem meta from other peers, and fallback to normal init process if load
	// meta failed.
	if m.config.PeerMeta() {
		err := m.waitAndLoadMeta()
		if err == nil {
			return nil
		}
		logger.Infof("Load meta from peer failed: %s, fallback to normal fuse init process", err)
	}

	var fi os.FileInfo
	if eno := m.runSourceOp("init", "/", func(dir string) syscall.Errno {
		var err error
		fi, err = os.Stat(filepath.Join(dir, "/"))
		return PathErrno(err)
	}); eno != 0 {
		return fmt.Errorf("stat source dirs %v: %s", m.config.SourceDirs, eno)
	}
	if !fi.IsDir() {
		return fmt.Errorf("source dirs %v is not a directory", m.config.SourceDirs)
	}
	attr := &meta.Attr{Parent: meta.RootInode}
	_ = m.fileInfoToAttr(fi, attr)

	// add root inode to inode map
	inode := meta.Ino(m.nextIno.Add(1) - 1)
	rootIno := NewInode(inode, nil, "/", attr.Typ, attr)
	shard := m.inodeShard(inode)
	shard.Lock()
	shard.inodes[inode] = rootIno
	shard.Unlock()
	m.cachedAttrs.Add(1)
	m.root = rootIno

	return nil
}

func (m *metaData) InitMetrics(reg prometheus.Registerer) {
	m.loadMetaCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "meta_loadmeta_count",
		Help: "load meta request count",
	})
	m.opDist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "meta_ops_durations_histogram_seconds",
		Help:    "Operation latency distributions.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 1.5, 30),
	})
	if reg == nil {
		return
	}

	reg.MustRegister(m.loadMetaCount)
	reg.MustRegister(m.opDist)
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "meta_cached_inodes",
		Help: "Current number of inodes cached in memory.",
	}, func() float64 {
		return float64(m.inodeCount())
	}))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "meta_cached_attrs",
		Help: "Current number of inode attributes cached in memory.",
	}, func() float64 {
		return float64(m.cachedAttrs.Load())
	}))
}

func (m *metaData) GetInode(inode meta.Ino) (*Inode, syscall.Errno) {
	shard := m.inodeShard(inode)
	shard.Lock()
	cino, ok := shard.inodes[inode]
	shard.Unlock()
	if !ok {
		return nil, syscall.EBADF
	}
	return cino, 0
}

// Used to set source ready in sourceless meta provider mode. Should be called twice, the first call
// with `prepare` to be true to check and prepare source and cache size, the second call with
// `prepare` to be false to announce source ready actually.
func (m *metaData) SourceReady(prepare bool) syscall.Errno {
	if prepare {
		// prepare check conditions
		if !m.config.MetaProvider() {
			logger.Warn("Set sourceless ready on non-metaprovider mode")
			return syscall.ENOTSUP
		}
		m.sourcePrepare.Store(true)
		if err := m.cacheMeta(); err != 0 {
			return err
		}

		if m.totalSize > m.config.CacheSize {
			cs := m.config.CacheSize
			logger.Warnf("SourceReady: require at least %dMiB(%d) cache size, have %dMiB(%d)",
				m.totalSize>>20, m.totalSize, cs>>20, cs)
			return syscall.ENOSPC
		}
		return 0
	} else {
		// wait serf service to be ready for 1s
		t := time.NewTimer(time.Second)
		select {
		case <-t.C:
			logger.Warn("Waiting for serf service timed out, send meta ready anyway")
		case <-m.serfReadyCh:
		}
		m.metaReady.Store(true)
		m.sourcePrepare.Store(false)
		m.announceMetaSource()
		return 0
	}
}

func (m *metaData) cacheMeta() syscall.Errno {
	// Walk whole filesystem tree and cache meta first
	err := m.walkDir(meta.RootInode)
	if err != 0 {
		return err
	}

	// Then cache all symlinks
	m.cacheSymlinks()

	// Update total size after walking filesystem tree
	m.updateTotalSize()

	// Update total inode number
	m.totalInode = m.inodeCount()
	if m.totalInode <= 1 {
		// Empty dir that only contains root inode? Don't cache such dir.
		return syscall.ENODATA
	}
	return 0
}

// Used to notify that source dir metadata is fully cached and ready in peer meta mode.
func (m *metaData) MetaReady() syscall.Errno {
	if !m.config.PeerMeta() {
		logger.Warn("Set meta ready not supported as peer meta not enabled")
		return syscall.ENOTSUP
	}

	// Go through and cache filesystem meta, then set metaReady
	start := time.Now()
	if err := m.cacheMeta(); err != 0 {
		return err
	}
	m.metaReady.Store(true)
	m.announceMetaSource()
	logger.Infof("Set meta ready in %s", time.Since(start))
	return 0
}

// Similar to Inode.GetPath(), but use path within source dir directly.
func (m *metaData) GetPath(p string) string {
	return filepath.Join("/", p)
}

// GetRoot returns the root Inode for in-memory tree traversal.
func (m *metaData) GetRoot() *Inode {
	return m.root
}

// IsMetaReady reports whether the in-memory metadata has been fully loaded,
// either from peers (peer meta mode) or from the source dir (sourceless mode).
func (m *metaData) IsMetaReady() bool {
	return m.metaReady.Load()
}

func (m *metaData) Stop() {
	if m.stopCh != nil {
		close(m.stopCh)
		m.stopCh = nil
	}
}

func (m *metaData) Lookup(ctx meta.Context, parent meta.Ino, name string, inode *meta.Ino, attr *meta.Attr) syscall.Errno {
	if inode == nil || attr == nil {
		return syscall.EINVAL // bad request
	}
	defer m.timeit(time.Now())
	if name == ".." {
		if parent == meta.RootInode {
			name = "."
		} else {
			if st := m.GetAttr(ctx, parent, attr); st != 0 {
				return st
			}
			if attr.Typ != meta.TypeDirectory {
				return syscall.ENOTDIR
			}
			*inode = attr.Parent
			return m.GetAttr(ctx, *inode, attr)
		}
	}
	if name == "." {
		if st := m.GetAttr(ctx, parent, attr); st != 0 {
			return st
		}
		*inode = parent
		return 0
	}

	// If we're in sourceless meta provider mode, and source is not ready yet, return nothing.
	// But allow requests from ourselves.
	if m.config.MetaProvider() && !m.metaReady.Load() && !m.sourcePrepare.Load() {
		return syscall.ENOENT
	}

	return m.doLookup(ctx, parent, name, inode, attr)
}

func (m *metaData) GetAttr(ctx meta.Context, inode meta.Ino, attr *meta.Attr) syscall.Errno {
	defer m.timeit(time.Now())
	// all inodes should have been looked up before and cached in inode map
	cino, eno := m.GetInode(inode)
	if eno != 0 {
		return eno
	}
	cached, eno := m.ensureAttr(ctx, cino)
	if eno != 0 {
		return eno
	}
	*attr = *cached
	return 0
}

func (m *metaData) ReadLink(ctx meta.Context, inode meta.Ino, path *[]byte) syscall.Errno {
	m.symLock.Lock()
	defer m.symLock.Unlock()
	if target, ok := m.symlinks[inode]; ok {
		*path = target
		return 0
	}
	defer m.timeit(time.Now())
	m.symLock.Unlock()
	target, err := m.doReadlink(ctx, inode)
	m.symLock.Lock()
	if err != 0 {
		return err
	}
	if len(target) == 0 {
		return syscall.ENOENT
	}
	*path = target
	m.symlinks[inode] = target
	return 0
}

func (m *metaData) Open(ctx meta.Context, inode meta.Ino, flags uint32, attr *meta.Attr) syscall.Errno {
	defer m.timeit(time.Now())
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_TRUNC|syscall.O_APPEND) != 0 {
		return syscall.EROFS
	}
	return m.GetAttr(ctx, inode, attr)
}

func (m *metaData) Opendir(ctx meta.Context, inode meta.Ino, attr *meta.Attr) syscall.Errno {
	defer m.timeit(time.Now())
	if err := m.GetAttr(ctx, inode, attr); err != 0 {
		return err
	}
	if attr.Typ != meta.TypeDirectory {
		return syscall.ENOTDIR
	}
	return 0
}

func (m *metaData) Close(ctx meta.Context, inode meta.Ino) syscall.Errno {
	return 0
}

func (m *metaData) StatFS(ctx meta.Context, totalspace, availspace, itotal, iavail *uint64) syscall.Errno {
	if m.config.MetaConsumer() || m.metaReady.Load() {
		*totalspace = m.totalSize
		*availspace = 0
		*itotal = m.totalInode
		*iavail = 0
		return 0
	}

	defer m.timeit(time.Now())
	var stat syscall.Statfs_t
	if eno := m.runSourceOp("statfs", "/", func(dir string) syscall.Errno {
		// syscall.Statfs returns a bare syscall.Errno; PathErrno's fallback
		// would turn semantic errors like ENOENT into EIO and wrongly fail
		// the replica over.
		err := syscall.Statfs(filepath.Join(dir, "/"), &stat)
		if eno, ok := err.(syscall.Errno); ok {
			return eno
		}
		return PathErrno(err)
	}); eno != 0 {
		return eno
	}
	*totalspace = stat.Blocks * uint64(stat.Bsize)
	*availspace = stat.Bavail * uint64(stat.Bsize)
	*itotal = stat.Files
	*iavail = stat.Ffree
	return 0
}

func (m *metaData) Readdir(ctx meta.Context, inode meta.Ino, entries *[]*meta.Entry) (rerr syscall.Errno) {
	cino, err := m.GetInode(inode)
	if err != 0 {
		return err
	}
	if cino.typ != meta.TypeDirectory {
		return syscall.ENOTDIR
	}
	dirAttr, eno := m.ensureAttr(ctx, cino)
	if eno != 0 {
		return eno
	}

	defer m.timeit(time.Now())
	*entries = []*meta.Entry{
		{
			Inode: inode,
			Name:  []byte("."),
			Attr:  dirAttr,
		},
	}
	*entries = append(*entries, &meta.Entry{
		Inode: dirAttr.Parent,
		Name:  []byte(".."),
		Attr:  &meta.Attr{Typ: meta.TypeDirectory},
	})

	// If we're in sourceless meta provider mode, and source is not ready yet, return nothing.
	// But allow requests if we're in source ready prepare phase.
	if m.config.MetaProvider() && !m.metaReady.Load() && !m.sourcePrepare.Load() {
		return 0
	}

	type childEntry struct {
		name  string
		child *Inode
	}
	fillEntries := func() {
		cino.childLock.Lock()
		children := make([]childEntry, 0, len(cino.children))
		for name, child := range cino.children {
			children = append(children, childEntry{name: name, child: child})
		}
		cino.childLock.Unlock()
		for _, item := range children {
			attr, eno := m.ensureAttr(ctx, item.child)
			if eno != 0 {
				logger.Warnf("Readdir: load attr for %s failed: %s, skip entry", item.child.GetPath(), eno)
				continue
			}
			*entries = append(*entries, &meta.Entry{
				Inode: item.child.inode,
				Name:  []byte(item.name),
				Attr:  attr,
			})
		}
	}

	// Readdir from source dir when:
	// - child entries not cached
	// - child entries cached, but cache expired and we have no meta op timeout
	now := time.Now()
	timeout := m.config.ReaddirTimeout
	expired := (timeout >= 0) && (now.Sub(cino.readAt) > time.Duration(timeout)*time.Second)
	if !cino.readdir || (expired && m.config.MetaOpTimeout <= 0) {
		if err := m.doReaddir(ctx, inode); err != 0 {
			return err
		}
		cino.readdir = true
		cino.readAt = now
		fillEntries()
		return 0
	}

	// Otherwise just return cached readdir results if cache is still valid
	if cino.readdir && !expired {
		fillEntries()
		return 0
	}

	// Now we have expired readdir cache, and have meta operation timeout set, just return the
	// cached result, and do readdir in background, in case source dir is not responding or
	// returning error.
	go func() {
		errnoChan := make(chan syscall.Errno, 1)
		opTimeout := time.Duration(m.config.MetaOpTimeout) * time.Second
		timer := time.NewTimer(opTimeout)
		// Log the relative path only: doReaddir may fail over between
		// replicas, so any replica prefix here could name one that never
		// served the op. Replica-accurate records are in doReaddir/kill logs.
		sourcePath := cino.GetPath()
		go func() {
			if !injectReaddirFault {
				// Normal path, no fault injection, just do readdir
				errnoChan <- m.doReaddir(ctx, inode)
			} else {
				timeoutFault := func() {
					logger.Infof("Inject readdir timeout on %s", sourcePath)
					time.Sleep(opTimeout + time.Second)
					errnoChan <- m.doReaddir(ctx, inode)
				}
				enoentFault := func() {
					logger.Infof("Inject readdir error on %s", sourcePath)
					errnoChan <- syscall.Errno(2)
				}
				noFault := func() {
					logger.Infof("Inject no readdir fault on %s", sourcePath)
					errnoChan <- m.doReaddir(ctx, inode)
				}
				if injectRandomFault {
					// Inject readdir error, either timeout or error
					switch rand.Intn(3) {
					case 0:
						timeoutFault()
					case 1:
						enoentFault()
					case 2:
						fallthrough
					default:
						noFault()
					}
				} else {
					if injectTimeoutFault {
						timeoutFault()
						injectTimeoutFault = false
					} else {
						enoentFault()
						injectTimeoutFault = true
					}
				}
			}
		}()
		select {
		case <-timer.C:
			logger.Warnf("readdir %s timeout", sourcePath)
		case eno := <-errnoChan:
			if eno == 0 {
				cino.readAt = now
			} else {
				logger.Warnf("readdir %s failed: %s", sourcePath, eno)
			}
		}
	}()
	fillEntries()
	return 0
}

// cachefs meta private methods

// runSourceOp runs op against the highest-priority live source dir
// replica and fails over to the next replica on infrastructure errors
// (srcmgr.IsInfraError), via srcmgr.Manager.RunOp. Any other errno, ENOENT
// included, is returned as-is. name and target describe the operation for
// the kill log. A nil SourceManager (tests that don't wire one) degenerates
// to a single attempt on the first configured source dir. op is never
// invoked with an empty dir: with no source dirs configured the op fails
// with ENOENT without running.
func (m *metaData) runSourceOp(name, target string, op func(dir string) syscall.Errno) syscall.Errno {
	if m.config.SourceManager == nil {
		if len(m.config.SourceDirs) == 0 {
			return syscall.ENOENT
		}
		return op(m.config.SourceDirs[0])
	}
	if m.config.SourceManager.Len() == 0 {
		return syscall.ENOENT
	}
	err := m.config.SourceManager.RunOp(name, target, func(dir string) error {
		if eno := op(dir); eno != 0 {
			return eno
		}
		return nil
	})
	if err == nil {
		return 0
	}
	// The only errors flowing through RunOp are produced by op above, but
	// don't panic if that ever changes.
	var eno syscall.Errno
	if errors.As(err, &eno) {
		return eno
	}
	logger.Warnf("runSourceOp %s on %s: unexpected error type %T, contract broken, returning EIO: %s", name, target, err, err)
	return syscall.EIO
}

func (m *metaData) doLookup(ctx meta.Context, parent meta.Ino, name string, inode *meta.Ino, attr *meta.Attr) syscall.Errno {
	var lattr = meta.Attr{}
	var savedIno *Inode
	pino, eno := m.GetInode(parent)
	if eno != 0 {
		return eno
	}
	if pino.typ != meta.TypeDirectory {
		return syscall.ENOTDIR
	}

	// lookup children
	pino.childLock.Lock()
	child, ok := pino.children[name]
	pino.childLock.Unlock()
	if ok {
		if cached := m.cachedAttr(child); cached != nil {
			*inode = child.inode
			*attr = *cached
			return 0
		}
		savedIno = child
	}

	// not in children map, look it up from source
	childPath := filepath.Join(pino.GetPath(), name)
	if !m.isAllowedSubPath(childPath) {
		return syscall.ENOENT
	}

	// lookup from source dir, don't follow symlink, which will be resolved by `ReadLink()`
	var fi os.FileInfo
	if eno := m.runSourceOp("lookup", childPath, func(dir string) syscall.Errno {
		srcPath := filepath.Join(dir, childPath)
		logger.Debugf("Lookup: parent %v, name %s => %s", parent, name, srcPath)
		var err error
		fi, err = os.Lstat(srcPath)
		return PathErrno(err)
	}); eno != 0 {
		return eno
	}

	// fill attr
	altKey := m.fileInfoToAttr(fi, &lattr)
	lattr.Parent = parent

	if lattr.Typ != meta.TypeDirectory && lattr.Nlink > 1 {
		// hardlink files, lookup alternative inode map
		value, ok := m.inoAltMap.Load(altKey)
		if ok {
			cino := value.(*Inode)
			if savedIno == nil || cino.inode != savedIno.inode {
				if cached := m.cachedAttr(cino); cached != nil {
					*inode = cino.inode
					*attr = *cached
					pino.childLock.Lock()
					pino.children[name] = cino
					pino.childLock.Unlock()
					return 0
				}
				savedIno = cino
			}
		}
	}

	// other thread may add the same path, check it again
	pino.childLock.Lock()
	defer pino.childLock.Unlock()
	child, ok = pino.children[name]
	if ok && (savedIno == nil || child.inode != savedIno.inode) {
		if cached := m.cachedAttr(child); cached != nil {
			*inode = child.inode
			*attr = *cached
			return 0
		}
		savedIno = child
	}

	// not found in inode map, add it
	var newInode *Inode
	created := savedIno == nil
	if created {
		newIno := meta.Ino(m.nextIno.Add(1) - 1)

		// Delay attr publication until hardlink selection so sweeping cannot recycle an
		// uncounted candidate.
		newInode = NewInode(newIno, pino, name, lattr.Typ, nil)
		shard := m.inodeShard(newIno)
		shard.Lock()
		shard.inodes[newIno] = newInode
		shard.Unlock()
	} else {
		newInode = savedIno
	}
	candidate := newInode
	if lattr.Typ != meta.TypeDirectory && lattr.Nlink > 1 {
		candidate.altKey = altKey
		actual, loaded := m.inoAltMap.LoadOrStore(altKey, candidate)
		if loaded && actual.(*Inode) != candidate {
			if created {
				shard := m.inodeShard(candidate.inode)
				shard.Lock()
				delete(shard.inodes, candidate.inode)
				shard.Unlock()
			}
			newInode = actual.(*Inode)
		}
	}
	storedAttr := m.storeAttr(newInode, &lattr)
	pino.children[name] = newInode

	*inode = newInode.inode
	*attr = *storedAttr
	return 0
}

func (m *metaData) doReadlink(ctx meta.Context, inode meta.Ino) ([]byte, syscall.Errno) {
	cino, eno := m.GetInode(inode)
	if eno != 0 {
		return nil, eno
	}
	if cino.typ != meta.TypeSymlink {
		return nil, syscall.EINVAL
	}

	path := cino.GetPath()

	var target string
	if eno := m.runSourceOp("readlink", path, func(dir string) syscall.Errno {
		srcPath := filepath.Join(dir, path)
		t, err := os.Readlink(srcPath)
		if err != nil {
			eno := PathErrno(err)
			logger.Warnf("doReadlink: Readlink %s failed: %v (errno=%d)", srcPath, err, eno)
			return eno
		}
		target = t
		return 0
	}); eno != 0 {
		return nil, eno
	}

	return []byte(target), 0
}

func (m *metaData) doReaddir(ctx meta.Context, inode meta.Ino) syscall.Errno {
	cino, eno := m.GetInode(inode)
	if eno != 0 {
		return eno
	}

	dirPath := cino.GetPath()
	var files []os.DirEntry
	var okDir string
	if eno := m.runSourceOp("readdir", dirPath, func(dir string) syscall.Errno {
		srcPath := filepath.Join(dir, dirPath)
		entries, err := os.ReadDir(srcPath)
		if err != nil {
			eno := PathErrno(err)
			logger.Warnf("doReaddir: ReadDir %s failed: %v (errno=%d)", srcPath, err, eno)
			return eno
		}
		files = entries
		okDir = dir
		return 0
	}); eno != 0 {
		return eno
	}
	srcPath := filepath.Join(okDir, dirPath)

	var ino = meta.Ino(0)
	var attr = meta.Attr{}

	for _, de := range files {
		name := de.Name()
		dePath := filepath.Join(dirPath, name)
		if !m.isAllowedSubPath(dePath) {
			// Skip non-allowed subpaths
			continue
		}
		if eno := m.doLookup(ctx, inode, name, &ino, &attr); eno != 0 {
			logger.Warnf("doReaddir: lookup file \"%s\" failed: %s",
				filepath.Join(srcPath, name), eno)
		}
	}
	return 0
}

func (m *metaData) timeit(start time.Time) {
	m.opDist.Observe(time.Since(start).Seconds())
}

func (m *metaData) reloadSubpathFile() {
	conf := &m.config.SubPathConfig
	if (conf.Cmd == "" && conf.File == "") || conf.Reload <= 0 {
		return
	}

	interval := time.Duration(conf.Reload) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
		}

		subPaths, cmdOut, cmdDur, err := genSourceSubpaths(conf)
		if err != nil {
			// error detail already logged in genSourceSubpaths/ReadPathsFromCmd
			continue
		}
		same, removed := subPathEqual(m.sourceSubPath, subPaths)
		if same {
			continue
		}
		// cmdOut is non-empty only in Cmd mode
		if cmdOut != "" {
			logger.Infof("Source subpath command %q succeeded in %s, output: %s", conf.Cmd, cmdDur, cmdOut)
		}
		oldSubPaths := m.sourceSubPath
		m.sourceSubPathMu.Lock()
		m.sourceSubPath = subPaths
		m.sourceSubPathMu.Unlock()

		// Invalidate the whole in-memory dir cache, as the subpath list changed. doReaddir
		// only appends allowed entries without clearing the children map, so a cached dir
		// would keep serving now-disallowed entries until its own ReaddirTimeout.
		for i := range m.inodeShards {
			shard := &m.inodeShards[i]
			shard.Lock()
			inodes := make([]*Inode, 0, len(shard.inodes))
			for _, cino := range shard.inodes {
				inodes = append(inodes, cino)
			}
			shard.Unlock()
			for _, cino := range inodes {
				cino.childLock.Lock()
				if cino.readdir || len(cino.children) > 0 {
					cino.children = make(map[string]*Inode)
					cino.readdir = false
				}
				cino.childLock.Unlock()
			}
		}

		// Evict all cache associated with removed subpaths
		if m.config.RemoveCache != nil {
			var toRemove []string
			var toKeep []string
			if len(subPaths) > 0 && len(removed) > 0 {
				// no need to remove cache if new subPaths is empty,
				// which means allow all paths.
				toRemove = removed
			} else if len(oldSubPaths) == 0 {
				for p := range subPaths {
					toKeep = append(toKeep, p+string(os.PathSeparator))
				}
			}
			if len(toRemove) > 0 || len(toKeep) > 0 {
				start := time.Now()
				cnt, size := m.config.RemoveCache(toRemove, toKeep)
				reason := fmt.Sprintf("subpaths removed %v", toRemove)
				if len(toRemove) == 0 {
					reason = fmt.Sprintf("subpath restricted from allow-all, kept only %v", toKeep)
				}
				logger.Infof("Evicted %dMiB cache from %d blocks in %s: %s",
					size>>20, cnt, time.Since(start), reason)
			}
		}

		// Trigger cache data reload as subpaths changed
		if m.config.ReloadCache != nil {
			m.config.ReloadCache()
		}

		paths := []string{}
		for path := range subPaths {
			paths = append(paths, path)
		}
		logger.Infof("Reloaded new source subpath: %v", paths)
	}
}

func (m *metaData) inodeShard(ino meta.Ino) *inodeShard {
	return &m.inodeShards[uint64(ino)&(inodeShardCount-1)]
}

func (m *metaData) inodeCount() uint64 {
	var count uint64
	for i := range m.inodeShards {
		shard := &m.inodeShards[i]
		shard.Lock()
		count += uint64(len(shard.inodes))
		shard.Unlock()
	}
	return count
}

func (m *metaData) storeAttr(cino *Inode, attr *meta.Attr) *meta.Attr {
	cached := &inodeAttr{attr: *attr, loadedAt: time.Now()}
	old := cino.attrCache.Swap(cached)
	if old == nil {
		m.cachedAttrs.Add(1)
	}
	return &cached.attr
}

// ensureAttr returns the cached attr, loading it from the source when missing.
func (m *metaData) ensureAttr(ctx meta.Context, cino *Inode) (*meta.Attr, syscall.Errno) {
	if attr := cino.GetAttr(); attr != nil {
		return attr, 0
	}
	return m.loadAttr(ctx, cino)
}

// loadAttr reads the attr from the source and caches the new immutable snapshot.
func (m *metaData) loadAttr(ctx meta.Context, cino *Inode) (*meta.Attr, syscall.Errno) {
	path := cino.GetPath()
	var fi os.FileInfo
	if eno := m.runSourceOp("getattr", path, func(dir string) syscall.Errno {
		var err error
		fi, err = os.Lstat(filepath.Join(dir, path))
		return PathErrno(err)
	}); eno != 0 {
		return nil, eno
	}

	var attr meta.Attr
	_ = m.fileInfoToAttr(fi, &attr)
	if cino.parent != nil {
		attr.Parent = cino.parent.inode
	} else {
		attr.Parent = cino.inode
	}
	return m.storeAttr(cino, &attr), 0
}

// cachedAttr returns the cached attr only when it is present and within the readdir timeout.
func (m *metaData) cachedAttr(cino *Inode) *meta.Attr {
	cached := cino.attrCache.Load()
	if isStaleAttr(cached, time.Duration(m.config.ReaddirTimeout)*time.Second) {
		return nil
	}
	return &cached.attr
}

// isStaleAttr reports whether cached is nil or expired; a negative timeout disables expiry.
func isStaleAttr(cached *inodeAttr, timeout time.Duration) bool {
	if cached == nil {
		return true
	}
	return timeout >= 0 && time.Since(cached.loadedAt) > timeout
}

// cachefs inode public methods
func (i *Inode) GetPath() string {
	return i.buildPath()
}

// buildPath joins the basenames up the parent chain. A parent-less inode is a
// tree root and returns its own name verbatim.
func (i *Inode) buildPath() string {
	if i.parent == nil {
		return i.name
	}

	var nodes []*Inode
	length := 0
	cur := i
	for cur.parent != nil {
		nodes = append(nodes, cur)
		length += len(cur.name) + 1
		cur = cur.parent
	}
	rootName := cur.name
	if rootName != "/" {
		length += len(rootName)
	}

	var b strings.Builder
	b.Grow(length)
	if rootName != "/" {
		b.WriteString(rootName)
	}
	for idx := len(nodes) - 1; idx >= 0; idx-- {
		b.WriteByte('/')
		b.WriteString(nodes[idx].name)
	}
	return b.String()
}

func (i *Inode) GetAttr() *meta.Attr {
	cached := i.attrCache.Load()
	if cached == nil {
		return nil
	}
	return &cached.attr
}

// Ino returns the inode number.
func (i *Inode) Ino() meta.Ino {
	return i.inode
}

// Lookup looks up a child inode by name in the in-memory children map.
// It is safe to call concurrently.
func (i *Inode) Lookup(name string) (*Inode, bool) {
	i.childLock.Lock()
	child, ok := i.children[name]
	i.childLock.Unlock()
	return child, ok
}

// Children returns a shallow copy of the children map under the
// childLock.  Callers must not modify the returned map.
func (i *Inode) Children() map[string]*Inode {
	i.childLock.Lock()
	out := make(map[string]*Inode, len(i.children))
	for k, v := range i.children {
		out[k] = v
	}
	i.childLock.Unlock()
	return out
}

// helper functions
func PathErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if perr, ok := err.(*os.PathError); ok {
		if eno, ok := perr.Err.(syscall.Errno); ok {
			return eno
		}
	}
	if status.Code(err) == codes.Canceled || err.Error() == "context canceled" {
		return syscall.EINTR
	}
	if status.Code(err) == codes.Unauthenticated {
		return syscall.EPERM
	}
	logger.Errorf("error: %s\n%s", err, debug.Stack())
	return syscall.EIO
}

// When conf.Cmd is used, also returns the command's stdout and how long it took,
// empty/zero otherwise.
func genSourceSubpaths(conf *SourceSubPathConfig) (map[string]struct{}, string, time.Duration, error) {
	var err error
	var rawSubPaths []string
	var cmdOut string
	var cmdDur time.Duration

	if conf.Cmd != "" {
		rawSubPaths, cmdOut, cmdDur, err = utils.ReadPathsFromCmd("Source subpath", conf.Cmd, 5*time.Second)
		if err != nil {
			return nil, "", 0, fmt.Errorf("read from command %s: %w", conf.Cmd, err)
		}
	} else if conf.File != "" {
		rawSubPaths, err = utils.ReadPathsFromFile(conf.File)
		if err != nil {
			logger.Warnf("Failed to read source subpaths from file %s: %s", conf.File, err)
			return nil, "", 0, fmt.Errorf("read from file %s: %w", conf.File, err)
		}
	} else {
		rawSubPaths = strings.Fields(conf.Paths)
	}

	pathSet := make(map[string]struct{})
	for _, path := range rawSubPaths {
		path = filepath.Clean(path)
		if path == "/" {
			logger.Warn("Source subpath contains \"/\", which allows all subpaths in source dir, clear source subpath config")
			pathSet = make(map[string]struct{})
			break
		} else if !strings.HasPrefix(path, string(os.PathSeparator)) {
			logger.Warnf("Source subpath \"%s\" doesn't start with \"%s\", skip", path, string(os.PathSeparator))
		} else {
			// dedupe subpaths
			_, ok := pathSet[path]
			if !ok {
				pathSet[path] = struct{}{}
			} else {
				logger.Infof("Duplicated source subpath \"%s\", skip", path)
			}
		}
	}
	return pathSet, cmdOut, cmdDur, nil
}

// Compare and return keys exist in `oldSet` but not in `newSet`
func getRemovedKeys(oldSet, newSet map[string]struct{}) []string {
	removed := make([]string, 0, len(oldSet))
	for key := range oldSet {
		if _, exists := newSet[key]; !exists {
			// Append "/" to the end of key to make sure removed cache keys are all
			// dir-separated. e.g. we had subpaths /testfile and /test which is a dir,
			// and new subpath has only /testfile, we have to make sure cache of
			// /testfile is not removed.
			removed = append(removed, key+string(os.PathSeparator))
		}
	}
	return removed
}

func subPathEqual(oldSet, newSet map[string]struct{}) (bool, []string) {
	if utils.StringSetEqual(oldSet, newSet) {
		return true, []string{}
	}
	return false, getRemovedKeys(oldSet, newSet)
}

var _ MetaData = &metaData{}
