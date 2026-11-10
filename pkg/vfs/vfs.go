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

package vfs

import (
	"encoding/json"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/meta"
	cachefsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	serfSvc "github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/eryugey/serf/serf"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	rootID      = 1
	maxName     = meta.MaxName
	maxFileSize = meta.ChunkSize << 31
)

type Ino = meta.Ino
type Attr = meta.Attr
type Context = LogContext

type Statfs struct {
	Total  uint64
	Avail  uint64
	Files  uint64
	Favail uint64
}

// VFS-layer counter names registered in the per-instance CounterRegistry.
// Both registration and lookup (vfs/preload) must use these constants so a
// typo becomes a compile error.
const (
	counterSourceReads     = "source_reads"
	counterSourceReadBytes = "source_read_bytes"
)

var (
	readSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fuse_read_size_bytes",
		Help:    "size of read distributions.",
		Buckets: prometheus.LinearBuckets(4096, 4096, 32),
	})

	// cache metrics, include remote cache
	cacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cache_hits",
		Help: "read from cache",
	})
	cacheMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cache_miss",
		Help: "missed read from cache",
	})
	cacheHitBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cache_hit_bytes",
		Help: "read bytes from cache",
	})
	cacheMissBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cache_miss_bytes",
		Help: "missed bytes from cache",
	})

	// read from source metrics, include prefetch
	sourceReads = metric.NewCounter(prometheus.CounterOpts{
		Name: counterSourceReads,
		Help: "reads from source",
	})
	sourceReadHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "source_read_hist_seconds",
		Help:    "reads from source latency distributions.",
		Buckets: prometheus.ExponentialBuckets(0.01, 1.5, 25),
	})
	sourceReadErrors = metric.NewCounter(prometheus.CounterOpts{
		Name: "source_read_errors",
		Help: "failed reads from source",
	})
	sourceReadBytes = metric.NewCounter(prometheus.CounterOpts{
		Name: counterSourceReadBytes,
		Help: "read from source size in bytes.",
	})

	// Per-replica views of the source read metrics above, labeled by source
	// dir; the aggregate counters feeding load_stats are unchanged.
	sourceReadsPerSource = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "source_reads_per_source",
		Help: "reads served by each source dir replica",
	}, []string{"source"})
	sourceReadBytesPerSource = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "source_read_bytes_per_source",
		Help: "read bytes served by each source dir replica",
	}, []string{"source"})
	sourceReadErrorsPerSource = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "source_read_errors_per_source",
		Help: "failed read attempts on each source dir replica, including failures rescued by fallback",
	}, []string{"source"})
	sourceReadHistPerSource = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "source_read_hist_seconds_per_source",
		Help:    "read latency distributions of each source dir replica.",
		Buckets: prometheus.ExponentialBuckets(0.01, 1.5, 25),
	}, []string{"source"})

	// sourceless mode metrics
	sourcelessRetry = metric.NewCounter(prometheus.CounterOpts{
		Name: "sourceless_retry",
		Help: "peer load retry count in sourceless meta consumer mode",
	})
	sourcelessRetryBytes = metric.NewCounter(prometheus.CounterOpts{
		Name: "sourceless_retry_bytes",
		Help: "peer load retry bytes in sourceless meta consumer mode",
	})

	sourceReadInFlight int32 = 0

	// sourceDirectIOEnabledPerSource reflects, per source dir replica, whether
	// O_DIRECT is currently in effect for reads on it: 1 while enabled, 0
	// when disabled/ignored or permanently downgraded to buffered IO.
	sourceDirectIOEnabledPerSource = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "source_directio_enabled",
		Help: "whether O_DIRECT is currently in effect for source reads on each source dir replica (1) or not (0)",
	}, []string{"source"})
)

type PinPathConfig struct {
	Paths  string
	File   string
	Cmd    string
	Reload int
}

type Config struct {
	Meta            *cachefsmeta.MetaConfig
	Store           *chunk.StoreConfig
	ShareReserve    int64 // bytes of burst cache reserved for regular read traffic, unavailable to the mmap API (GetFileChunks)
	BlockPoolSize   int64 // bytes of idle warm blocks, from --buffer-size; 0 disables the pool
	BufferReuse     bool
	BufferPrefault  bool
	BufferTimeout   int // seconds idle warm blocks are retained after the last allocation, 0 means no timeout
	AttrTimeout     time.Duration
	EntryTimeout    time.Duration
	DirEntryTimeout time.Duration
	NegativeTimeout time.Duration
	Prefetch        uint32
	PrefetchWorkers uint32
	Preload         []string
	PreloadInterval int
	PreloadWorkers  uint32
	ForcePreload    bool
	PinPath         PinPathConfig
	ApiSocket       string
	DaemonSocket    string
	SafeCacheKey    bool
	SourceDirectIO  bool
}

// run in fuse only mode or not.
// when running in fuse layer only mode, there's no cache layer, which is running as a standalone
// daemon, and daemon socket path must not be empty.
func (c *Config) IsFuseLayerMode() bool {
	return c.DaemonSocket != ""
}

type Cachefs struct {
	Conf         *Config
	Meta         cachefsmeta.MetaData
	cache        cacheReader
	source       sourceStore
	sourceReader *sourceReader

	handles map[Ino][]*handle
	handleM sync.Mutex
	nextfh  uint64

	prefetchMu sync.RWMutex
	prefetchCh chan *prefetchReq
	prefetches map[string]*prefetchStatus

	preloadState *preloadState
	pinPaths     map[string]struct{}

	// pending cache operations manager
	pendingOps *utils.PendingOpsManager

	mountMu sync.Mutex
	mountCh chan struct{}

	api *apiServer

	serf      serfSvc.SerfService
	serfUser  *serfSvc.SerfUser
	birthTime time.Time

	registry   *prometheus.Registry
	counterReg *metric.CounterRegistry // per-instance registry for VFS-layer counters

	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewCachefs(conf *Config, m cachefsmeta.MetaData, store chunk.Store,
	registerer prometheus.Registerer, registry *prometheus.Registry,
	serfService serfSvc.SerfService, rpcSvc rpcsvc.RpcService, cacherootSvc rpcsvc.RpcService,
) *Cachefs {
	prefetchLen := int(conf.Prefetch)
	if prefetchLen < 8 {
		prefetchLen = 8
	}
	fs := &Cachefs{
		Conf:       conf,
		Meta:       m,
		handles:    make(map[Ino][]*handle),
		nextfh:     1,
		prefetches: make(map[string]*prefetchStatus),
		prefetchCh: make(chan *prefetchReq, prefetchLen),
		mountCh:    make(chan struct{}),
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
		registry:   registry,
		serf:       serfService,
		counterReg: metric.NewCounterRegistry(),
		stopCh:     make(chan struct{}),
	}
	fs.counterReg.Register(counterSourceReads, sourceReads)
	fs.counterReg.Register(counterSourceReadBytes, sourceReadBytes)
	fs.InitMetrics(registerer)
	cache := newCacheReader(fs, store, registerer)
	fs.cache = cache
	fs.source = newSourceStore(fs, conf.Meta.SourceDirs, conf.Store.BlockSize)
	fs.sourceReader = newSourceReader(fs, fs.source, sourceSingleLoadTimeout)
	n := getInternalNode(configInode)
	data, _ := json.MarshalIndent(fs.Conf, "", " ")
	n.attr.Length = uint64(len(data))

	if conf.ApiSocket != "" {
		go func() {
			fs.WaitMount() // Wait for filesystem to be mounted
			var err error
			fs.api, err = newApiServer(conf.ApiSocket, fs, registerer)
			if err != nil {
				logger.Fatal(err.Error())
			}
			fs.api.serve()
		}()
	}

	if fs.Conf.IsFuseLayerMode() {
		// Setup THP policy separately in fuse only mode.
		utils.SetupThp(conf.Store.ThpPolicy)

		// Cache pages are always allocated off the Go heap, via memfd or anonymous
		// mmap, to keep the live heap low and avoid OOM.
		if chunk.PageMemfd {
			utils.SetupShmThp(conf.Store.ThpPolicy)
			logger.Info("Use memfd to allocate memory for cache")
		} else {
			logger.Info("Use mmap to allocate memory for cache")
		}

		// Disable GO GC by default to keep goruntime heap low, and limit heap usage under 1GiB.
		utils.SetupGoGc(conf.Store.GoGc, int64(1<<30))

		// All THP policies set, now check if we should do madvise call for hugepage.
		utils.SetupMadvise()
	}

	poolSlots := 0
	if conf.Store.BlockSize > 0 {
		poolSlots = int(conf.BlockPoolSize / int64(conf.Store.BlockSize))
	}
	utils.SetupBlockPool(&utils.BlockPoolConfig{
		BlockSize: int(conf.Store.BlockSize),
		Slots:     poolSlots,
		Timeout:   time.Duration(conf.BufferTimeout) * time.Second,
		Reuse:     conf.BufferReuse,
		Prefault:  conf.BufferPrefault,
		Memfd:     chunk.PageMemfd,
	})

	go fs.prefetch()

	conf.Meta.RemoveCache = fs.cache.removeByPrefix
	conf.Meta.ReloadCache = func() {
		if len(conf.Preload) == 0 {
			// No initial preload setup, skip cache reload
			return
		}
		err := fs.loadCache(nil)
		if err != 0 {
			logger.Warnf("Trigger load cache failed: %s", err)
		}
	}

	// Initialize pin cache
	fs.initPinPath()

	// Start preload workers
	var req *loadReq
	if len(conf.Preload) > 0 {
		req = &loadReq{
			paths:      conf.Preload,
			stopOnFull: true,
			force:      conf.ForcePreload,
		}
		// Cache size is big enough in sourceless meta consumer mode, just load all data
		if conf.Meta.MetaConsumer() {
			req.loadAll = true
		}
	}
	go fs.preload(req)

	// Register and setup serf user for VFS layer preload broadcasting
	if serfService != nil {
		fs.serfUser = &serfSvc.SerfUser{
			Name:        "vfs",
			UserEvents:  supportedSerfEvents,
			UserEventCh: make(chan serf.UserEvent, 128),
			Filter:      &vfsEventFilter{},
			ReadyCh:     make(chan struct{}),
		}
		if err := serfService.RegisterUser(fs.serfUser); err != nil {
			logger.Warnf("Register VFS serf user: %s", err)
		} else {
			go fs.handleSerfUserEvent()
		}
	}
	fs.birthTime = time.Now().UTC()

	// Register vfs-implemented services on the shared rpc servers before they
	// start serving: CachefsState on the internal rpc server (cachePort) and
	// CacheRootCluster on the cacheroot rpc server (cacheroot-port).
	fs.registerCacheRootServices(rpcSvc, cacherootSvc, registerer)

	// In peer meta auto mode, cache all meta and announce meta ready after mount so this node
	// can act as a meta source for other peers. In manual mode, meta-ready must be triggered
	// explicitly.
	if fs.Conf.Meta.PeerMetaAutoMode() {
		go func() {
			fs.WaitMount()
			start := time.Now()
			eno := fs.Meta.MetaReady()
			if eno != 0 {
				logger.Warnf("Set meta ready failed: %s", eno)
			} else {
				logger.Infof("Set meta ready in auto mode in %s", time.Since(start))
			}
		}()
	}
	return fs
}

// Cachefs fuse callbacks
func (c *Cachefs) Lookup(ctx Context, parent Ino, name string) (entry *meta.Entry, err syscall.Errno) {
	var inode Ino
	var attr = &Attr{}
	if parent == rootID {
		n := getInternalNodeByName(name)
		if n != nil {
			entry = &meta.Entry{Inode: n.inode, Attr: n.attr}
			return
		}
	}
	defer func() {
		logit(ctx, "lookup (%d,%s): %s%s", parent, name, strerr(err), (*Entry)(entry))
	}()
	if len(name) > maxName {
		err = syscall.ENAMETOOLONG
		return
	}
	err = c.Meta.Lookup(ctx, parent, name, &inode, attr)
	if err == 0 {
		entry = &meta.Entry{Inode: inode, Attr: attr}
	}
	return
}

func (c *Cachefs) GetAttr(ctx Context, ino Ino, opened uint8) (entry *meta.Entry, err syscall.Errno) {
	if IsSpecialNode(ino) && getInternalNode(ino) != nil {
		n := getInternalNode(ino)
		entry = &meta.Entry{Inode: n.inode, Attr: n.attr}
		return
	}
	defer func() { logit(ctx, "getattr (%d): %s%s", ino, strerr(err), (*Entry)(entry)) }()
	var attr = &Attr{}
	err = c.Meta.GetAttr(ctx, ino, attr)
	if err == 0 {
		entry = &meta.Entry{Inode: ino, Attr: attr}
	}
	return
}

func (c *Cachefs) SetAttr(ctx Context, ino Ino, set int, opened uint8, mode, uid, gid uint32, atime, mtime int64, atimensec, mtimensec uint32, size uint64) (entry *meta.Entry, err syscall.Errno) {
	return nil, syscall.EROFS
}

func (c *Cachefs) Mknod(ctx Context, parent Ino, name string, mode uint16, cumask uint16, rdev uint32) (entry *meta.Entry, err syscall.Errno) {
	return nil, syscall.EROFS
}

func (c *Cachefs) Mkdir(ctx Context, parent Ino, name string, mode uint16, cumask uint16) (entry *meta.Entry, err syscall.Errno) {
	return nil, syscall.EROFS
}

func (c *Cachefs) Unlink(ctx Context, parent Ino, name string) (err syscall.Errno) {
	return syscall.EROFS
}

func (c *Cachefs) Rmdir(ctx Context, parent Ino, name string) (err syscall.Errno) {
	return syscall.EROFS
}

func (c *Cachefs) Rename(ctx Context, parent Ino, name string, newparent Ino, newname string, flags uint32) (err syscall.Errno) {
	return syscall.EROFS
}

func (c *Cachefs) Link(ctx Context, ino Ino, newparent Ino, newname string) (entry *meta.Entry, err syscall.Errno) {
	return nil, syscall.EROFS
}

func (c *Cachefs) Symlink(ctx Context, path string, parent Ino, name string) (entry *meta.Entry, err syscall.Errno) {
	return nil, syscall.EROFS
}

func (c *Cachefs) Readlink(ctx Context, ino Ino) (path []byte, err syscall.Errno) {
	defer func() { logit(ctx, "readlink (%d): %s (%s)", ino, strerr(err), string(path)) }()
	err = c.Meta.ReadLink(ctx, ino, &path)
	return
}

func (c *Cachefs) GetXattr(ctx Context, ino Ino, name string, size uint32) (value []byte, err syscall.Errno) {
	return nil, syscall.ENOTSUP
}

func (c *Cachefs) ListXattr(ctx Context, ino Ino, size int) (data []byte, err syscall.Errno) {
	return nil, syscall.ENOTSUP
}

func (c *Cachefs) SetXattr(ctx Context, ino Ino, name string, value []byte, flags uint32) (err syscall.Errno) {
	return syscall.EROFS
}

func (c *Cachefs) RemoveXattr(ctx Context, ino Ino, name string) (err syscall.Errno) {
	return syscall.EROFS
}

func (c *Cachefs) Create(ctx Context, parent Ino, name string, mode uint16, cumask uint16, flags uint32) (entry *meta.Entry, fh uint64, err syscall.Errno) {
	return nil, 0, syscall.EROFS
}

func (c *Cachefs) Open(ctx Context, ino Ino, flags uint32) (entry *meta.Entry, fh uint64, err syscall.Errno) {
	defer func() {
		if entry != nil {
			logit(ctx, "open (%d): %s [fh:%d]", ino, strerr(err), fh)
		} else {
			logit(ctx, "open (%d): %s", ino, strerr(err))
		}
	}()
	var attr = &Attr{}
	if IsSpecialNode(ino) {
		if ino != controlInode && (flags&syscall.O_ACCMODE) != syscall.O_RDONLY {
			err = syscall.EACCES
			return
		}
		h := c.newHandle(ino, nil, nil)
		fh = h.fh
		n := getInternalNode(ino)
		if n == nil {
			return
		}
		entry = &meta.Entry{Inode: ino, Attr: n.attr}
		switch ino {
		case logInode:
			openAccessLog(fh)
		case statsInode:
			h.data = collectMetrics(c.registry)
		case configInode:
			h.data, _ = json.MarshalIndent(c.Conf, "", " ")
			entry.Attr.Length = uint64(len(h.data))
		}
		return
	}

	err = c.Meta.Open(ctx, ino, flags, attr)
	if err != 0 {
		return
	}
	cino, err := c.Meta.GetInode(ino)
	if err != 0 {
		return
	}
	fh = c.newHandle(ino, cino, attr).fh
	entry = &meta.Entry{Inode: ino, Attr: attr}
	return
}

func (c *Cachefs) Read(ctx Context, ino Ino, buf []byte, off uint64, fh uint64) (n int, err syscall.Errno) {
	size := uint64(len(buf))
	if IsSpecialNode(ino) {
		return c.readInternalFile(ctx, ino, buf, off, fh)
	}

	defer func() {
		readSizeHistogram.Observe(float64(n))
		logit(ctx, "read (%d,%d,%d): %s (%d)", ino, size, off, strerr(err), n)
	}()
	h := c.getHandle(ino, fh)
	if h == nil {
		err = syscall.EBADF
		return
	}
	if off >= maxFileSize || off+size >= maxFileSize {
		err = syscall.EFBIG
		return
	}
	if off >= h.length {
		return
	}
	if off+size > h.length {
		size = h.length - off
	}

	blockSize := c.Conf.Store.BlockSize
	sblk, eblk := c.getBlockIndex(off, size)
	readdata := func(reader chunk.ChunkReader) error {
		boff := off % uint64(blockSize)
		read, rderr := reader.ReadAt(buf[n:], int64(boff))
		reader.Close()
		n += read
		off += uint64(read)
		if rderr == nil || rderr == io.EOF {
			return nil
		} else {
			return rderr
		}
	}

	for blk := sblk; blk < eblk; blk++ {
	read_from_store:
		dataSize := c.bytesInBlock(h.target, off, size, blk)

		// read from store
		reader, rerr := c.cache.readFromStore(ctx, h.target, h.length, blk, dataSize, true)
		if rerr == nil {
			if rderr := readdata(reader); rderr == nil {
				cacheHits.Inc()
				cacheHitBytes.Add(float64(dataSize))
				continue
			}
		}

		// check & wait for pending cache, in case it's being added to cache store
		cached := c.checkPendingCache(h.target, blk)
		if cached {
			goto read_from_store
		}

		cacheMiss.Inc()
		cacheMissBytes.Add(float64(dataSize))

		// cache miss, read from source and add data to cache
		reader, rerr = c.sourceReader.readFromSource(h.sourceFile, h.target, h.length, blk, true)
		if rerr == nil {
			if rderr := readdata(reader); rderr == nil {
				continue
			}
		}

		// all read failed
		logger.Warnf("read %s [%v:%v:%v]: %s", h.target, ino, off, size, rerr)
		err = syscall.EIO
		return 0, err
	}

	// Do prefetch
	go func() {
		start := eblk
		end := sblk + c.Conf.Prefetch
		if end > h.blkcnt {
			end = h.blkcnt
		}
		for blk := start; blk < end; blk++ {
			if !c.checkPrefetch(h.target, blk) {
				continue
			}
			req := prefetchReqPool.Get().(*prefetchReq)
			req.target = h.target
			req.srcTarget = h.srcTarget
			req.length = h.length
			req.block = blk
			req.cache = true
			req.ctx = h.ctx
			req.doneCh = nil
			select {
			case c.prefetchCh <- req:
			default:
			}
		}
	}()
	return
}

func (c *Cachefs) Release(ctx Context, ino Ino, fh uint64) {
	defer func() { logit(ctx, "release (%d): OK", ino) }()
	if IsSpecialNode(ino) {
		if ino == logInode {
			closeAccessLog(fh)
		}
		c.releaseHandle(ino, fh)
		return
	}
	_ = c.Meta.Close(ctx, ino)
	c.releaseHandle(ino, fh)
}

func (c *Cachefs) Write(ctx Context, ino Ino, buf []byte, off, fh uint64) (err syscall.Errno) {
	// Only control inode supports write
	if ino != controlInode {
		return syscall.EROFS
	}
	h := c.getHandle(ino, fh)
	if h == nil {
		err = syscall.EBADF
		return
	}

	// Allow 1MiB control data
	maxSize := uint64(1) << 20
	dataSize := uint64(len(buf))
	if off >= maxSize || off+dataSize >= maxSize {
		err = syscall.EFBIG
		return
	}

	h.Lock()
	defer h.Unlock()
	h.pending = append(h.pending, buf...)
	rb := utils.ReadBuffer(h.pending)
	cmd := rb.Get32()
	size := int(rb.Get32())
	if rb.Left() < size {
		logger.Warnf("Control message not complete: cmd %d, expect size %d but %d left", cmd, size, rb.Left())
		h.data = append(h.data, uint8(syscall.EINVAL&0xff))
		return
	}
	h.data = append(h.data, h.pending...)
	h.pending = h.pending[:0]
	if rb.Left() == size {
		h.bctx = meta.NewContext(ctx.Pid(), ctx.Uid(), ctx.Gids())
		go c.handleInternalMsg(h.bctx, cmd, rb, h)
	} else {
		logger.Warnf("Broken control message: cmd %d, expect %d but %d left", cmd, size, rb.Left())
		h.data = append(h.data, uint8(syscall.EINVAL&0xff))
	}
	return
}

func (c *Cachefs) Flush(ctx Context, ino Ino, fh uint64, lockOwner uint64) (err syscall.Errno) {
	return 0
}

func (c *Cachefs) Fsync(ctx Context, ino Ino, datasync int, fh uint64) (err syscall.Errno) {
	return syscall.EROFS
}

func (c *Cachefs) Fallocate(ctx Context, ino Ino, mode uint8, off, length int64, fh uint64) (err syscall.Errno) {
	return syscall.ENOTSUP
}

func (c *Cachefs) CopyFileRange(ctx Context, nodeIn Ino, fhIn, offIn uint64, nodeOut Ino, fhOut, offOut, size uint64, flags uint32) (copied uint64, err syscall.Errno) {
	return 0, syscall.ENOTSUP
}

func (c *Cachefs) Getlk(ctx Context, ino Ino, fh uint64, owner uint64, start, len *uint64, typ *uint32, pid *uint32) (err syscall.Errno) {
	return syscall.ENOTSUP
}

func (c *Cachefs) Setlk(ctx Context, ino Ino, fh uint64, owner uint64, start, end uint64, typ uint32, pid uint32, block bool) (err syscall.Errno) {
	return syscall.ENOTSUP
}

func (c *Cachefs) Flock(ctx Context, ino Ino, fh uint64, owner uint64, typ uint32, block bool) (err syscall.Errno) {
	return syscall.ENOTSUP
}

func (c *Cachefs) Opendir(ctx Context, ino Ino) (fh uint64, err syscall.Errno) {
	var attr = &Attr{}
	defer func() { logit(ctx, "opendir (%d): %s [fh:%d]", ino, strerr(err), fh) }()
	err = c.Meta.Opendir(ctx, ino, attr)
	if err != 0 {
		return 0, err
	}
	cino, err := c.Meta.GetInode(ino)
	if err != 0 {
		return 0, err
	}
	fh = c.newHandle(ino, cino, attr).fh
	return fh, 0
}

func (c *Cachefs) Readdir(ctx Context, ino Ino, size uint32, off int, fh uint64, plus bool) (entries []*meta.Entry, readAt time.Time, err syscall.Errno) {
	defer func() { logit(ctx, "readdir (%d,%d,%d): %s (%d)", ino, size, off, strerr(err), len(entries)) }()
	h := c.getHandle(ino, fh)
	if h == nil {
		err = syscall.EBADF
		return
	}
	h.Lock()
	defer h.Unlock()

	if h.children == nil || off == 0 {
		var inodes []*meta.Entry
		err = c.Meta.Readdir(ctx, ino, &inodes)
		if err != 0 {
			return
		}
		h.children = inodes
	}
	if off < len(h.children) {
		entries = h.children[off:]
	}
	readAt = time.Now()
	return
}

func (c *Cachefs) Releasedir(ctx Context, ino Ino, fh uint64) int {
	c.releaseHandle(ino, fh)
	logit(ctx, "releasedir (%d): OK", ino)
	return 0
}

func (c *Cachefs) StatFS(ctx Context, ino Ino) (st *Statfs, err syscall.Errno) {
	var totalspace, availspace, itotal, iavail uint64
	defer func() {
		logit(ctx, "statfs (%d): %s (%d,%d,%d,%d)", ino, strerr(err), totalspace-availspace, availspace, itotal-iavail, iavail)
	}()
	err = c.Meta.StatFS(ctx, &totalspace, &availspace, &itotal, &iavail)
	if err != 0 {
		return nil, err
	}
	st = new(Statfs)
	st.Total = totalspace
	st.Avail = availspace
	st.Files = itotal
	st.Favail = iavail
	return
}

func (c *Cachefs) SetMounted() {
	c.mountMu.Lock()
	if c.mountCh != nil {
		c.mountMu.Unlock()
		close(c.mountCh)
		c.mountCh = nil
	} else {
		c.mountMu.Unlock()
	}
}

// WaitMount waits for filesystem to be mounted. Use this to avoid racing between accessing the
// (empty or not yet mounted) mountpoint, and the OS trying to setup the user-space mount.
func (c *Cachefs) WaitMount() {
	c.mountMu.Lock()
	if c.mountCh == nil {
		c.mountMu.Unlock()
		return
	}
	c.mountMu.Unlock()
	<-c.mountCh
}

func (c *Cachefs) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	if c.api != nil {
		c.api.stop()
	}
	if c.preloadState != nil {
		c.preloadState.stop()
	}
	c.cache.stop()
}

func (c *Cachefs) InitMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "fuse_open_handlers",
		Help: "number of open files and directories.",
	}, func() float64 {
		c.handleM.Lock()
		defer c.handleM.Unlock()
		return float64(len(c.handles))
	}))

	initPackageMetrics(reg)
}

func initPackageMetrics(reg prometheus.Registerer) {
	reg.MustRegister(readSizeHistogram)
	reg.MustRegister(opsDurationsHistogram)
	reg.MustRegister(cacheHits)
	reg.MustRegister(cacheHitBytes)
	reg.MustRegister(cacheMiss)
	reg.MustRegister(cacheMissBytes)
	reg.MustRegister(sourceReads)
	reg.MustRegister(sourceReadHist)
	reg.MustRegister(sourceReadErrors)
	reg.MustRegister(sourceReadBytes)
	reg.MustRegister(sourceReadsPerSource)
	reg.MustRegister(sourceReadBytesPerSource)
	reg.MustRegister(sourceReadErrorsPerSource)
	reg.MustRegister(sourceReadHistPerSource)
	reg.MustRegister(sourcelessRetry)
	reg.MustRegister(sourcelessRetryBytes)
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "source_read_flight",
			Help: "in flight read from source",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&sourceReadInFlight))
		}))
	reg.MustRegister(sourceDirectIOEnabledPerSource)
}

func (c *Cachefs) readInternalFile(ctx Context, ino Ino, buf []byte, off uint64, fh uint64) (n int, err syscall.Errno) {
	size := uint64(len(buf))
	if ino == logInode {
		n = readAccessLog(fh, buf)
		return
	}
	defer func() { logit(ctx, "read (%d,%d,%d,%d): %s (%d)", ino, size, off, fh, strerr(err), n) }()
	h := c.getHandle(ino, fh)
	if h == nil {
		err = syscall.EBADF
		return
	}
	h.Lock()
	defer h.Unlock()
	if off < h.off {
		logger.Errorf("read dropped data from %s: %d < %d", ino, off, h.off)
		err = syscall.EIO
		return
	}
	if int(off-h.off) < len(h.data) {
		n = copy(buf, h.data[off-h.off:])
	}
	if len(h.data) > 2<<20 && off-h.off > 1<<20 {
		// drop first part to avoid OOM
		h.off += 1 << 20
		h.data = h.data[1<<20:]
	}
	return
}

// convert given offset and size to block index
func (c *Cachefs) getBlockIndex(offset, size uint64) (uint32, uint32) {
	blksize := uint64(c.Conf.Store.BlockSize)
	sblk := uint32(offset / blksize)
	eblk := uint32((offset + size + blksize - 1) / blksize)
	return sblk, eblk
}

// get data size in given block
func (c *Cachefs) bytesInBlock(target string, offset, size uint64, blknum uint32) uint32 {
	blksize := uint64(c.Conf.Store.BlockSize)
	blkBegin := uint64(blknum) * blksize
	blkEnd := blkBegin + blksize

	if offset >= blkEnd {
		return 0
	}
	if offset+size <= blkBegin {
		return 0
	}

	var dataBegin uint64 = 0
	var dataEnd uint64 = blksize
	if blkBegin < offset {
		dataBegin = offset - blkBegin
	}
	if blkEnd > offset+size {
		dataEnd = dataEnd - (blkEnd - (offset + size))
	}
	if dataEnd < dataBegin {
		logger.Warnf("Invalid bytes in block: target %s, offset %v, size %v, blknum %v, dataBegin %v, dataEnd %v", target, offset, size, blknum, dataBegin, dataEnd)
		return 0
	}
	return uint32(dataEnd - dataBegin)
}

// Generate cache target used as part of cache key to interact with cache store,
// if `SafeCacheKey` is set, embed file i_size and mtime meta info to target key
// to uniquely identify a target, otherwise just use file path as target key.
func (c *Cachefs) encodeCacheTarget(path string, length uint64, mtime int64, mtimensec uint32) string {
	if !c.Conf.SafeCacheKey {
		return path
	}
	return fmt.Sprintf("%s:%d-%d-%d", path, length, mtime, mtimensec)
}

func (c *Cachefs) inodeToCacheTarget(cino *cachefsmeta.Inode, attr *meta.Attr) string {
	return c.encodeCacheTarget(cino.GetPath(), attr.Length, attr.Mtime, attr.Mtimensec)
}

// Convert relative paths to cache prefixes by looking up inodes.
// Returns the list of cache prefixes.
func (c *Cachefs) pathsToCachePrefixes(paths []string) []string {
	mountPoint := c.Conf.Meta.MountPoint
	prefix := make([]string, 0, len(paths))

	for _, relPath := range paths {
		if relPath == "" {
			continue
		}

		// Convert relative path to absolute path
		var absPath string
		if relPath == "/" {
			absPath = mountPoint
		} else {
			absPath = filepath.Join(mountPoint, relPath)
		}

		// Get inode from filesystem
		ino, err := utils.GetFileInode(absPath)
		if err != nil {
			logger.Warnf("Get inode for path %s failed: %s", absPath, err)
			continue
		}

		// Get cache prefix from inode
		cino, eno := c.Meta.GetInode(meta.Ino(ino))
		if eno != 0 {
			logger.Warnf("Get inode %d for path %s failed: %s", ino, absPath, eno)
			continue
		}
		prefix = append(prefix, cino.GetPath())
	}

	return prefix
}

func (c *Cachefs) addPendingCache(target string, blk uint32) bool {
	key := chunk.CacheKey{Target: target, Blk: blk}
	return c.pendingOps.Add(key.String())
}

func (c *Cachefs) removePendingCache(target string, blk uint32) {
	key := chunk.CacheKey{Target: target, Blk: blk}
	c.pendingOps.Remove(key.String())
}

func (c *Cachefs) checkPendingCache(target string, blk uint32) bool {
	key := chunk.CacheKey{Target: target, Blk: blk}
	return c.pendingOps.Wait(key.String())
}

// When conf.Cmd is used, it also returns the command's stdout and how long it took
// (empty/zero otherwise), so callers can log them only on first run / change.
func (c *Cachefs) parsePinPaths() ([]string, string, time.Duration, error) {
	conf := &c.Conf.PinPath
	var paths []string
	var cmdOut string
	var cmdDur time.Duration
	if conf.Cmd != "" {
		var err error
		paths, cmdOut, cmdDur, err = utils.ReadPathsFromCmd("Pin cache", conf.Cmd, 5*time.Second)
		if err != nil {
			return nil, "", 0, fmt.Errorf("read from command %s: %w", conf.Cmd, err)
		}
	} else if conf.File != "" {
		var err error
		paths, err = utils.ReadPathsFromFile(conf.File)
		if err != nil {
			logger.Warnf("Failed to read pin cache paths from file %s: %s", conf.File, err)
			return nil, "", 0, fmt.Errorf("read from file %s: %w", conf.File, err)
		}
	} else if conf.Paths != "" {
		paths = strings.Fields(conf.Paths)
	}

	var valid []string
	for _, p := range paths {
		p = path.Clean(p)
		if p == "/" {
			logger.Warnf("Pin path \"/\" pins everything and disables normal eviction, skipped")
			continue
		}
		if !strings.HasPrefix(p, "/") {
			logger.Warnf("Pin path %q does not start with '/', skipped", p)
			continue
		}
		valid = append(valid, p+"/")
	}
	return valid, cmdOut, cmdDur, nil
}

func (c *Cachefs) initPinPath() {
	// error already logged in parsePinPaths/ReadPathsFromCmd; proceed with empty pin paths
	pinPaths, cmdOut, cmdDur, _ := c.parsePinPaths()
	// cmdOut is non-empty only in Cmd mode
	if cmdOut != "" {
		logger.Infof("Pin cache command %q succeeded in %s, output: %s", c.Conf.PinPath.Cmd, cmdDur, cmdOut)
	}

	if c.Conf.IsFuseLayerMode() && len(pinPaths) > 0 {
		logger.Warnf("Pin cache is not supported in fuse layer (daemon) mode, ignoring --pin-cache config")
		return
	}

	if len(pinPaths) > 0 {
		c.cache.setPinCache(pinPaths)
		logger.Infof("Pin cache initialized with %d paths: %v", len(pinPaths), pinPaths)
	}
	c.pinPaths = utils.ToStringSet(pinPaths)

	go c.reloadPinPathFile()
}

func (c *Cachefs) reloadPinPathFile() {
	conf := &c.Conf.PinPath
	if (conf.Cmd == "" && conf.File == "") || conf.Reload <= 0 {
		return
	}

	interval := time.Duration(conf.Reload) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
		}

		newPaths, cmdOut, cmdDur, err := c.parsePinPaths()
		if err != nil {
			// error detail already logged in parsePinPaths/ReadPathsFromCmd
			continue
		}
		newSet := utils.ToStringSet(newPaths)
		if utils.StringSetEqual(c.pinPaths, newSet) {
			continue
		}
		// cmdOut is non-empty only in Cmd mode
		if cmdOut != "" {
			logger.Infof("Pin cache command %q succeeded in %s, output: %s", conf.Cmd, cmdDur, cmdOut)
		}

		c.cache.setPinCache(newPaths)
		c.pinPaths = newSet
		logger.Infof("Reloaded pin cache paths: %v", newPaths)
		if len(newSet) == 0 {
			continue
		}

		pinReq := &loadReq{
			paths: newPaths,
			force: true,
		}
		eno := c.loadCache(pinReq)
		if eno != 0 {
			if eno == syscall.EINPROGRESS {
				logger.Infof("Preload in progress, pin cache updated and will take effect on next preload cycle")
			} else {
				logger.Warnf("Preload triggered by pin cache failed: %s", eno)
			}
		}
	}
}
