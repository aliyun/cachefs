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

package cmd

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/fuse"
	meta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/srcmgr"
	"github.com/aliyun/cachefs/pkg/transfer"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs"
	"github.com/juicedata/godaemon"
)

const (
	defCacherootTrackerPort = 80
)

var gracePeriod int = 3

func cmdCache() *cli.Command {
	compoundFlags := [][]cli.Flag{
		commonFlags(),
		cacheFlags(),
	}
	return &cli.Command{
		Name:      "cache",
		Action:    cache,
		Category:  "SERVICE",
		Usage:     "Mount cachefs",
		ArgsUsage: "FSNAME MOUNTPOINT",
		Description: `
Mount cachefs to cache content from the given source dir, and multiple cachefs instances
construct a distributed cache cluster.

Examples:
# The mounted /mnt/cachefs mirrors and caches files in /mnt/slow to /mnt/fast
$ cachefs cache --source-dir /mnt/slow --cache-dir /mnt/fast cachefs /mnt/cachefs

# Use memory to cache content instead of a directory
$ cachefs cache --source-dir /mnt/slow --cache-dir memory cachefs /mnt/cachefs

# Join to an existing cluster by specifying an init node addr
$ cachefs cache --source-dir /mnt/slow --cache-dir memory --serf-join "192.168.1.2 192.168.1.3" cachefs /mnt/cachefs

# Change default block size to 4MiB, cachefs caches data by block
$ cachefs cache --source-dir /mnt/slow --cache-dir memory --block-size 4 cachefs /mnt/cachefs

# Change default cache size to 1G, cachefs will use at most cache-size in cache-dir
$ cachefs cache --source-dir /mnt/slow --cache-dir memory --cache-size 1024 cachefs /mnt/cachefs`,
		Flags: expandFlags(compoundFlags),
	}
}

func initMetrics(c *cli.Context, registry *prometheus.Registry, ss serf.SerfService) {
	var ip, port string
	// default set
	ip, port, err := net.SplitHostPort(c.String("metrics"))
	if err != nil {
		logger.Fatalf("metrics format error: %v", err)
	}

	http.Handle("/metrics", promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))

	if ss != nil {
		http.HandleFunc("/peers", peersHandler(ss, false))
		http.HandleFunc("/serf_members", peersHandler(ss, true))
	}

	addr := net.JoinHostPort(ip, port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		// Don't try other ports on metrics set but listen failed
		if c.IsSet("metrics") {
			logger.Errorf("failed to listen on metrics addr %s: %v", addr, err)
			return
		}

		// Listen port on 0 will auto listen on a free port
		logger.Infof("Failed to listen on metrics addr %s: %v, fallback to random port", addr, err)
		ln, err = net.Listen("tcp", net.JoinHostPort(ip, "0"))
		if err != nil {
			logger.Errorf("failed to listen on random metrics addr: %v", err)
			return
		}
	}

	go func() {
		if err := http.Serve(ln, nil); err != nil {
			logger.Errorf("Serve for metrics: %s", err)
		}
	}()

	logger.Infof("Prometheus metrics listening on %s", ln.Addr())

	return
}

func mkDaemon(c *cli.Context, name, mp string) error {
	var attrs godaemon.DaemonAttr
	logfile := c.String("log")
	attrs.OnExit = func(stage int) error {
		if stage != 0 {
			return nil
		}
		checkMountpoint(name, mp, logfile, true)
		liveAddr := c.String("liveness")
		liveSet := c.IsSet("liveness")
		if err := utils.LivenessProbe(liveAddr, !liveSet); err != nil {
			logger.Fatal(err.Error())
		}
		return nil
	}

	// the current dir will be changed to root in daemon,
	// so the mount point has to be an absolute path.
	if godaemon.Stage() == 0 {
		for i, a := range os.Args {
			if a == mp {
				amp, err := filepath.Abs(mp)
				if err == nil {
					os.Args[i] = amp
				} else {
					logger.Warnf("abs of %s: %s", mp, err)
				}
			}
		}
		var err error
		attrs.Stdout, err = os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			logger.Errorf("open log file %s: %s", logfile, err)
		}
	}
	_, _, err := godaemon.MakeDaemon(&attrs)
	return err
}

func runDaemon(c *cli.Context, metaConf *meta.MetaConfig) {
	// The default log to syslog is only in daemon mode.
	utils.InitLoggers(!c.Bool("no-syslog"))
	err := mkDaemon(c, metaConf.Name, metaConf.MountPoint)
	if err != nil {
		logger.Fatalf("Failed to make daemon: %s", err)
	}
}

func genMetaConf(c *cli.Context, fsname, mp string, sourceDirs []string, cacheSize int64) (*meta.MetaConfig, error) {
	cfg := &meta.MetaConfig{
		Name:            fsname,
		MountPoint:      mp,
		SourceDirs:      sourceDirs,
		Sourceless:      c.Bool("sourceless"),
		PeerMetaTimeout: c.Int("source-peermeta"),
		PeerMetaMode:    c.String("peermeta-mode"),
		CacheSize:       uint64(cacheSize),
		AttrCleanup:     c.Bool("attr-cleanup"),
		FuseCache:       !c.Bool("no-pagecache"),
		SubPathConfig: meta.SourceSubPathConfig{
			Paths:  c.String("source-subpath"),
			File:   c.String("source-subpath-file"),
			Cmd:    c.String("source-subpath-cmd"),
			Reload: c.Int("source-subpath-reload"),
		},
	}

	// Validate peermeta-mode value.
	switch cfg.PeerMetaMode {
	case "auto", "manual", "":
		// valid
	default:
		return nil, fmt.Errorf("--peermeta-mode must be 'auto' or 'manual', got %q", cfg.PeerMetaMode)
	}

	timeout := c.Int("readdir-cache")
	if timeout < 0 {
		timeout = -1
	}
	if timeout >= 0 {
		if cfg.Sourceless {
			logger.Info("Sourceless mode must cache all metadata, reset readdir-timeout to -1")
			timeout = -1
		}
		if cfg.PeerMeta() {
			logger.Info("Source peer meta must cache all metadata, reset readdir-timeout to -1")
			timeout = -1
		}
	}
	cfg.ReaddirTimeout = timeout

	// PeerMeta and subpath reload are mutually exclusive.
	if cfg.PeerMeta() && cfg.SubPathConfig.File != "" && cfg.SubPathConfig.Reload > 0 {
		return nil, fmt.Errorf("--source-peermeta and --source-subpath-reload are incompatible")
	}

	// Source subpath related configs are ignored in sourceless mode
	if cfg.Sourceless {
		sub := &cfg.SubPathConfig
		if sub.Paths != "" || sub.File != "" || sub.Reload != 0 {
			sub.Paths = ""
			sub.File = ""
			sub.Reload = 0
			logger.Info("Ignore source subpath configs in sourceless mode")
		}
	} else {
		timeout = c.Int("metaop-timeout")
		if timeout < 0 {
			timeout = 0
		}
		cfg.MetaOpTimeout = timeout

		// CacheSize is only needed in sourceless mode
		cfg.CacheSize = 0

		if cfg.SubPathConfig.File != "" && cfg.SubPathConfig.Reload > 0 && cfg.ReaddirTimeout < 0 {
			logger.Warnf("Source subpath file reload enabled, but readdir cache has no timeout, reset readdir cache to 30s")
			cfg.ReaddirTimeout = 30
		}
	}

	if cfg.AttrCleanup && (cfg.Sourceless || cfg.PeerMeta()) {
		logger.Warn("Attr cleanup is disabled in sourceless and peer-meta modes")
		cfg.AttrCleanup = false
	}
	if cfg.AttrCleanup && cfg.ReaddirTimeout < 0 {
		logger.Warn("Attr cleanup requires readdir-cache >= 0, disabling it")
		cfg.AttrCleanup = false
	}
	if cfg.AttrCleanup && cfg.MetaOpTimeout > 0 {
		logger.Warn("Attr cleanup cannot preserve metadata for metaop-timeout, disabling metaop-timeout")
		cfg.MetaOpTimeout = 0
	}

	return cfg, nil
}

func genSerfConf(c *cli.Context, storeConf *chunk.StoreConfig) (*serf.SerfConfig, error) {
	reconnect := c.Int("serf-reconnect-timeout")
	leaveDelay := c.Int("serf-leave-delay")
	cfg := &serf.SerfConfig{
		CacheAddr: storeConf.CacheAddr,
		CachePort: storeConf.CachePort,
		BindAddr:  storeConf.CacheIP,
		BindPort:  c.Int("serf-port"),
		JoinAddr:  c.String("serf-join"),
		JoinCmd:   c.String("serf-memberlist-cmd"),
		MaxRetry:  uint32(c.Int("serf-maxretry")),
		Group:     c.String("serf-group"),
		RdmaGroup: storeConf.RdmaGroup,
		NodeName:  c.String("serf-node-name"),
		HashRing:  storeConf.HashringEnabled(),
		NodeCount: c.Int("virtual-node-count"),
	}
	if reconnect < 0 {
		return nil, errors.New("--serf-reconnect-timeout must be >= 0")
	}
	if leaveDelay < 0 {
		return nil, errors.New("--serf-leave-delay must be >= 0")
	}
	if cfg.NodeCount <= 0 {
		return nil, errors.New("--virtual-node-count must be > 0")
	}
	cfg.Reconnect = time.Duration(reconnect) * time.Second
	cfg.LeaveDelay = time.Duration(leaveDelay) * time.Second
	if c.Bool("serf-initnode") {
		cfg.Role = serf.SerfRoleInit
	}
	gracePeriod += leaveDelay
	return cfg, nil
}

func configureCacheSize(c *cli.Context, cfg *chunk.StoreConfig) error {
	cacheSizeMiB := int64(c.Int("cache-size"))
	if cacheSizeMiB < 0 {
		return fmt.Errorf("Invalid cache size: %dMiB", cacheSizeMiB)
	}
	burstSizeMiB := int64(c.Int("burst-cache-size"))
	if burstSizeMiB < 0 {
		return fmt.Errorf("Invalid burst cache size: %dMiB", burstSizeMiB)
	}
	if burstSizeMiB > 0 && burstSizeMiB < cacheSizeMiB {
		logger.Warnf("--burst-cache-size (%dMiB) is smaller than --cache-size (%dMiB), burst cache disabled",
			burstSizeMiB, cacheSizeMiB)
	}
	cfg.CacheSize = cacheSizeMiB << 20
	cfg.DeferBurst = c.Bool("defer-burst")
	cfg.BurstCacheSize, cfg.InitialBurstSize = chunk.GenBurstSizes(cfg.CacheSize, burstSizeMiB<<20, cfg.DeferBurst)
	return nil
}

func genStoreConf(c *cli.Context, cacheDir string) (*chunk.StoreConfig, error) {
	cfg := &chunk.StoreConfig{
		CacheDir:           cacheDir,
		BufferSize:         int64(c.Int("writeback-buffer")),
		WritebackWorker:    c.Int("writeback-worker"),
		ThpPolicy:          strings.ToLower(c.String("thp-policy")),
		FreeSpace:          float32(c.Float64("free-space-ratio")),
		BlockSize:          uint32(c.Int("block-size")),
		UploadLimit:        c.Int64("upload-limit") * transfer.MbtoB,
		DownloadLimit:      c.Int64("download-limit") * transfer.MbtoB,
		TotalLimit:         c.Int64("total-limit") * transfer.MbtoB,
		SingleLimit:        c.Int64("single-nic-limit") * transfer.MbtoB,
		SerfGroup:          c.String("serf-group"),
		Cleanup:            c.Bool("cleanup-cache"),
		PeerTimeout:        c.Int("peer-timeout"),
		BlacklistTime:      c.Int("blacklist-time"),
		ExchangeTime:       c.Int("exchange-interval"),
		CacheEviction:      chunk.CleanupMode(c.String("cache-eviction")),
		BlockLRUSize:       max(0, c.Int("prefetch")),
		CacheExpire:        utils.Duration(c.String("cache-expire")),
		Hasher:             c.String("checksum"),
		Transport:          strings.ToLower(c.String("transport")),
		RdmaBufSize:        c.Int("rdma-buffer-size"),
		RdmaBufCount:       c.Int("rdma-buffer-count"),
		RdmaMaxMem:         uint64(c.Int("rdma-max-mem")),
		RdmaFallbackTcp:    c.Bool("rdma-fallback-tcp"),
		RdmaGroup:          strings.TrimSpace(c.String("rdma-group")),
		CacheRootPort:      c.Int("cacheroot-port"),
		CacheRootAddrs:     strings.Fields(c.String("cacheroot-tracker")),
		CacheRootXprt:      strings.ToLower(c.String("cacheroot-transport")),
		KeyPrefix:          c.String("key-prefix"),
		SourceID:           c.String("source-id"),
		CacheReadDirectIO:  c.Bool("cache-read-direct-io"),
		CacheWriteDirectIO: c.Bool("cache-write-direct-io"),
		GoGc: utils.GoGcConfig{
			GCPercent:    c.Int("go-gc"),
			GCPercentSet: c.IsSet("go-gc"),
		},
	}
	if cfg.RdmaGroup != "" {
		if !transfer.IsRdmaTransport(cfg.Transport) {
			logger.Warnf("--rdma-group %q is ignored for %s transport", cfg.RdmaGroup, cfg.Transport)
			cfg.RdmaGroup = ""
		} else if !cfg.RdmaFallbackTcp {
			logger.Warnf("--rdma-group %q is set without --rdma-fallback-tcp; enabling TCP fallback automatically", cfg.RdmaGroup)
			cfg.RdmaFallbackTcp = true
		}
	}
	if c.IsSet("go-memlimit") {
		if mib := c.Int64("go-memlimit"); mib <= 0 || mib > math.MaxInt64>>20 {
			logger.Warnf("Invalid --go-memlimit %d, ignored", mib)
		} else {
			cfg.GoGc.MemLimit = mib << 20
		}
	}
	if strings.Contains(cfg.SourceID, "/") {
		return nil, fmt.Errorf("Source ID \"%s\" must not contain \"/\"", cfg.SourceID)
	}
	if cfg.PeerTimeout < 0 {
		logger.Fatal("--peer-timeout must be >= 0")
	}
	if cfg.BlacklistTime < 0 {
		logger.Fatal("--blacklist-time must be >= 0")
	}
	if cfg.ExchangeTime < 0 {
		cfg.ExchangeTime = 0
	}

	bs := cfg.BlockSize
	if bs < 4 || bs > 128 || !utils.IsPowerOfTwo(int(bs)) {
		return nil, errors.New("Only support block size in 4/16/32/64/128 MiB")
	}

	if err := configureCacheSize(c, cfg); err != nil {
		return nil, err
	}
	cfg.BlockSize = cfg.BlockSize << 20
	cfg.BufferSize = cfg.BufferSize << 20

	var err error
	cacheNic := c.String("cache-nic")
	if cacheNic == "" {
		cfg.CacheIP, err = utils.GetDefaultIp()
	} else {
		cfg.CacheIP, err = utils.GetInterfaceIp(cacheNic)
	}
	if err != nil {
		return nil, fmt.Errorf("Get IP of cache server: %s", err)
	}

	cfg.TransportNics = strings.FieldsFunc(c.String("transport-nic"), func(r rune) bool { return unicode.IsSpace(r) || r == ',' })
	cfg.CacheRootNics = strings.FieldsFunc(c.String("cacheroot-nic"), func(r rune) bool { return unicode.IsSpace(r) || r == ',' })

	port := c.Int("cache-port")
	cfg.CacheAddr = fmt.Sprintf("%s:%d", cfg.CacheIP, port)
	cfg.CachePort = port

	// Validate RDMA related options
	if cfg.RdmaEnabled() {
		if !utils.IsPowerOfTwo(cfg.RdmaBufSize) {
			return nil, errors.New("RDMA buffer size must be power of 2")
		}
		if c.Int("rdma-max-mem") <= 0 {
			return nil, errors.New("Max RDMA buffer memory usage must be > 0")
		}
		if cfg.RdmaBufSize < 128 {
			logger.Warnf("RDMA buffer size is too small, may hit performance: %dKiB", cfg.RdmaBufSize)
		}
		cfg.RdmaBufSize = cfg.RdmaBufSize << 10
		cfg.RdmaMaxMem = cfg.RdmaMaxMem << 20
		if cfg.RdmaBufCount <= 0 {
			return nil, fmt.Errorf("Invalid RDMA buffer count: %d", cfg.RdmaBufCount)
		}
		if cfg.RdmaBufCount > 120 {
			// coro-rpc has bug that load will hang if buffer count >= 128, limit it
			// below 120 for now.
			logger.Warnf("RDMA buffer count %d is too large, should be <= 120, reset to 120", cfg.RdmaBufCount)
			cfg.RdmaBufCount = 120
		}
		if uint64(cfg.RdmaBufSize)*uint64(cfg.RdmaBufCount) >= uint64(cfg.RdmaMaxMem) {
			return nil, errors.New("RDMA max memory usage is too small to support one connection")
		}
	}

	if len(cfg.CacheRootAddrs) > 0 {
		for i, addr := range cfg.CacheRootAddrs {
			_, _, err := net.SplitHostPort(addr)
			if err != nil {
				if strings.Contains(err.Error(), "missing port in address") {
					cfg.CacheRootAddrs[i] = fmt.Sprintf("%s:%d", addr, defCacherootTrackerPort)
				} else {
					return nil, fmt.Errorf("Cache root tracker address: %s", err)
				}
			}
		}
	}

	return cfg, nil
}

func genConf(c *cli.Context, metaConf *meta.MetaConfig, storeConf *chunk.StoreConfig) *vfs.Config {
	cfg := &vfs.Config{
		Meta:            metaConf,
		Store:           storeConf,
		BlockPoolSize:   int64(c.Int("buffer-size")) << 20,
		BufferReuse:     c.Bool("buffer-reuse"),
		BufferPrefault:  c.Bool("buffer-prefault"),
		BufferTimeout:   c.Int("buffer-timeout"),
		Prefetch:        uint32(c.Int("prefetch")),
		PrefetchWorkers: uint32(c.Int("prefetch-workers")),
		Preload:         strings.Fields(c.String("preload")), // only support paths w/o spaces
		PreloadWorkers:  uint32(c.Int("preload-workers")),
		PreloadInterval: c.Int("preload-interval"),
		ForcePreload:    c.Bool("force-preload"),
		PinPath: vfs.PinPathConfig{
			Paths:  c.String("pin-cache"),
			File:   c.String("pin-cache-file"),
			Cmd:    c.String("pin-cache-cmd"),
			Reload: c.Int("pin-cache-reload"),
		},
		ApiSocket:      c.String("api-server-sock"),
		DaemonSocket:   c.String("socket"),
		SafeCacheKey:   !c.Bool("unsafe-cache-key"),
		SourceDirectIO: c.Bool("source-direct-io"),
	}
	if cfg.BlockPoolSize < 0 {
		cfg.BlockPoolSize = 0
	}
	// A negative timeout would silently mean "never reap", i.e. hold the whole pool
	// resident forever; 0 says that explicitly.
	if cfg.BufferTimeout < 0 {
		cfg.BufferTimeout = 0
	}
	if !c.Bool("enable-api-server") {
		cfg.ApiSocket = ""
	}

	// For simplicity, set fuse AttrTimeout, EntryTimeout, DirEntryTimeout and NegativeTimeout
	// all to ReaddirTimeout by default
	dirtime := time.Duration(fuse.DefTimeout) * time.Second
	if metaConf.ReaddirTimeout >= 0 {
		dirtime = time.Duration(metaConf.ReaddirTimeout) * time.Second
	}
	cfg.AttrTimeout = dirtime
	cfg.EntryTimeout = dirtime
	cfg.DirEntryTimeout = dirtime
	cfg.NegativeTimeout = dirtime

	// If negative timeout is set explicitly, use it
	if c.IsSet("negative-timeout") {
		negativeTimeout := c.Int("negative-timeout")
		if negativeTimeout < 0 {
			negativeTimeout = fuse.DefTimeout
		}
		cfg.NegativeTimeout = time.Duration(negativeTimeout) * time.Second
	}

	if cfg.ApiSocket != "" {
		// share-reserve protects burst cache headroom for regular read traffic (fuse reads,
		// prefetch, preload, peer fills) from the mmap API (GetFileChunks zero-copy), whose
		// clients lock cached blocks resident for their mapping lifetime. The mmap API's
		// admission cap is burstCap - shareReserve. Unset defaults to prefetch x block size
		// (the typical in-flight prefetch window); explicitly 0 lets mmap clients use the
		// full burst budget; a value >= burstCap would admit nothing and wedge every
		// GetFileChunks forever, so reject it at startup when the API server is enabled.
		//
		// Note that storeConf.BurstCacheSize is already the effective burst size, we can
		// use it directly.
		burstCap := storeConf.BurstCacheSize
		if c.IsSet("share-reserve") {
			reserve := c.Int("share-reserve")
			if reserve < 0 {
				logger.Fatalf("share-reserve (%dMiB) must not be negative", reserve)
			}
			cfg.ShareReserve = int64(reserve) << 20
			if cfg.ShareReserve >= burstCap {
				logger.Fatalf("share-reserve (%dMiB) must be smaller than the burst cache size in effect (%dMiB)",
					cfg.ShareReserve>>20, burstCap>>20)
			}
		} else {
			cfg.ShareReserve = int64(cfg.Prefetch) * int64(storeConf.BlockSize)
			if cfg.ShareReserve >= burstCap {
				logger.Warnf("Default share-reserve (%dMiB) >= burst cache size in effect (%dMiB), reset to 0, use --share-reserve to protect regular read traffic",
					cfg.ShareReserve>>20, burstCap>>20)
				cfg.ShareReserve = 0
			} else {
				logger.Infof("Default share-reserve to prefetch x block size: %dMiB", cfg.ShareReserve>>20)
			}
		}
	}

	// If prefetch-workers is not set, use Prefetch workers
	if !c.IsSet("prefetch-workers") || cfg.PrefetchWorkers == 0 {
		cfg.PrefetchWorkers = cfg.Prefetch
	}

	// If we're initial source dir source, disable preload, as source dir
	// is not ready on startup
	if metaConf.MetaProvider() && len(cfg.Preload) > 0 {
		logger.Warn("Disable preload on startup in sourceless meta provider mode")
		cfg.Preload = nil
	}
	return cfg
}

func genRpcConf(storeConf *chunk.StoreConfig) *rpcsvc.RpcConfig {
	return &rpcsvc.RpcConfig{
		Addr: storeConf.CacheAddr,
	}
}

func cache_main(cfs *vfs.Cachefs, c *cli.Context) {
	if os.Getuid() == 0 && os.Getpid() != 1 {
		disableUpdatedb()
	}

	logger.Infof("Mounting volume %s at %s ...", cfs.Conf.Meta.Name, cfs.Conf.Meta.MountPoint)
	err := cfs.Meta.Init()
	if err != nil {
		logger.Fatalf("Init meta: %s", err)
	}
	err = fuse.ServeCachefs(cfs, c.String("o"))
	if err != nil {
		logger.Fatalf("fuse: %s", err)
	}
}

func validateCacheConfig(metaConf *meta.MetaConfig, cachefsConf *vfs.Config) error {
	if len(metaConf.SourceDirs) > 1 {
		if metaConf.Sourceless {
			return errors.New("Sourceless mode doesn't support multiple source dirs.")
		}
	}
	if metaConf.Sourceless {
		if cachefsConf.DaemonSocket != "" {
			return errors.New("Sourceless mode doesn't support Fuse/Store separation.")
		}
	}
	if metaConf.PeerMeta() && cachefsConf.DaemonSocket != "" {
		return errors.New("Peer meta mode requires serf and rpc services which are only available " +
			"in standalone mode, not supported with --socket (Fuse/Store separation).")
	}
	return nil
}

func cache(c *cli.Context) error {
	logger.Infof("Run CacheFS version %s", c.App.Version)
	setup(c, 2)
	fsname := c.Args().Get(0)
	mp := c.Args().Get(1)

	registerer, registry := metric.InitRegister("cachefs_", mp, fsname)

	sourceDirs, cacheDir := setupDirectories(c, mp)

	// Create all required configs
	storeConf, err := genStoreConf(c, cacheDir)
	if err != nil {
		logger.Fatal(err.Error())
	}
	metaConf, err := genMetaConf(c, fsname, mp, sourceDirs, storeConf.CacheSize)
	if err != nil {
		logger.Fatal(err.Error())
	}
	cachefsConf := genConf(c, metaConf, storeConf)

	// Check if config combinations are supported
	if err := validateCacheConfig(metaConf, cachefsConf); err != nil {
		logger.Fatal(err.Error())
	}
	metaConf.SourceManager = srcmgr.NewSourceManager(metaConf.SourceDirs, registerer)
	if cachefsConf.ApiSocket != "" {
		// GetFileChunks API requires memfd to share memory to clients
		chunk.PageMemfd = true
	}

	// Create serf service and cachefs store only in cachefs standalone mode, not in fuse mode
	var store chunk.Store
	var serfService serf.SerfService
	var rpcService rpcsvc.RpcService
	var cacherootRpcSvc rpcsvc.RpcService
	if !cachefsConf.IsFuseLayerMode() {
		serfConf, err := genSerfConf(c, storeConf)
		if err != nil {
			logger.Fatal(err.Error())
		}
		rpcConf := genRpcConf(storeConf)

		serfService = serf.NewSerfService(serfConf, registerer)
		rpcService, err = rpcsvc.NewRpcService(rpcConf)
		if err != nil {
			logger.Fatal(err.Error())
		}

		// Create a dedicated rpcsvc for the cache root cluster port, shared by the
		// CachefsCacheRoot (Query) service registered by the store and the
		// CacheRootCluster service registered by vfs. Only when cacheroot server
		// is enabled.
		if storeConf.CacheRootPort > 0 {
			addr := net.JoinHostPort("0.0.0.0", fmt.Sprintf("%d", storeConf.CacheRootPort))
			cacherootConf := &rpcsvc.RpcConfig{Addr: addr}
			cacherootRpcSvc, err = rpcsvc.NewRpcService(cacherootConf)
			if err != nil {
				logger.Fatal(err.Error())
			}
		}

		store = chunk.NewStore(storeConf, rpcService, cacherootRpcSvc, serfService, registerer)
	}

	// Create cachefs meta
	meta := meta.NewMetaData(metaConf, rpcService, serfService, registerer)

	if c.Bool("background") && os.Getenv("CFS_FOREGROUND") == "" {
		runDaemon(c, metaConf)
	} else {
		go func() {
			checkMountpoint(fsname, mp, c.String("log"), false)
			liveAddr := c.String("liveness")
			liveSet := c.IsSet("liveness")
			if err := utils.LivenessProbe(liveAddr, !liveSet); err != nil {
				logger.Fatal(err.Error())
			}
		}()
	}

	// Create cachefs
	installHandler(mp)
	cfs := vfs.NewCachefs(cachefsConf, meta, store, registerer, registry, serfService, rpcService, cacherootRpcSvc)
	initMetrics(c, registry, serfService)

	// Start RPC service before serf service, to make local service ready before
	// broadcasting states to cluster.
	if rpcService != nil {
		rpcService.Serve()
	}
	if cacherootRpcSvc != nil {
		cacherootRpcSvc.Serve()
	}

	// Start serf service after all users registered themselves to serf service
	if serfService != nil {
		if err := serfService.Start(); err != nil {
			logger.Fatalf("Start serf service: %s", err)
		}
	}

	// Start fuse service loop
	cache_main(cfs, c)
	cfs.Stop()

	meta.Stop()
	if store != nil {
		store.Stop()
	}
	if rpcService != nil {
		rpcService.Stop()
	}
	if cacherootRpcSvc != nil {
		cacherootRpcSvc.Stop()
	}
	if serfService != nil {
		serfService.Stop()
	}
	return nil
}

func setupDirectories(c *cli.Context, mp string) ([]string, string) {
	prepareMp(mp)
	sourceDirs, err := processSourceDirs(c)
	if err != nil {
		logger.Fatalf("Process source-dir: %s", err)
	}
	// For cache directories, try to create the directory if it does not exist.
	cacheDir, isCacheDirNew, err := processPath(c, "cache-dir", true)
	if err != nil {
		logger.Fatalf("Process cache-dir: %s", err)
	}

	cleanupNewCacheDir := func() {
		if isCacheDirNew {
			if err := os.Remove(cacheDir); err != nil {
				logger.Warnf("Failed to clean up cache directory: %s", err)
			}
		}
	}

	if len(sourceDirs) == 0 {
		// Only sourceless consumer mode legitimately has no source dir;
		// processSourceDirs already rejected the missing flag otherwise.
		return nil, cacheDir
	}

	if cacheDir != "memory" && !checkPathDependency(mp, cacheDir) {
		cleanupNewCacheDir()
		logger.Fatalf("The mount point: %s and cache-dir: %s must not be the same or have a parent/child relationship", mp, cacheDir)
	}

	for _, sourceDir := range sourceDirs {
		if cacheDir != "memory" {
			if !checkPathDependency(sourceDir, cacheDir) {
				cleanupNewCacheDir()
				logger.Fatalf("The source-dir: %s and cache-dir: %s must not be the same or have a parent/child relationship", sourceDir, cacheDir)
			}
		}

		if !checkPathDependency(sourceDir, mp) {
			cleanupNewCacheDir()
			logger.Fatalf("The source-dir: %s and mountpoint: %s must not be the same or have a parent/child relationship", sourceDir, mp)
		}
	}

	return sourceDirs, cacheDir
}

// processSourceDirs resolves the source dirs from --source-dir (exactly one
// dir, taken literally) and --extra-source-dirs (optional fallback replicas
// separated by spaces and/or commas) into a single priority-ordered slice
// with --source-dir first. Extra dirs require --source-dir, in sourceless
// mode too. --source-dir is validated strictly like processPath validates a
// single source dir, but an extra dir failing validation is dropped with a
// warning instead: the replica backends are exactly what the fallback list
// is meant to tolerate being down. Duplicates are dropped (keeping the
// first occurrence) with a warning listing the duplicated paths.
func processSourceDirs(c *cli.Context) ([]string, error) {
	sourceDir := c.String("source-dir")
	extraDirs := strings.FieldsFunc(c.String("extra-source-dirs"), func(r rune) bool {
		return r == ',' || unicode.IsSpace(r)
	})
	if sourceDir == "" {
		if len(extraDirs) > 0 {
			return nil, fmt.Errorf("--extra-source-dirs requires --source-dir")
		}
		if !c.Bool("sourceless") {
			return nil, fmt.Errorf("Missing --source-dir")
		}
		return nil, nil
	}
	resolve := func(dir string) (string, error) {
		fi, err := os.Stat(dir)
		if err != nil {
			return "", fmt.Errorf("stat source dir %s: %w", dir, err)
		}
		if !fi.IsDir() {
			return "", fmt.Errorf("source-dir %s is not a directory", dir)
		}
		if ad, err := filepath.Abs(dir); err != nil {
			logger.Warnf("Find absolute path of source-dir %s: %s", dir, err)
		} else {
			dir = ad
		}
		if sd, err := filepath.EvalSymlinks(dir); err != nil {
			logger.Warnf("Eval symlinks of source-dir %s: %s", dir, err)
		} else {
			dir = sd
		}
		return dir, nil
	}
	primary, err := resolve(sourceDir)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{primary: {}}
	keep := []string{primary}
	for _, dir := range extraDirs {
		dir, err := resolve(dir)
		if err != nil {
			logger.Warnf("Ignoring extra source dir: %s", err)
			continue
		}
		if _, ok := seen[dir]; ok {
			logger.Warnf("Duplicate source dir ignored: %s", dir)
			continue
		}
		seen[dir] = struct{}{}
		keep = append(keep, dir)
	}
	// Replicas must be independent trees: a nested replica shows up as
	// ordinary content inside its parent replica, and its backend likely
	// fails together with the parent's.
	for i := 0; i < len(keep); i++ {
		for j := i + 1; j < len(keep); j++ {
			if !checkPathDependency(keep[i], keep[j]) {
				return nil, fmt.Errorf("source dirs %s and %s must not be the same or have a parent/child relationship", keep[i], keep[j])
			}
		}
	}
	return keep, nil
}

// processPath processes the specified path. If the path is a cacheDir and does not exist,
// it will attempt to create it.
// It returns the absolute path and a flag indicating if the directory was newly created.
func processPath(c *cli.Context, pathConfigName string, required bool) (string, bool, error) {
	path := c.String(pathConfigName)
	if path == "" {
		if required {
			return "", false, fmt.Errorf("Missing --%s", pathConfigName)
		} else {
			return "", false, nil
		}
	}

	createdNew := false

	if pathConfigName == "cache-dir" && path == "memory" {
		return path, createdNew, nil
	}

	fi, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		if pathConfigName == "cache-dir" {
			if err := os.MkdirAll(path, 0700); err != nil {
				return "", false, err
			}
			createdNew = true
			return path, createdNew, nil
		}
		return "", false, err
	}

	if !fi.IsDir() {
		return "", false, fmt.Errorf("%s %s is not a directory", pathConfigName, path)
	}

	if ad, err := filepath.Abs(path); err != nil {
		logger.Warnf("Find absolute path of %s %s: %s", pathConfigName, path, err)
	} else {
		path = ad
	}

	if sd, err := filepath.EvalSymlinks(path); err != nil {
		logger.Warnf("Eval symlinks of %s %s: %s", pathConfigName, path, err)
	} else {
		path = sd
	}

	return path, createdNew, nil
}

// checkPathDependency checks whether two paths are independent, neither equal nor parent/child directories.
// Returns true if the two paths are independent, false if there's a dependency relationship (equal or parent/child directories).
func checkPathDependency(path1, path2 string) bool {
	// Check if paths are exactly equal
	if path1 == path2 {
		return false
	}
	// Check if path1 is a subdirectory of path2 or vice versa
	if isSubdirectory(path1, path2) || isSubdirectory(path2, path1) {
		return false
	}
	return true
}

// isSubdirectory checks if the first path is a subdirectory of the second path.
// This is achieved by checking the relative path obtained from filepath.Rel(parent, child).
func isSubdirectory(child, parent string) bool {
	// relPath is the relative path from parent to child.
	relPath, err := filepath.Rel(parent, child)
	if err != nil {
		logger.Warnf("Unable to calculate relative path between %s and %s: %v", child, parent, err)
		return false
	}

	// The following conditions ensure that child is indeed a subdirectory of parent:
	// 1. The relative path must not be an absolute path, this guarantees that parent and child share a common root, not being two independent paths.
	// 2. The relative path must not be "..", indicating that child is not the immediate parent directory.
	// 3. The relative path must not start with "..", indicating that child is not somewhere above parent in the directory hierarchy.
	return !filepath.IsAbs(relPath) && relPath != ".." && !strings.HasPrefix(relPath, ".."+string(filepath.Separator))
}
