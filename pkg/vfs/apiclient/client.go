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

// Package apiclient provides a lightweight client for the CacheFS VFS API server. It has no CGO
// dependencies and can be used by tools that do not require the full vfs package.
package apiclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	pb "github.com/aliyun/cachefs/pkg/ttrpc/cachefs"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/containerd/ttrpc"
	"golang.org/x/sync/semaphore"
)

// DefAPIServerSock is the default Unix socket path for the CacheFS API server.
const DefAPIServerSock = "/run/cachefs/cachefs-api.sock"

var (
	logger = utils.GetLogger("cachefs")

	errServerInfoUnavailable = errors.New("server information is unavailable")
)

// CacheLimiter bounds the total bytes of file content that may be concurrently
// materialized in the CacheFS daemon's cache (memory or disk burst cache) via
// mmap requests from one or more ApiClients. It is meant to be created once per
// process and shared across every ApiClient served by the same daemon, so their
// aggregate reserved bytes never exceed the limit (typically the daemon's burst
// cache size). It is safe for concurrent use.
type CacheLimiter struct {
	sem *semaphore.Weighted
	cap int64
}

// NewCacheLimiter creates a shared cache limiter with the given byte capacity.
// limitBytes must be > 0; a single mmap request larger than the capacity is
// rejected rather than blocked forever.
func NewCacheLimiter(limitBytes int64) *CacheLimiter {
	return &CacheLimiter{sem: semaphore.NewWeighted(limitBytes), cap: limitBytes}
}

// Cap returns the limiter's byte capacity.
func (l *CacheLimiter) Cap() int64 { return l.cap }

// mappedRegion tracks a live mmap and the exact number of limiter bytes it
// reserved, so MunmapFile/Close can release precisely what was acquired. data
// is the exposed view; base is the complete selected-block mapping that must be
// unmapped. It also records the daemon requestId (to send ReleaseFileChunks at
// munmap) and the per-request fd-transfer connection kept open for the whole
// mapping lifetime: the daemon uses its EOF as the crash-safety unlock fallback.
type mappedRegion struct {
	data      []byte
	base      []byte
	reserved  int64
	requestId uint32
	conn      net.Conn
}

// ServerInfo is a snapshot of daemon server information, such as PID, block size,
// and cache configuration.
//
// ShareCap is the number of bytes GetFileChunks may lock (burst cache size minus the
// daemon's share reserve). 0 means nothing can be admitted.
type ServerInfo struct {
	BlockSize      uint32
	CacheSize      int64
	BurstCacheSize int64
	Pid            int64
	ShareCap       int64
}

// ApiClient provides a client interface to interact with the CacheFS VFS API
// server over a Unix socket using ttrpc. It also supports zero-copy mmap-based
// file access via the fd-transfer socket.
type ApiClient struct {
	SockPath   string
	FdSockPath string
	TtrpcSvc   pb.CachefsApiService
	TtrpcConn  *ttrpc.Client

	// limiter, when non-nil, bounds concurrent mmap'd bytes. It may be shared
	// across multiple ApiClients in the same process; Close never destroys it.
	limiter *CacheLimiter

	serverInfoMu sync.RWMutex
	serverInfo   *ServerInfo

	mappingMu sync.Mutex
	mappings  map[uintptr]*mappedRegion // track mmap'd regions and their reserved bytes
	closed    bool                      // Close ran: the drained map accepts no new entries
	fdMu      sync.Mutex                // serialize fd transfer sessions
}

func newConn(sock string) (pb.CachefsApiService, *ttrpc.Client, error) {
	conn, err := net.DialTimeout("unix", sock, 2*time.Second)
	if err != nil {
		return nil, nil, err
	}
	c := ttrpc.NewClient(conn)
	return pb.NewCachefsApiClient(c), c, nil
}

// NewApiClient creates a new CacheFS API client connected to the specified Unix socket. It probes
// server information once to cache a snapshot for later use, and the probe is best-effort.
func NewApiClient(sockPath string) (*ApiClient, error) {
	svc, conn, err := newConn(sockPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CacheFS API server: %s", err)
	}
	client := &ApiClient{
		SockPath:   sockPath,
		FdSockPath: sockPath + ".fd",
		TtrpcSvc:   svc,
		TtrpcConn:  conn,
		mappings:   make(map[uintptr]*mappedRegion),
	}
	if _, err := client.GetServerInfo(); err != nil {
		logger.Warnf("NewApiClient: server information probe for %s failed: %s", sockPath, err)
	}
	return client, nil
}

// NewApiClientWithLimiter creates an API client that charges its GetFileChunks bytes against the
// given shared CacheLimiter. Pass the same *CacheLimiter to every client in a process served by the
// same daemon to bound their aggregate reserved cache bytes. A nil limiter disables limiting (same
// as NewApiClient).
func NewApiClientWithLimiter(sockPath string, limiter *CacheLimiter) (*ApiClient, error) {
	c, err := NewApiClient(sockPath)
	if err != nil {
		return nil, err
	}
	c.limiter = limiter
	return c, nil
}

// GetServerInfo queries a snapshot of daemon server information, such as block
// size and cache configuration.
func (c *ApiClient) GetServerInfo() (ServerInfo, error) {
	resp, err := c.TtrpcSvc.GetServerInfo(context.Background(), &pb.GetServerInfoRequest{})
	if err != nil {
		return ServerInfo{}, err
	}
	info := ServerInfo{
		BlockSize:      resp.GetBlockSize(),
		CacheSize:      resp.GetCacheSize(),
		BurstCacheSize: resp.GetBurstCacheSize(),
		Pid:            resp.GetPid(),
		ShareCap:       resp.GetShareCap(),
	}
	c.serverInfoMu.Lock()
	c.serverInfo = &info
	c.serverInfoMu.Unlock()
	return info, nil
}

func (c *ApiClient) cachedServerInfo() (ServerInfo, error) {
	c.serverInfoMu.RLock()
	defer c.serverInfoMu.RUnlock()
	if c.serverInfo == nil {
		return ServerInfo{}, errServerInfoUnavailable
	}
	return *c.serverInfo, nil
}

// getLimitBytes picks the auto limiter capacity from the daemon's share cap. 0 means no limiter.
func getLimitBytes(info ServerInfo) int64 {
	if info.ShareCap > 0 {
		return info.ShareCap
	}
	return 0
}

// NewApiClientAutoLimit connects to sockPath and installs a CacheLimiter sized to the daemon's
// cached share cap, so this client's concurrent GetFileChunks bytes never exceed what the daemon
// can admit. It is a convenience wrapper that uses the construction-time probe for the common
// single-client case.
//
// If the daemon reports no share cap (e.g. daemon mode) the returned client is unlimited; when the
// probe did not produce server information, it returns an error. To share one limiter across
// several clients, use NewAutoCacheLimiter together with NewApiClientWithLimiter instead.
func NewApiClientAutoLimit(sockPath string) (*ApiClient, error) {
	c, err := NewApiClient(sockPath)
	if err != nil {
		return nil, err
	}
	info, err := c.cachedServerInfo()
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	if limit := getLimitBytes(info); limit > 0 {
		logger.Infof("ApiClient: cache limiter sized to daemon share cap %d bytes", limit)
		c.limiter = NewCacheLimiter(limit)
	}
	return c, nil
}

// NewAutoCacheLimiter returns a CacheLimiter sized like NewApiClientAutoLimit, ready to be shared
// across multiple ApiClients via NewApiClientWithLimiter. It returns a nil limiter (and no error)
// when the daemon reports no share cap, which NewApiClientWithLimiter treats as unlimited; it
// returns an error when the construction-time probe did not produce server information.
func NewAutoCacheLimiter(sockPath string) (*CacheLimiter, error) {
	c, err := NewApiClient(sockPath)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	info, err := c.cachedServerInfo()
	if err != nil {
		return nil, err
	}
	limit := getLimitBytes(info)
	if limit == 0 {
		return nil, nil
	}
	logger.Infof("ApiClient: cache limiter sized to daemon share cap %d bytes", limit)
	return NewCacheLimiter(limit), nil
}

// CacheLimit returns the byte capacity of this client's cache limiter, or 0 when
// the client has no limiter (unlimited).
func (c *ApiClient) CacheLimit() int64 {
	if c.limiter == nil {
		return 0
	}
	return c.limiter.Cap()
}

// acquireCache reserves n bytes from the limiter before materializing a file.
// It is a no-op when no limiter is set or n <= 0. A request larger than the
// limiter capacity is rejected (semaphore.Acquire would otherwise block forever).
func (c *ApiClient) acquireCache(ctx context.Context, n int64) error {
	if c.limiter == nil || n <= 0 {
		return nil
	}
	if n > c.limiter.cap {
		return fmt.Errorf("file size %d exceeds cache limit %d", n, c.limiter.cap)
	}
	if err := c.limiter.sem.Acquire(ctx, n); err != nil {
		return fmt.Errorf("acquire cache limit: %w", err)
	}
	return nil
}

// releaseCache returns n previously reserved bytes to the limiter.
func (c *ApiClient) releaseCache(n int64) {
	if c.limiter != nil && n > 0 {
		c.limiter.sem.Release(n)
	}
}

// Close unmaps all tracked file mappings and closes the underlying ttrpc connection.
func (c *ApiClient) Close() error {
	c.mappingMu.Lock()
	if c.closed {
		c.mappingMu.Unlock()
		return nil
	}
	c.closed = true
	mappings := c.mappings
	c.mappings = make(map[uintptr]*mappedRegion)
	c.mappingMu.Unlock()
	for _, r := range mappings {
		if len(r.base) > 0 {
			_ = munmapSlice(r.base)
		}
		c.releaseFileChunks(r)
		// Return reserved budget to the (possibly shared) limiter, but never
		// destroy the limiter itself: other clients may still be using it.
		c.releaseCache(r.reserved)
	}
	if c.TtrpcConn != nil {
		err := c.TtrpcConn.Close()
		c.TtrpcConn = nil
		return err
	}
	return nil
}

// GetPid retrieves the process ID of the CacheFS daemon.
func (c *ApiClient) GetPid() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.TtrpcSvc.GetPid(ctx, &pb.GetPidRequest{})
	if err != nil {
		return 0, err
	}
	return resp.GetPid(), nil
}

// GetPeers retrieves the list of peer addresses in the serf cluster.
// If raw is true, returns live members; otherwise returns the cached list.
// If port is true, addresses include port information.
func (c *ApiClient) GetPeers(raw bool, port bool) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.TtrpcSvc.GetPeers(ctx, &pb.GetPeersRequest{Raw: raw, Port: port})
	if err != nil {
		return nil, err
	}
	return resp.GetPeers(), nil
}

// Unmount requests the CacheFS daemon to unmount the filesystem.
func (c *ApiClient) Unmount(name string) error {
	req := &pb.UnmountRequest{
		Name: name,
		Pid:  int64(os.Getpid()),
	}
	_, err := c.TtrpcSvc.Unmount(context.Background(), req)
	return err
}

// LoadCacheOptions holds options for LoadCache operation.
type LoadCacheOptions struct {
	Background bool
	Broadcast  bool
	LoadAll    bool
	Force      bool
	Cancel     bool
}

// LoadCacheOption is a function that configures LoadCacheOptions.
type LoadCacheOption func(*LoadCacheOptions)

// LoadCacheWithBackground sets the background option.
func LoadCacheWithBackground(background bool) LoadCacheOption {
	return func(o *LoadCacheOptions) { o.Background = background }
}

// LoadCacheWithBroadcast sets the broadcast option.
func LoadCacheWithBroadcast(broadcast bool) LoadCacheOption {
	return func(o *LoadCacheOptions) { o.Broadcast = broadcast }
}

// LoadCacheWithLoadAll sets the loadAll option.
func LoadCacheWithLoadAll(loadAll bool) LoadCacheOption {
	return func(o *LoadCacheOptions) { o.LoadAll = loadAll }
}

// LoadCacheWithForce sets the force option.
func LoadCacheWithForce(force bool) LoadCacheOption {
	return func(o *LoadCacheOptions) { o.Force = force }
}

// LoadCacheWithCancel sets the cancel option.
func LoadCacheWithCancel(cancel bool) LoadCacheOption {
	return func(o *LoadCacheOptions) { o.Cancel = cancel }
}

// LoadCache preloads files/directories into cache.
func (c *ApiClient) LoadCache(paths []string, opts ...LoadCacheOption) (uint64, uint64, error) {
	options := &LoadCacheOptions{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pb.LoadCacheRequest{
		Paths:      paths,
		Background: options.Background,
		Broadcast:  options.Broadcast,
		LoadAll:    options.LoadAll,
		Force:      options.Force,
		Cancel:     options.Cancel,
	}
	resp, err := c.TtrpcSvc.LoadCache(context.Background(), req)
	if err != nil {
		return 0, 0, err
	}
	return resp.GetLoadCount(), resp.GetLoadBytes(), nil
}

// ClearCacheOptions holds options for ClearCache operation.
type ClearCacheOptions struct {
	Broadcast  bool
	ClearBurst bool
}

// ClearCacheOption is a function that configures ClearCacheOptions.
type ClearCacheOption func(*ClearCacheOptions)

// ClearCacheWithBroadcast sets the broadcast option.
func ClearCacheWithBroadcast(broadcast bool) ClearCacheOption {
	return func(o *ClearCacheOptions) { o.Broadcast = broadcast }
}

// ClearCacheWithClearBurst sets the clear burst option.
func ClearCacheWithClearBurst(clearBurst bool) ClearCacheOption {
	return func(o *ClearCacheOptions) { o.ClearBurst = clearBurst }
}

// ClearCache evicts cached data for the specified paths.
func (c *ApiClient) ClearCache(paths []string, opts ...ClearCacheOption) (uint64, uint64, error) {
	options := &ClearCacheOptions{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pb.ClearCacheRequest{
		Paths:      paths,
		Broadcast:  options.Broadcast,
		ClearBurst: options.ClearBurst,
	}
	resp, err := c.TtrpcSvc.ClearCache(context.Background(), req)
	if err != nil {
		return 0, 0, err
	}
	return resp.GetClearCount(), resp.GetClearBytes(), nil
}

// MetaReady notifies CacheFS that source dir metadata is fully cached.
func (c *ApiClient) MetaReady() error {
	_, err := c.TtrpcSvc.MetaReady(context.Background(), &pb.MetaReadyRequest{})
	return err
}

// DataReady notifies CacheFS that all data has been read and cached.
func (c *ApiClient) DataReady() error {
	_, err := c.TtrpcSvc.DataReady(context.Background(), &pb.DataReadyRequest{})
	return err
}

// SourceReady notifies CacheFS that source dir data and metadata are ready.
func (c *ApiClient) SourceReady(background bool) (uint64, uint64, error) {
	res, err := c.TtrpcSvc.SourceReady(context.Background(), &pb.SourceReadyRequest{
		Background: background,
	})
	if err != nil {
		return 0, 0, err
	}
	return res.GetLoadCount(), res.GetLoadBytes(), nil
}

// SetCacheSizeOptions holds options for SetCacheSize operation.
type SetCacheSizeOptions struct {
	Broadcast bool
}

// SetCacheSizeOption is a function that configures SetCacheSizeOptions.
type SetCacheSizeOption func(*SetCacheSizeOptions)

// SetCacheSizeWithBroadcast sets the broadcast option.
func SetCacheSizeWithBroadcast(broadcast bool) SetCacheSizeOption {
	return func(o *SetCacheSizeOptions) { o.Broadcast = broadcast }
}

// SetCacheSize dynamically updates cache capacity and burst cache capacity.
// Sizes are in bytes. CacheSizeKeepCurrent means keep current value;
// BurstCacheSizeRestore as burstCacheSize restores the startup-configured burst size.
// Returns new cache size, new burst size, new share cap, evicted block count and bytes.
func (c *ApiClient) SetCacheSize(cacheSize, burstCacheSize int64, opts ...SetCacheSizeOption) (int64, int64, int64, uint64, uint64, error) {
	options := &SetCacheSizeOptions{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pb.SetCacheSizeRequest{
		CacheSize:      cacheSize,
		BurstCacheSize: burstCacheSize,
		Broadcast:      options.Broadcast,
	}
	resp, err := c.TtrpcSvc.SetCacheSize(context.Background(), req)
	if err != nil {
		return -1, -1, -1, 0, 0, err
	}

	// Update local server info snapshot
	c.serverInfoMu.Lock()
	if c.serverInfo == nil {
		c.serverInfo = &ServerInfo{}
	}
	c.serverInfo.CacheSize = resp.GetCacheSize()
	c.serverInfo.BurstCacheSize = resp.GetBurstCacheSize()
	c.serverInfo.ShareCap = resp.GetShareCap()
	c.serverInfoMu.Unlock()
	return resp.GetCacheSize(), resp.GetBurstCacheSize(), resp.GetShareCap(), resp.GetEvictCount(), resp.GetEvictBytes(), nil
}

// GetLoadStats returns the mount's data-loading report: a rendered single-line
// text summary ready to print, plus the same figures as a JSON payload for
// programmatic use (unmarshal into vfs.LoadStats).
func (c *ApiClient) GetLoadStats(log bool) (text string, payload []byte, err error) {
	resp, err := c.TtrpcSvc.GetLoadStats(context.Background(), &pb.GetLoadStatsRequest{Log: log})
	if err != nil {
		return "", nil, err
	}
	return resp.GetText(), resp.GetPayload(), nil
}

// ParseInode parses an inode number from a decimal string.
func ParseInode(inodeStr string) (uint64, error) {
	return strconv.ParseUint(inodeStr, 10, 64)
}
