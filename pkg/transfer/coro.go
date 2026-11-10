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

package transfer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	coro "github.com/aliyun/cachefs/pkg/coro"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/juju/ratelimit"
	"github.com/prometheus/client_golang/prometheus"
)

/*
#include <stdint.h>
*/
import "C"

const (
	defIdleTime       = 10
	defConnectTimeout = 3 // coro-rpc client connect timeout in seconds
	defConnectRetry   = 1 // coro-rpc connect retries on top of the initial attempt

	EnvServerThreads = "CFS_CORO_RPC_SERVER_THREADS"
)

var (
	transferSvcMu sync.RWMutex
	transferSvcs  = make(map[uint32]*coroTransfer)
	transferSvcID = &atomic.Uint32{}

	coroOnce sync.Once

	// getAllRDMADevs is a seam for testing device discovery without RDMA hardware.
	getAllRDMADevs = coro.GetAllRDMADevs
)

type coroTransfer struct {
	config    *TransferConfig
	id        uint32
	rdma      bool
	nextReqID *atomic.Uint32

	// Transport NICs (RDMA devices or IPs) this instance is serving on.
	nics []string

	lisAddrs   []string
	rpcServers []unsafe.Pointer
	serverSeq  atomic.Uint64 // round-robin counter over lisAddrs

	clientMu   sync.Mutex
	clients    map[string]*coroTransferClient
	clientNics []string // client-side source devices, round-robin per target addr
	clientSeq  uint64   // round-robin counter over clientNics (under clientMu)

	// max RDMA buffer count per client, override via RDMA_MAX_BUFFER_COUNT
	rdmaMaxBuffer int

	// coro-rpc server configs
	serverThreads int // worker threads per server, override by CFS_CORO_RPC_SERVER_THREADS

	// coro-rpc client configs
	idleTimeoutSec    int // connection idle timeout in seconds, override by CFS_CORO_RPC_IDLE_TIMEOUT
	connectTimeoutSec int // connect timeout in seconds, override by CFS_CORO_RPC_CONNECT_TIMEOUT
	connectRetryCount int // connect retries in addition to initial attempt, override by CFS_CORO_RPC_CONNECT_RETRY

	buffersMu sync.Mutex
	buffers   map[RequestID]*transferBuffer

	upLimit    *ratelimit.Bucket
	downLimit  *ratelimit.Bucket
	totalLimit *ratelimit.Bucket
}

// resolveTransportLimit picks the transport bandwidth limit based on the number
// of NICs actually in use: single-nic-limit when exactly one NIC is used,
// otherwise total-limit. It warns when single-nic-limit is set but cannot take
// effect (multiple NICs in use and total-limit unset), which leaves the
// transport bandwidth unlimited.
func resolveTransportLimit(cfg *TransferConfig, nicCount int) int64 {
	if cfg.SingleLimit > 0 && nicCount == 1 {
		return cfg.SingleLimit
	}
	if cfg.SingleLimit > 0 && cfg.TotalLimit == 0 {
		logger.Warnf("single-nic-limit %d Mbps is set but not effective: %d transport NICs in use and total-limit is unset, transport bandwidth is unlimited",
			cfg.SingleLimit/MbtoB, nicCount)
	}
	return cfg.TotalLimit
}

func newCoroTransfer(cfg *TransferConfig, reg prometheus.Registerer) (TransferService, error) {
	coroSvc := &coroTransfer{
		config:    cfg,
		rdma:      cfg.RdmaEnabled(),
		nextReqID: &atomic.Uint32{},
		clients:   make(map[string]*coroTransferClient),
		buffers:   make(map[RequestID]*transferBuffer),
	}

	level := strings.ToUpper(logger.Level.String())
	coro.InitLog(level)

	if coroSvc.rdma {
		if err := coroSvc.initRDMA(); err != nil {
			return nil, err
		}
	} else {
		if err := coroSvc.initTCP(); err != nil {
			return nil, err
		}
	}
	initMetrics(reg)

	coroSvc.nics = sortedNics(coroSvc.nics)
	if limit := resolveTransportLimit(cfg, len(coroSvc.nics)); limit > 0 {
		logger.Infof("Total transport bandwidth limit %d Mbps (%d transport NIC: %v)", limit/MbtoB, len(coroSvc.nics), coroSvc.nics)
		coroSvc.totalLimit = ratelimit.NewBucketWithRate(float64(limit), limit)
	}
	if limit := cfg.DownloadLimit; limit > 0 {
		logger.Infof("Download bandwidth limit %d Mbps", limit/MbtoB)
		coroSvc.downLimit = ratelimit.NewBucketWithRate(float64(limit), limit)
	}
	if limit := cfg.UploadLimit; limit > 0 {
		logger.Infof("Upload bandwidth limit %d Mbps", limit/MbtoB)
		coroSvc.upLimit = ratelimit.NewBucketWithRate(float64(limit), limit)
	}

	// require positive values; connect retry allows 0 (no retry).
	coroSvc.rdmaMaxBuffer = utils.GetEnvInt("RDMA_MAX_BUFFER_COUNT", cfg.RdmaBufCount+2, 1)
	coroSvc.idleTimeoutSec = utils.GetEnvInt("CFS_CORO_RPC_IDLE_TIMEOUT", defIdleTime, 1)
	coroSvc.connectTimeoutSec = utils.GetEnvInt("CFS_CORO_RPC_CONNECT_TIMEOUT", defConnectTimeout, 1)
	coroSvc.connectRetryCount = utils.GetEnvInt("CFS_CORO_RPC_CONNECT_RETRY", defConnectRetry, 0)
	coroSvc.serverThreads = utils.GetEnvInt(EnvServerThreads, runtime.NumCPU(), 1)

	go coroSvc.gcClients()

	// Allocate ID when service is ready, to avoid allocating ID for failed initialization.
	svcID := transferSvcID.Add(1)
	coroSvc.id = svcID
	if err := addCoroTransfer(svcID, coroSvc); err != nil {
		return nil, err
	}
	return coroSvc, nil
}

func (ct *coroTransfer) getReqID() RequestID {
	return RequestID(ct.nextReqID.Add(1))
}

// unregisterLocked drops the registration for reqID and returns its release hook,
// if any. The caller must hold ct.buffersMu and run the hook after dropping it:
// the hook can unmap memory and take unrelated locks.
func (ct *coroTransfer) unregisterLocked(reqID RequestID) func() {
	buf, ok := ct.buffers[reqID]
	if !ok {
		return nil
	}
	delete(ct.buffers, reqID)
	return buf.onRelease
}

func (ct *coroTransfer) getBuffer(reqID RequestID) (*transferBuffer, error) {
	ct.buffersMu.Lock()
	defer ct.buffersMu.Unlock()
	buf, ok := ct.buffers[reqID]
	if ok {
		// Claim transfers ownership out of buffers. GC and StopServer cannot
		// reclaim it after this point; the data plane releases it only after
		// WaitResponseFinish returns.
		delete(ct.buffers, reqID)
		return buf, nil
	}
	return nil, fmt.Errorf("transfer buffer not found for request ID %d", reqID)
}

func (ct *coroTransfer) gcBuffers() {
	for {
		now := time.Now()
		var released []func()
		ct.buffersMu.Lock()
		oldCnt := len(ct.buffers)
		for id, buf := range ct.buffers {
			if buf.atime.Add(time.Second * 1800).Before(now) {
				if onRelease := ct.unregisterLocked(id); onRelease != nil {
					released = append(released, onRelease)
				}
				logger.Infof("Coro transfer service: GC buffer for %s with request ID %d", buf.key, id)
			}
		}
		newCnt := len(ct.buffers)
		ct.buffersMu.Unlock()
		for _, onRelease := range released {
			onRelease()
		}
		logger.Debugf("Coro transfer service: GC buffer %d -> %d", oldCnt, newCnt)
		utils.SleepWithJitter(ct.config.PeerTimeout)
	}
}

// Returns (or creates) a client pool for the given addr, picking the source
// NIC round-robin.
func (ct *coroTransfer) getClient(addr string) (*coroTransferClient, error) {
	ct.clientMu.Lock()
	defer ct.clientMu.Unlock()

	var nic string
	if len(ct.clientNics) > 0 {
		nic = ct.clientNics[ct.clientSeq%uint64(len(ct.clientNics))]
		ct.clientSeq++
	}
	logger.Debugf("Select client binding %s for dest addr %s", nic, addr)

	key := addr + nic
	client, ok := ct.clients[key]
	if ok {
		client.touch()
		return client, nil
	}

	pcfg := &coro.ClientPoolConfig{
		Addr:              addr,
		Src:               nic,
		ConnTimeout:       ct.connectTimeoutSec,
		ReqTimeout:        int(ct.config.PeerTimeout.Seconds()),
		IdleTimeout:       ct.idleTimeoutSec,
		ConnectRetryCount: ct.connectRetryCount,
		EnableRdma:        ct.rdma,
		RdmaMinBuffer:     ct.config.RdmaBufCount,
		RdmaMaxBuffer:     ct.rdmaMaxBuffer,
	}
	client, err := newCoroTransferClient(pcfg)
	if err != nil {
		return nil, err
	}
	ct.clients[key] = client
	return client, nil
}

func (ct *coroTransfer) gcClients() {
	for {
		now := time.Now()
		ct.clientMu.Lock()
		for addr, client := range ct.clients {
			if client.getAtime().Add(4 * ct.config.PeerTimeout).Before(now) {
				client.stop()
				delete(ct.clients, addr)
				logger.Debugf("Coro transfer service: GC client to %s", addr)
			}
		}
		ct.clientMu.Unlock()
		utils.SleepWithJitter(time.Minute)
	}
}

func (ct *coroTransfer) initRDMA() error {
	cfg := ct.config

	// Keep this ahead of getAllRDMADevs below: coro_rpc latches the device
	// configuration when the devices are first touched, and enumerating them
	// counts as touching, which would drop everything configured here.
	coroOnce.Do(func() {
		poolCfg := &coro.BufferPoolConfig{
			BufferSize:  cfg.RdmaBufSize,
			MaxMemUsage: cfg.RdmaMaxMem,
		}
		logger.Infof("Coro transfer service: Init RDMA buffer with size %dKiB, max memory %dMiB",
			cfg.RdmaBufSize>>10, cfg.RdmaMaxMem>>20)
		coro.InitBufferPool(poolCfg)
	})

	// If no transport device specified, use all RDMA devices
	allDevs := getAllRDMADevs()
	if len(cfg.TransportNics) == 0 {
		cfg.TransportNics = allDevs
		logger.Infof("Use all available RDMA devices: %v", cfg.TransportNics)
	} else {
		logger.Infof("Use user specified RDMA devices: %v", cfg.TransportNics)
		// Keep only the specified devices that actually exist, de-duplicating
		// repeated names. As long as at least one is found, use RDMA with the
		// available subset; only fail (and fallback to TCP if configured) when
		// none of them are found.
		devSet := make(map[string]struct{}, len(allDevs))
		for _, dev := range allDevs {
			devSet[dev] = struct{}{}
		}
		seen := make(map[string]struct{}, len(cfg.TransportNics))
		found := make([]string, 0, len(cfg.TransportNics))
		var notFound []string
		for _, dev := range cfg.TransportNics {
			if _, dup := seen[dev]; dup {
				continue
			}
			seen[dev] = struct{}{}
			if _, ok := devSet[dev]; ok {
				found = append(found, dev)
			} else {
				notFound = append(notFound, dev)
			}
		}
		if len(found) == 0 {
			return fmt.Errorf("none of specified RDMA devices %v found, available: %v", cfg.TransportNics, allDevs)
		}
		if len(notFound) > 0 {
			logger.Warnf("RDMA devices %v not found, use available subset: %v", notFound, found)
		}
		cfg.TransportNics = found
	}
	if len(cfg.TransportNics) == 0 {
		return errors.New("require RDMA device")
	}

	ct.nics = cfg.TransportNics
	ct.clientNics = cfg.TransportNics
	return nil
}

func (ct *coroTransfer) initTCP() error {
	cfg := ct.config

	// Get IPs of specified transport devices.
	transportIPs, err := utils.GetIPsFromInterfaces(cfg.TransportNics)
	if err != nil {
		return err
	}

	// No transport device specified, use configured IP as the single server,
	// and let OS pick the proper NIC for outgoing connections.
	if len(transportIPs) == 0 {
		ct.nics = []string{cfg.TransportIP}
		ct.clientNics = nil
	} else {
		// Use IP of each NIC to represent transport devices.
		ct.nics = transportIPs
		ct.clientNics = transportIPs
	}
	return nil
}

func (ct *coroTransfer) startRDMA() error {
	cfg := ct.config

	// Start one server per RDMA device
	for i, nic := range ct.nics {
		port := cfg.BasePort + i
		lis, err := utils.FindAvailablePort(cfg.TransportIP, port)
		if err != nil {
			return fmt.Errorf("failed to listen on %s:%d: %v", cfg.TransportIP, port, err)
		}
		addr := lis.Addr().String()
		ct.lisAddrs = append(ct.lisAddrs, addr)
		lis.Close()

		s := coro.StartServer(&coro.ServerConfig{
			Addr:      addr,
			RdmaDev:   nic,
			ThreadNum: ct.serverThreads,
		})
		if s == nil {
			return fmt.Errorf("start server on %s failed", nic)
		}
		ct.rpcServers = append(ct.rpcServers, s)
		logger.Infof("Coro transfer service: serve transport on %s for %s", addr, nic)
	}
	return nil
}

func (ct *coroTransfer) startTCP() error {
	cfg := ct.config

	// Start one server per listening IP.
	port := cfg.BasePort
	for _, ip := range ct.nics {
		lis, err := utils.FindAvailablePort(ip, port)
		if err != nil {
			return fmt.Errorf("failed to listen on %s:%d: %v", ip, port, err)
		}
		addr := lis.Addr().String()
		ct.lisAddrs = append(ct.lisAddrs, addr)
		lis.Close()

		s := coro.StartServer(&coro.ServerConfig{
			Addr:      addr,
			RdmaDev:   "",
			ThreadNum: ct.serverThreads,
		})
		if s == nil {
			return fmt.Errorf("failed to start server on %s", addr)
		}
		ct.rpcServers = append(ct.rpcServers, s)
		logger.Infof("Coro transfer service: serve transport on %s", addr)
	}
	return nil
}

func (ct *coroTransfer) StartServer() (rerr error) {
	defer func() {
		if rerr == nil {
			go ct.gcBuffers()
		}
	}()
	if ct.config.RdmaEnabled() {
		return ct.startRDMA()
	} else {
		return ct.startTCP()
	}
}

func (ct *coroTransfer) StopServer() error {
	for _, server := range ct.rpcServers {
		if server != nil {
			coro.StopServer(server)
		}
	}
	ct.rpcServers = nil

	ct.clientMu.Lock()
	for addr, client := range ct.clients {
		client.stop()
		delete(ct.clients, addr)
	}
	ct.clientMu.Unlock()

	// Servers are down and cannot read any source buffer again, so drop the rest.
	var released []func()
	ct.buffersMu.Lock()
	for id := range ct.buffers {
		if onRelease := ct.unregisterLocked(id); onRelease != nil {
			released = append(released, onRelease)
		}
	}
	ct.buffersMu.Unlock()
	for _, onRelease := range released {
		onRelease()
	}

	delCoroTransfer(ct.id)
	return nil
}

func (ct *coroTransfer) ServerAddrs() []string {
	return ct.lisAddrs
}

// SelectServerAddr returns a server address, round-robin over all addresses.
func (ct *coroTransfer) SelectServerAddr() (string, error) {
	if len(ct.lisAddrs) == 0 {
		return "", errors.New("no server addr available")
	}
	seq := ct.serverSeq.Add(1) - 1
	return ct.lisAddrs[seq%uint64(len(ct.lisAddrs))], nil
}

func (ct *coroTransfer) ServerID() uint32 {
	return ct.id
}

func (ct *coroTransfer) Register(key string, data []byte, onRelease func()) (RequestID, error) {
	ct.buffersMu.Lock()
	defer ct.buffersMu.Unlock()
	reqID := ct.getReqID()
	buf := &transferBuffer{
		key:       key,
		data:      data,
		atime:     time.Now(),
		onRelease: onRelease,
	}
	logger.Debugf("Coro transfer service: registered key %s with reuqest ID %d", key, reqID)
	ct.buffers[reqID] = buf
	return reqID, nil
}

func (ct *coroTransfer) Unregister(reqID RequestID) {
	ct.buffersMu.Lock()
	onRelease := ct.unregisterLocked(reqID)
	ct.buffersMu.Unlock()
	if onRelease != nil {
		onRelease()
	}
}

func (ct *coroTransfer) Download(ctx context.Context, addr string, svcID uint32, reqID RequestID, data []byte) error {
	return ct.downloadWithRetry(addr, int64(len(data)), func(pool unsafe.Pointer) error {
		return coro.Load(pool, svcID, uint64(reqID), data)
	})
}

// downloadWithRetry acquires a client for the given addr, applies rate
// limiting, then invokes loadFn with the client's pool handle.
//
// On a stale-connection error (ENOTCONN, signalling that the remote peer
// restarted) it calls FlushClientPool to atomically drain every idle stale
// connection then retries loadFn once.
func (ct *coroTransfer) downloadWithRetry(addr string, want int64, loadFn func(pool unsafe.Pointer) error) error {
	client, err := ct.getClient(addr)
	if err != nil {
		return fmt.Errorf("getClient failed: %v", err)
	}

	ct.waitLimits(want)
	err = loadFn(client.pool)
	if err == nil || !isStaleConnError(err) {
		return err
	}

	// Retry on stale connection
	if client.pool != nil {
		n := coro.FlushClientPool(client.pool, addr)
		logger.Infof("Coro transfer service: stale connection to %s, flushed %d stale clients, retrying: %v", addr, n, err)
	} else {
		logger.Infof("Coro transfer service: stale connection to %s, retrying: %v", addr, err)
	}
	ct.waitLimits(want)
	return loadFn(client.pool)
}

// waitLimits blocks until the download and total rate limiters grant want bytes.
func (ct *coroTransfer) waitLimits(want int64) {
	if ct.downLimit != nil {
		ct.downLimit.Wait(want)
	}
	if ct.totalLimit != nil {
		ct.totalLimit.Wait(want)
	}
}

// Clear all idle cached connections whose server IP matches any entry in peerIPs.
func (ct *coroTransfer) EvictPeer(peerIPs []string) int {
	if len(peerIPs) == 0 {
		return 0
	}
	logger.Debugf("Coro transfer service: EvictPeer peer IPs: %v", peerIPs)

	// Build a lookup set for O(1) per-client check.
	ipSet := make(map[string]struct{}, len(peerIPs))
	for _, ip := range peerIPs {
		ipSet[ip] = struct{}{}
	}

	ct.clientMu.Lock()
	defer ct.clientMu.Unlock()

	// flushedByIP tracks how many idle connections were drained per peer IP.
	flushedByIP := make(map[string]int, len(peerIPs))
	for _, client := range ct.clients {
		ip, _, err := net.SplitHostPort(client.addr)
		if err != nil {
			continue
		}
		if _, ok := ipSet[ip]; !ok {
			continue
		}
		if client.pool == nil {
			continue
		}
		logger.Debugf("Coro transfer service: flushing client pool with addr %s", client.addr)
		n := int(coro.FlushClientPool(client.pool, client.addr))
		flushedByIP[ip] += n
	}

	total := 0
	for ip, n := range flushedByIP {
		total += n
		logger.Debugf("Coro transfer service: evicted peer %s, flushed %d idle connections", ip, n)
	}
	logger.Infof("Coro transfer service: eviction summary — peers: %v, total flushed: %d", peerIPs, total)
	return total
}

func initMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "rdma_mem_usage",
			Help: "Current memory usage in bytes of coro-rpc RDMA transport",
		},
		func() float64 { return float64(coro.RdmaMemUsage()) },
	))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "rdma_max_mem_usage",
			Help: "History max memory usage in bytes of coro-rpc RDMA transport",
		},
		func() float64 { return float64(coro.RdmaMaxMemUsage()) },
	))
}

// sortedNics returns unique NICs in deterministic order, for logging and
// bandwidth limit resolution.
func sortedNics(nics []string) []string {
	seen := make(map[string]struct{}, len(nics))
	out := make([]string, 0, len(nics))
	for _, n := range nics {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func addCoroTransfer(id uint32, coroSvc *coroTransfer) error {
	transferSvcMu.Lock()
	defer transferSvcMu.Unlock()
	_, ok := transferSvcs[id]
	if ok {
		return fmt.Errorf("Coro transfer service ID %d has been initialized", id)
	}
	transferSvcs[id] = coroSvc
	return nil
}

func delCoroTransfer(id uint32) {
	transferSvcMu.Lock()
	delete(transferSvcs, id)
	transferSvcMu.Unlock()
}

func getCoroTransfer(id uint32) (*coroTransfer, error) {
	transferSvcMu.RLock()
	defer transferSvcMu.RUnlock()
	coroSvc, ok := transferSvcs[id]
	if ok {
		return coroSvc, nil
	} else {
		return nil, fmt.Errorf("Coro transfer service ID %d not found", id)
	}
}

// Returns true if the error indicates that the RDMA/TCP connection to the remote peer has become
// stale because the peer restarted (e.g. RDMA QP destroyed, peer process exited).
// Matches coro_rpc::errc::not_connected which maps both connection_refused and ENOTCONN
// to the stable, locale-independent message "not connected".
func isStaleConnError(err error) bool {
	return strings.Contains(err.Error(), "not connected")
}

//export load_service
func load_service(ctx unsafe.Pointer, svc_id C.uint32_t, req_id C.uint64_t) {
	svcID := uint32(svc_id)
	if svcID == 0 {
		// For backward compatibility, old client doesn't have service ID,
		// and coro_rpc will set it to 0, so use the 1st service by default.
		// This only works with one transfer service.
		svcID = 1
	}
	coroSvc, err := getCoroTransfer(svcID)
	if err != nil {
		coro.ResponseError(ctx, err.Error())
		return
	}

	reqID := RequestID(req_id)
	buf, err := coroSvc.getBuffer(reqID)
	if err != nil {
		coro.ResponseError(ctx, err.Error())
		return
	}

	want := int64(len(buf.data))
	if coroSvc.upLimit != nil {
		coroSvc.upLimit.Wait(want)
	}
	if coroSvc.totalLimit != nil {
		coroSvc.totalLimit.Wait(want)
	}

	promise := coro.ResponseMsg(ctx, buf.data)

	onRelease := buf.onRelease
	go func(p unsafe.Pointer) {
		if err := coro.WaitResponseFinish(p); err != nil {
			logger.Errorf("Coro transfer service: WaitResponseFinish for reqID %d failed: %s", reqID, err)
		}
		if onRelease != nil {
			onRelease()
		}
	}(promise)
}

type coroTransferClient struct {
	pool  unsafe.Pointer
	addr  string // server "ip:port", used by EvictPeer for IP matching
	atime time.Time
}

// Create new client to specified server `addr`, and `src` is to support multiple client side NIC,
// specify which device to use, which could be IP of client NIC if RDMA is not enabled, and RDMA
// device name if RDMA is enabled
func newCoroTransferClient(pcfg *coro.ClientPoolConfig) (*coroTransferClient, error) {
	pool := coro.CreateClientPool(pcfg)
	if pool == nil {
		return nil, errors.New("Coro transfer service: create client pool failed")
	}
	client := &coroTransferClient{
		pool:  pool,
		addr:  pcfg.Addr,
		atime: time.Now(),
	}
	return client, nil
}

func (c *coroTransferClient) touch() {
	c.atime = time.Now()
}

func (c *coroTransferClient) getAtime() time.Time {
	return c.atime
}

func (c *coroTransferClient) stop() {
	coro.FreeClientPool(c.pool)
}
