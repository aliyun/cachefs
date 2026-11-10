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

package vfs

import (
	"context"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aliyun/cachefs/pkg/meta"
	cachefsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	pb "github.com/aliyun/cachefs/pkg/rpc/cacheroot"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/srcmgr"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fanoutTimeout bounds a single GetCacheState call to a cluster member.
var fanoutTimeout = 10 * time.Second

// cacheStateService implements the CachefsState service. It exposes the per-instance raw cache
// state aggregated from the local store.
type cacheStateService struct {
	fs *Cachefs
}

// Create a CachefsState server backed by fs.
func newCacheStateService(fs *Cachefs) pb.CachefsStateServer {
	return &cacheStateService{fs: fs}
}

func (s *cacheStateService) GetCacheState(ctx context.Context, _ *pb.GetCacheStateRequest) (*pb.CacheState, error) {
	logger.Infof("GetCacheState called")
	st := s.fs.cache.getCacheState()
	return &pb.CacheState{
		Capacity:      uint64(st.Capacity),
		Used:          uint64(st.Used),
		HashhitBlocks: uint64(st.HashHitBlocks),
		HashhitBytes:  uint64(st.HashHitBytes),
		CachedBlocks:  uint64(st.CachedBlocks),
		CachedBytes:   uint64(st.CachedBytes),
		PinnedBlocks:  uint64(st.PinnedBlocks),
		PinnedBytes:   uint64(st.PinnedBytes),
	}, nil
}

// GetTrackerInfo returns a node-aware view: per target, the nodes and the block numbers each node
// holds. By default it returns this node's snapshot of the cluster tracker (metaMap); when
// req.Local is set it returns this node's own authoritative cache distribution (single local node)
// instead. When paths is non-empty the scan is narrowed to the matching target prefixes; otherwise
// the whole tracker is returned.
func (s *cacheStateService) GetTrackerInfo(ctx context.Context, req *pb.GetTrackerInfoRequest) (*pb.GetTrackerInfoResponse, error) {
	paths := req.GetPaths()
	local := req.GetLocal()
	logger.Infof("GetTrackerInfo called: paths=%v local=%v", paths, local)
	var prefixes []string
	if len(paths) > 0 {
		prefixes = s.fs.pathsToCachePrefixes(paths)
	}
	snap := s.fs.cache.trackerSnapshot(prefixes, local)

	resp := &pb.GetTrackerInfoResponse{
		Targets: make([]*pb.TargetTracker, 0, len(snap)),
	}
	for target, perNode := range snap {
		tt := &pb.TargetTracker{
			Target: target,
			Nodes:  make([]*pb.NodeBlocks, 0, len(perNode)),
		}
		for node, bs := range perNode {
			blocks := make([]uint32, 0, bs.Count())
			for i, e := bs.NextSet(0); e; i, e = bs.NextSet(i + 1) {
				blocks = append(blocks, uint32(i))
			}
			tt.Nodes = append(tt.Nodes, &pb.NodeBlocks{
				Node:   node,
				Blocks: blocks,
			})
		}
		resp.Targets = append(resp.Targets, tt)
	}
	return resp, nil
}

// cacheRootClusterService implements the CacheRootCluster service.
type cacheRootClusterService struct {
	fs *Cachefs

	// server-side call counters and latency histograms for the cluster methods;
	// nil when no registerer was provided (e.g. in tests).
	getCacheRatioCount prometheus.Counter
	getCacheRatioHist  prometheus.Histogram
	getCacheStatsCount prometheus.Counter
	getCacheStatsHist  prometheus.Histogram
}

// Create a CacheRootCluster server backed by fs. When reg is non-nil, the call counters and
// latency histograms are created and registered.
func newCacheRootClusterService(fs *Cachefs, reg prometheus.Registerer) pb.CacheRootClusterServer {
	s := &cacheRootClusterService{fs: fs}
	if reg != nil {
		s.getCacheRatioCount = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cacheroot_cluster_getcacheratio",
			Help: "GetCacheRatio request count in total",
		})
		s.getCacheRatioHist = prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cacheroot_cluster_getcacheratio_durations_histogram_seconds",
			Help:    "GetCacheRatio latency distribution",
			Buckets: prometheus.ExponentialBuckets(0.01, 1.5, 25),
		})
		s.getCacheStatsCount = prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cacheroot_cluster_getcachestats",
			Help: "GetCacheStats request count in total",
		})
		s.getCacheStatsHist = prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cacheroot_cluster_getcachestats_durations_histogram_seconds",
			Help:    "GetCacheStats latency distribution",
			Buckets: prometheus.ExponentialBuckets(0.01, 1.5, 25),
		})
		reg.MustRegister(s.getCacheRatioCount, s.getCacheRatioHist, s.getCacheStatsCount, s.getCacheStatsHist)
	}
	return s
}

// Register the vfs-implemented gRPC services on the shared rpc servers: CachefsState on the
// internal rpc server (cachePort) and CacheRootCluster on the cacheroot rpc server
// (cacheroot-port). Each is a no-op when the corresponding server is not enabled.
func (c *Cachefs) registerCacheRootServices(rpcSvc, cacherootSvc rpcsvc.RpcService, reg prometheus.Registerer) {
	if rpcSvc != nil {
		pb.RegisterCachefsStateServer(rpcSvc.GetServer(), newCacheStateService(c))
	}
	if cacherootSvc != nil {
		pb.RegisterCacheRootClusterServer(cacherootSvc.GetServer(), newCacheRootClusterService(c, reg))
	}
}

// Computes per-path cache progress. The denominator (total blocks & bytes) is derived by walking
// each path's files via meta; the numerator (cached blocks & bytes) is judged against a single
// cluster tracker snapshot.
func (s *cacheRootClusterService) GetCacheRatio(ctx context.Context, req *pb.GetCacheRatioRequest) (*pb.GetCacheRatioResponse, error) {
	paths := req.GetPaths()
	if len(paths) == 0 {
		paths = []string{"/"}
	} else {
		normalized := make([]string, 0, len(paths))
		for _, p := range paths {
			if p != "" {
				normalized = append(normalized, p)
			}
		}
		if len(normalized) == 0 {
			paths = []string{"/"}
		} else {
			paths = normalized
		}
	}
	if s.getCacheRatioCount != nil {
		s.getCacheRatioCount.Inc()
	}
	start := time.Now()
	logger.Infof("GetCacheRatio: paths=%v verbose=%v", paths, req.GetVerbose())
	defer func() {
		elapsed := time.Since(start)
		if s.getCacheRatioHist != nil {
			s.getCacheRatioHist.Observe(elapsed.Seconds())
		}
		logger.Infof("GetCacheRatio: paths=%v done in %s", paths, elapsed)
	}()

	// One snapshot per request; narrow the store-side lock scan by the requested
	// path prefixes. Best-effort: unresolved paths simply widen the scan and are
	// still reported per-path below.
	prefixes := s.fs.pathsToCachePrefixes(paths)
	snap := s.fs.cache.trackerSnapshot(prefixes, false)

	verbose := req.GetVerbose()
	blockSize := s.fs.Conf.Store.BlockSize
	resp := &pb.GetCacheRatioResponse{Ratios: make([]*pb.PathCacheRatio, 0, len(paths))}
	for _, p := range paths {
		pr := &pb.PathCacheRatio{Path: p}
		var totalBlocks, totalBytes, cachedBlocks, cachedBytes uint64
		// node addr -> per-node cached blocks/bytes of this path (verbose only).
		type nodeAmount struct{ blocks, bytes uint64 }
		var perNodeAgg map[string]*nodeAmount
		if verbose {
			perNodeAgg = make(map[string]*nodeAmount)
		}
		reset := func() {
			totalBlocks, totalBytes, cachedBlocks, cachedBytes = 0, 0, 0, 0
			if verbose {
				perNodeAgg = make(map[string]*nodeAmount)
			}
		}
		err := s.fs.walkPathTargets(p, func(target string, size uint64) {
			nblk := uint32((size + uint64(blockSize) - 1) / uint64(blockSize))
			totalBlocks += uint64(nblk)
			totalBytes += size
			perNode := snap[target]
			if len(perNode) == 0 {
				return
			}
			for i := uint32(0); i < nblk; i++ {
				var anyHolds bool
				for node, bs := range perNode {
					if !bs.Test(uint(i)) {
						continue
					}
					anyHolds = true
					if !verbose {
						break // presence is all we need; stop at the first holder
					}
					agg := perNodeAgg[node]
					if agg == nil {
						agg = &nodeAmount{}
						perNodeAgg[node] = agg
					}
					agg.blocks++
					agg.bytes += utils.GetBlockSize(blockSize, size, i)
				}
				if anyHolds {
					cachedBlocks++
					cachedBytes += utils.GetBlockSize(blockSize, size, i)
				}
			}
		}, reset)
		if err != nil {
			pr.Ok = false
			pr.Error = err.Error()
		} else {
			pr.Ok = true
			pr.TotalBlocks = totalBlocks
			pr.TotalBytes = totalBytes
			pr.CachedBlocks = cachedBlocks
			pr.CachedBytes = cachedBytes
			if verbose {
				pr.Nodes = make([]*pb.NodeCacheState, 0, len(perNodeAgg))
				for node, agg := range perNodeAgg {
					pr.Nodes = append(pr.Nodes, &pb.NodeCacheState{
						Node:         node,
						CachedBlocks: agg.blocks,
						CachedBytes:  agg.bytes,
					})
				}
			}
		}
		resp.Ratios = append(resp.Ratios, pr)
	}
	return resp, nil
}

// Fan out to every alive cluster member's CachefsState.GetCacheState and aggregates the results.
// A single failure is recorded on that instance's entry and does not fail the whole call.
func (s *cacheRootClusterService) GetCacheStats(ctx context.Context, _ *pb.GetCacheStatsRequest) (*pb.GetCacheStatsResponse, error) {
	if s.getCacheStatsCount != nil {
		s.getCacheStatsCount.Inc()
	}
	start := time.Now()
	logger.Infof("GetCacheStats called")
	defer func() {
		elapsed := time.Since(start)
		if s.getCacheStatsHist != nil {
			s.getCacheStatsHist.Observe(elapsed.Seconds())
		}
		logger.Infof("GetCacheStats: done in %s", elapsed)
	}()

	if s.fs.serf == nil {
		return nil, status.Error(codes.Unavailable, "serf service is not enabled")
	}
	members := s.fs.serf.GetMembers(true)

	// Bound the fanout concurrency
	tokens := utils.NewToken(32)
	var mu sync.Mutex
	var wg sync.WaitGroup
	resp := &pb.GetCacheStatsResponse{Instances: make([]*pb.InstanceCacheState, 0, len(members))}
	for _, addr := range members {
		wg.Add(1)
		tokens.Get()
		go func(addr string) {
			defer wg.Done()
			defer tokens.Put()
			ics := &pb.InstanceCacheState{Node: addr}
			state, err := fetchCacheState(ctx, addr)
			if err != nil {
				logger.Warnf("GetCacheStats: fanout to member %s failed: %s", addr, err)
				ics.Ok = false
				ics.Error = err.Error()
			} else {
				ics.Ok = true
				ics.State = state
			}
			mu.Lock()
			resp.Instances = append(resp.Instances, ics)
			mu.Unlock()
		}(addr)
	}
	wg.Wait()
	return resp, nil
}

// Call CachefsState.GetCacheState of the given address.
func fetchCacheState(ctx context.Context, addr string) (*pb.CacheState, error) {
	conn, err := rpcsvc.Dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cctx, cancel := context.WithTimeout(ctx, fanoutTimeout)
	defer cancel()
	return pb.NewCachefsStateClient(conn).GetCacheState(cctx, &pb.GetCacheStateRequest{})
}

// Walk all regular files under the given path (relative to the mount point) and invokes fn with
// each file's cache target and size. It uses the in-memory meta tree when available, otherwise the
// mounted filesystem.
//
// On source-dir replica failover the walk restarts from the beginning on the
// next replica; reset is invoked before every attempt of such a walk (the
// first included) so results accumulated by a failed attempt are discarded
// before the retry. The meta-tree walk never retries and invokes no reset.
func (c *Cachefs) walkPathTargets(relPath string, fn func(target string, size uint64), reset func()) error {
	base, start, useSource, err := c.walkBase(relPath)
	if err != nil {
		return err
	}
	walkFn := func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			if srcmgr.IsInfraError(werr) {
				// Propagate to WalkDir's caller for replica failover.
				return werr
			}
			logger.Warnf("Cacheroot: walk dir hit error %s on path %s, skip", werr, p)
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if srcmgr.IsInfraError(err) {
				// Propagate to WalkDir's caller for replica failover.
				return err
			}
			logger.Warnf("Cacheroot: walk dir hit stat error on %s, skip: %s", p, err)
			return nil
		}
		if !info.Mode().IsRegular() || info.Size() == 0 {
			return nil
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			logger.Warnf("Cacheroot: cannot cast stat of %s to Stat_t, skip", p)
			return nil
		}
		size := uint64(info.Size())

		// Walk via source dir, encode cache key with file size and mtime
		var target string
		if useSource {
			mtime, mtimensec := st.Mtim.Unix()
			target = c.encodeCacheTarget(c.Meta.GetPath(filepath.Join("/", p)), size, mtime, uint32(mtimensec))
		} else {
			cino, eno := c.Meta.GetInode(meta.Ino(st.Ino))
			if eno != 0 {
				return nil
			}
			mtime, mtimensec := st.Mtim.Unix()
			target = c.encodeCacheTarget(cino.GetPath(), size, mtime, uint32(mtimensec))
		}
		fn(target, size)
		return nil
	}

	if sm := c.Conf.Meta.SourceManager; useSource && sm != nil {
		// Reject non-existent paths so callers can distinguish "not found" from an
		// existing but empty file/dir (which legitimately walks to zero targets),
		// failing over between replicas on infrastructure errors.
		if err := sm.RunOp("stat", start, func(dir string) error {
			_, err := fs.Stat(sourceDirFS(dir), start)
			return err
		}); err != nil {
			return err
		}
		return sm.RunOp("walk", start, func(dir string) error {
			reset() // a retry re-walks from scratch; discard the failed attempt's results
			return fs.WalkDir(sourceDirFS(dir), start, walkFn)
		})
	} else {
		if _, err := fs.Stat(base, start); err != nil {
			return err
		}
		return fs.WalkDir(base, start, walkFn)
	}
}

// Resolve the fs.FS, starting sub-path, and whether source-dir target encoding is needed for
// walking relPath. Prefer the in-memory meta tree (no syscalls); otherwise fall
// back to the base dir chosen by getPreloadSource (source dir or mount point).
func (c *Cachefs) walkBase(relPath string) (fs.FS, string, bool, error) {
	start := path.Clean("/" + relPath)
	start = strings.TrimPrefix(start, "/")
	if start == "" {
		start = "."
	}
	if c.Meta.IsMetaReady() {
		return cachefsmeta.NewMetaFS(c.Meta), start, false, nil
	}
	sourceDir, useSource := c.getPreloadSource()
	return os.DirFS(sourceDir), start, useSource, nil
}
