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
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	cachefsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	pb "github.com/aliyun/cachefs/pkg/rpc/cacheroot"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/srcmgr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willf/bitset"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetCacheState(t *testing.T) {
	fs := &Cachefs{
		cache: &mockCacheManager{
			state: chunk.CacheState{
				Capacity:      2048,
				Used:          1024,
				HashHitBlocks: 3,
				HashHitBytes:  300,
				CachedBlocks:  6,
				CachedBytes:   600,
				PinnedBlocks:  4,
				PinnedBytes:   400,
			},
		},
	}
	svc := newCacheStateService(fs)
	resp, err := svc.GetCacheState(context.Background(), &pb.GetCacheStateRequest{})
	require.NoError(t, err)
	assert.Equal(t, uint64(2048), resp.GetCapacity())
	assert.Equal(t, uint64(1024), resp.GetUsed())
	assert.Equal(t, uint64(3), resp.GetHashhitBlocks())
	assert.Equal(t, uint64(300), resp.GetHashhitBytes())
	assert.Equal(t, uint64(6), resp.GetCachedBlocks())
	assert.Equal(t, uint64(600), resp.GetCachedBytes())
	assert.Equal(t, uint64(4), resp.GetPinnedBlocks())
	assert.Equal(t, uint64(400), resp.GetPinnedBytes())
}

func TestGetTrackerInfo(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		svc := newCacheStateService(&Cachefs{cache: &mockCacheManager{}})
		resp, err := svc.GetTrackerInfo(context.Background(), &pb.GetTrackerInfoRequest{})
		require.NoError(t, err)
		assert.Empty(t, resp.GetTargets())
	})

	t.Run("populated", func(t *testing.T) {
		// /a.txt: node1 holds block 0; node2 holds blocks 0 and 2.
		bs0 := bitset.New(0)
		bs0.Set(0)
		bs02 := bitset.New(0)
		bs02.Set(0)
		bs02.Set(2)
		svc := newCacheStateService(&Cachefs{cache: &mockCacheManager{
			snapshot: map[string]map[string]*bitset.BitSet{
				"/a.txt": {"node1": bs0, "node2": bs02},
			},
		}})

		resp, err := svc.GetTrackerInfo(context.Background(), &pb.GetTrackerInfoRequest{})
		require.NoError(t, err)
		require.Len(t, resp.GetTargets(), 1)
		tt := resp.GetTargets()[0]
		assert.Equal(t, "/a.txt", tt.GetTarget())

		byNode := make(map[string][]uint32, len(tt.GetNodes()))
		for _, n := range tt.GetNodes() {
			byNode[n.GetNode()] = n.GetBlocks()
		}
		require.Len(t, byNode, 2)
		// NextSet iteration yields ascending block numbers.
		assert.Equal(t, []uint32{0}, byNode["node1"])
		assert.Equal(t, []uint32{0, 2}, byNode["node2"])
	})

	t.Run("local", func(t *testing.T) {
		// Local mode reads the local-only snapshot; /b.txt held by the local node on blocks 1,3.
		bs13 := bitset.New(0)
		bs13.Set(1)
		bs13.Set(3)
		cache := &mockCacheManager{
			snapshot:      map[string]map[string]*bitset.BitSet{"/a.txt": {"peer": bitset.New(0)}},
			localSnapshot: map[string]map[string]*bitset.BitSet{"/b.txt": {"self": bs13}},
		}
		svc := newCacheStateService(&Cachefs{cache: cache})

		resp, err := svc.GetTrackerInfo(context.Background(), &pb.GetTrackerInfoRequest{Local: true})
		require.NoError(t, err)
		require.Len(t, resp.GetTargets(), 1)
		tt := resp.GetTargets()[0]
		assert.Equal(t, "/b.txt", tt.GetTarget())
		require.Len(t, tt.GetNodes(), 1)
		assert.Equal(t, "self", tt.GetNodes()[0].GetNode())
		assert.Equal(t, []uint32{1, 3}, tt.GetNodes()[0].GetBlocks())
	})
}

func TestGetCacheStats(t *testing.T) {
	t.Run("serf nil", func(t *testing.T) {
		svc := newCacheRootClusterService(&Cachefs{}, nil)
		_, err := svc.GetCacheStats(context.Background(), &pb.GetCacheStatsRequest{})
		require.Error(t, err)
		assert.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("fanout", func(t *testing.T) {
		// Shorten the per-member timeout so the dead address fails fast.
		orig := fanoutTimeout
		fanoutTimeout = time.Second
		defer func() { fanoutTimeout = orig }()

		// Stand up a real CachefsState server for the live member.
		served := &Cachefs{
			cache: &mockCacheManager{
				state: chunk.CacheState{Capacity: 100, Used: 50, CachedBlocks: 2, CachedBytes: 20},
			},
		}
		svc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: "127.0.0.1:0"})
		require.NoError(t, err)
		pb.RegisterCachefsStateServer(svc.GetServer(), newCacheStateService(served))
		svc.Serve()
		defer svc.Stop()

		liveAddr := svc.Addr()
		deadAddr := "127.0.0.1:1" // nothing listening -> connection refused

		cluster := newCacheRootClusterService(&Cachefs{
			serf: &MockSerfService{rawMembers: []string{liveAddr, deadAddr}},
		}, nil)
		resp, err := cluster.GetCacheStats(context.Background(), &pb.GetCacheStatsRequest{})
		require.NoError(t, err)
		require.Len(t, resp.GetInstances(), 2)

		byNode := make(map[string]*pb.InstanceCacheState, 2)
		for _, ins := range resp.GetInstances() {
			byNode[ins.GetNode()] = ins
		}

		live := byNode[liveAddr]
		require.NotNil(t, live)
		assert.True(t, live.GetOk())
		assert.Equal(t, uint64(100), live.GetState().GetCapacity())
		assert.Equal(t, uint64(50), live.GetState().GetUsed())
		assert.Equal(t, uint64(2), live.GetState().GetCachedBlocks())

		dead := byNode[deadAddr]
		require.NotNil(t, dead)
		assert.False(t, dead.GetOk())
		assert.NotEmpty(t, dead.GetError())
	})
}

func TestWalkBase(t *testing.T) {
	tree := buildTestTree()
	tests := []struct {
		name          string
		relPath       string
		metaReady     bool
		wantStart     string
		wantMetaFS    bool
		wantUseSource bool
	}{
		{"root slash, meta ready", "/", true, ".", true, false},
		{"empty, meta ready", "", true, ".", true, false},
		{"nested, meta ready", "/foo/bar", true, "foo/bar", true, false},
		{"relative, meta ready", "foo", true, "foo", true, false},
		{"cleaned, meta ready", "foo/../bar", true, "bar", true, false},
		{"root slash, meta not ready", "/", false, ".", false, true},
		{"nested, meta not ready", "/foo/bar", false, "foo/bar", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m *mockMeta
			if tt.metaReady {
				m = &mockMeta{root: tree.root, ready: true, inoMap: tree.inoMap}
			} else {
				m = &mockMeta{ready: false}
			}
			c := newTestCachefs(&Config{
				Meta: &cachefsmeta.MetaConfig{SourceDirs: []string{"/src"}, SourceManager: srcmgr.NewSourceManager([]string{"/src"}, nil)},
			}, m)

			base, start, useSource, err := c.walkBase(tt.relPath)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStart, start)
			assert.Equal(t, tt.wantUseSource, useSource)
			_, isMeta := base.(*cachefsmeta.MetaFS)
			assert.Equal(t, tt.wantMetaFS, isMeta)
		})
	}
}

// writeFile creates a file with n bytes of content under dir.
func writeFile(t *testing.T, dir, rel string, n int) {
	t.Helper()
	full := filepath.Join(dir, rel)
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0755))
	require.NoError(t, os.WriteFile(full, make([]byte, n), 0644))
}

func TestWalkPathTargets(t *testing.T) {
	srcDir := t.TempDir()
	writeFile(t, srcDir, "a.txt", 100)
	writeFile(t, srcDir, "sub/b.txt", 50)
	writeFile(t, srcDir, "empty.txt", 0) // zero-size, must be skipped

	c := newTestCachefs(&Config{
		Meta: &cachefsmeta.MetaConfig{SourceDirs: []string{srcDir}, MountPoint: "/mnt", SourceManager: srcmgr.NewSourceManager([]string{srcDir}, nil)},
	}, &mockMeta{ready: false}) // not ready -> walk the source dir

	got := make(map[string]uint64)
	err := c.walkPathTargets("/", func(target string, size uint64) {
		got[target] = size
	}, func() {})
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{
		"/a.txt":     100,
		"/sub/b.txt": 50,
	}, got)

	// Non-existent path must surface an error.
	err = c.walkPathTargets("/nope", func(string, uint64) {}, func() {})
	assert.Error(t, err)
}

func TestGetCacheRatio(t *testing.T) {
	srcDir := t.TempDir()
	writeFile(t, srcDir, "a.txt", 100) // 2 blocks at blockSize 64

	// Snapshot: block 0 of /a.txt held by node1 and node2, block 1 by node2 only.
	// Cluster presence (union) therefore covers both blocks.
	bs0 := bitset.New(0)
	bs0.Set(0)
	bs01 := bitset.New(0)
	bs01.Set(0)
	bs01.Set(1)
	c := newTestCachefs(&Config{
		Store: &chunk.StoreConfig{BlockSize: 64},
		Meta:  &cachefsmeta.MetaConfig{SourceDirs: []string{srcDir}, MountPoint: "/mnt", SourceManager: srcmgr.NewSourceManager([]string{srcDir}, nil)},
	}, &mockMeta{ready: false})
	c.cache = &mockCacheManager{snapshot: map[string]map[string]*bitset.BitSet{
		"/a.txt": {"node1": bs0, "node2": bs01},
	}}

	svc := newCacheRootClusterService(c, nil)
	require.NoError(t, (&pb.GetCacheRatioRequest{}).Validate())
	require.NoError(t, (&pb.GetCacheRatioRequest{Paths: []string{""}}).Validate())
	require.NoError(t, (&pb.GetCacheRatioRequest{Paths: []string{"", "/missing"}}).Validate())

	defaultResp, err := svc.GetCacheRatio(context.Background(), &pb.GetCacheRatioRequest{})
	require.NoError(t, err)
	require.Len(t, defaultResp.GetRatios(), 1)
	assert.Equal(t, "/", defaultResp.GetRatios()[0].GetPath())
	assert.True(t, defaultResp.GetRatios()[0].GetOk())

	allEmptyResp, err := svc.GetCacheRatio(context.Background(), &pb.GetCacheRatioRequest{Paths: []string{""}})
	require.NoError(t, err)
	require.Len(t, allEmptyResp.GetRatios(), 1)
	assert.Equal(t, "/", allEmptyResp.GetRatios()[0].GetPath())
	assert.True(t, allEmptyResp.GetRatios()[0].GetOk())

	mixedEmptyResp, err := svc.GetCacheRatio(context.Background(), &pb.GetCacheRatioRequest{Paths: []string{"", "/missing"}})
	require.NoError(t, err)
	require.Len(t, mixedEmptyResp.GetRatios(), 1)
	assert.Equal(t, "/missing", mixedEmptyResp.GetRatios()[0].GetPath())
	assert.False(t, mixedEmptyResp.GetRatios()[0].GetOk())

	// Query an existing path and a missing one in a single call.
	resp, err := svc.GetCacheRatio(context.Background(), &pb.GetCacheRatioRequest{Paths: []string{"/", "/missing"}})
	require.NoError(t, err)
	require.Len(t, resp.GetRatios(), 2)

	// Existing path: cluster cached is the union across nodes (both blocks).
	ok := resp.GetRatios()[0]
	assert.Equal(t, "/", ok.GetPath())
	assert.True(t, ok.GetOk())
	assert.Equal(t, uint64(2), ok.GetTotalBlocks())
	assert.Equal(t, uint64(100), ok.GetTotalBytes())
	assert.Equal(t, uint64(2), ok.GetCachedBlocks())
	assert.Equal(t, uint64(100), ok.GetCachedBytes()) // both blocks: 64 + 36
	// Non-verbose does not populate the per-node breakdown.
	assert.Empty(t, ok.GetNodes())

	// Missing path: reported as not ok with an error.
	missing := resp.GetRatios()[1]
	assert.Equal(t, "/missing", missing.GetPath())
	assert.False(t, missing.GetOk())
	assert.NotEmpty(t, missing.GetError())

	// Verbose: per-node breakdown against the same snapshot.
	vresp, err := svc.GetCacheRatio(context.Background(), &pb.GetCacheRatioRequest{Paths: []string{"/"}, Verbose: true})
	require.NoError(t, err)
	require.Len(t, vresp.GetRatios(), 1)
	vok := vresp.GetRatios()[0]
	// Cluster totals unchanged by verbose.
	assert.Equal(t, uint64(2), vok.GetCachedBlocks())
	assert.Equal(t, uint64(100), vok.GetCachedBytes())
	nodes := make(map[string]*pb.NodeCacheState, len(vok.GetNodes()))
	for _, n := range vok.GetNodes() {
		nodes[n.GetNode()] = n
	}
	require.Len(t, nodes, 2)
	// node1 holds only block 0 (64 bytes).
	assert.Equal(t, uint64(1), nodes["node1"].GetCachedBlocks())
	assert.Equal(t, uint64(64), nodes["node1"].GetCachedBytes())
	// node2 holds both blocks (64 + 36 = 100 bytes).
	assert.Equal(t, uint64(2), nodes["node2"].GetCachedBlocks())
	assert.Equal(t, uint64(100), nodes["node2"].GetCachedBytes())
}

func TestWalkPathTargetsFailover(t *testing.T) {
	dir1 := t.TempDir() // broken replica
	dir2 := t.TempDir()
	writeFile(t, dir2, "a.txt", 100)
	writeFile(t, dir2, "sub/b.txt", 50)
	stubSourceDirFS(t, dir1, syscall.EIO)

	sm := srcmgr.NewSourceManager([]string{dir1, dir2}, nil)
	c := newTestCachefs(&Config{
		Meta: &cachefsmeta.MetaConfig{SourceDirs: []string{dir1, dir2}, MountPoint: "/mnt", SourceManager: sm},
	}, &mockMeta{ready: false}) // not ready -> walk the source dir

	got := make(map[string]uint64)
	err := c.walkPathTargets("/", func(target string, size uint64) {
		got[target] = size
	}, func() {})
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{
		"/a.txt":     100,
		"/sub/b.txt": 50,
	}, got)
}
