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

package cachefsmeta

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/meta"
	pb "github.com/aliyun/cachefs/pkg/rpc/meta"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// mockCachefsMetaClient implements pb.CachefsMetaClient for testing.
type mockCachefsMetaClient struct {
	// loadMetaFn lets each test control exactly what LoadMeta returns.
	loadMetaFn func(ctx context.Context, req *pb.LoadMetaReq) (*pb.LoadMetaRes, error)
}

func (m *mockCachefsMetaClient) LoadMeta(ctx context.Context, in *pb.LoadMetaReq, opts ...grpc.CallOption) (*pb.LoadMetaRes, error) {
	return m.loadMetaFn(ctx, in)
}

// newTestMeta builds a minimal metaData suitable for waitAndLoadMeta tests.
// The caller controls PeerMetaTimeout and whether a fake client is pre-wired.
func newTestMeta(peerMetaTimeout int) *metaData {
	return &metaData{
		config: &MetaConfig{
			SourceDirs:      []string{"/src"},
			PeerMetaTimeout: peerMetaTimeout,
		},
		metaSource:   make(map[string]struct{}),
		metaSourceCh: make(chan struct{}, 1),
		clients:      make(map[string]*metaClient),
		inodeShards:  newInodeShards(),
		symlinks:     make(map[meta.Ino][]byte),
		rpc:          &MockRpcService{},
		serf:         &MockSerfService{},
		stopCh:       make(chan struct{}),
	}
}

// injectClient pre-wires a mock gRPC client so getClient() returns it
// without dialing any real address.
func injectClient(m *metaData, addr string, grpcClient pb.CachefsMetaClient) {
	m.clientMu.Lock()
	defer m.clientMu.Unlock()
	m.clients[addr] = &metaClient{
		grpcClient: grpcClient,
		addr:       addr,
		atime:      time.Now(),
	}
}

// minimalDumpedPayload returns a JSON-encoded dumpedCachefs with a root inode,
// sufficient for loadMeta to complete without error.
func minimalDumpedPayload(t *testing.T) []byte {
	t.Helper()
	dumped := &dumpedCachefs{
		RootInode: &dumpedInode{
			Inode: 1,
			Path:  "/src",
			Attr: &meta.Attr{
				Typ:  meta.TypeDirectory,
				Mode: 0755,
			},
		},
	}
	payload, err := json.Marshal(dumped)
	if !assert.NoError(t, err) {
		return nil
	}
	return payload
}

func TestWaitAndLoadMeta(t *testing.T) {
	t.Run("wait timeout", func(t *testing.T) {
		m := newTestMeta(1)
		// Do NOT send to metaSourceCh — simulate no peer becoming available.
		start := time.Now()
		err := m.waitAndLoadMeta()
		elapsed := time.Since(start)
		if !assert.Error(t, err) {
			return
		}
		assert.True(t, errors.Is(err, context.DeadlineExceeded),
			"expected DeadlineExceeded, got: %v", err)
		assert.Less(t, elapsed, 3*time.Second,
			"waitAndLoadMeta took too long: %s", elapsed)
		assert.False(t, m.metaReady.Load(), "metaReady should remain false on timeout")
	})

	t.Run("load timeout", func(t *testing.T) {
		const addr = "fake:9999"
		m := newTestMeta(1)
		// Wire a mock client whose LoadMeta blocks until the context is cancelled.
		injectClient(m, addr, &mockCachefsMetaClient{
			loadMetaFn: func(ctx context.Context, req *pb.LoadMetaReq) (*pb.LoadMetaRes, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		})
		m.metaSource[addr] = struct{}{}
		m.metaSourceCh <- struct{}{}
		start := time.Now()
		err := m.waitAndLoadMeta()
		elapsed := time.Since(start)
		if !assert.Error(t, err) {
			return
		}
		assert.True(t, errors.Is(err, context.DeadlineExceeded),
			"expected DeadlineExceeded, got: %v", err)
		assert.Less(t, elapsed, 3*time.Second,
			"waitAndLoadMeta should have returned after timeout, took: %s", elapsed)
		assert.False(t, m.metaReady.Load(), "metaReady should remain false when load times out")
	})

	t.Run("success sets metaReady and dumpedMeta", func(t *testing.T) {
		const addr = "fake:9999"
		m := newTestMeta(5)
		payload := minimalDumpedPayload(t)
		if payload == nil {
			return
		}
		injectClient(m, addr, &mockCachefsMetaClient{
			loadMetaFn: func(ctx context.Context, req *pb.LoadMetaReq) (*pb.LoadMetaRes, error) {
				return &pb.LoadMetaRes{Meta: payload}, nil
			},
		})
		m.metaSource[addr] = struct{}{}
		m.metaSourceCh <- struct{}{}
		err := m.waitAndLoadMeta()
		if !assert.NoError(t, err) {
			return
		}
		assert.True(t, m.metaReady.Load(), "metaReady should be true after successful load")
		assert.NotNil(t, m.root, "root inode should be populated after successful load")
		assert.NotNil(t, m.dumpedMeta.Load(), "dumpedMeta must be stored after successful load")
	})

	t.Run("all candidates fail then timeout", func(t *testing.T) {
		const addr = "fake:9999"
		m := newTestMeta(1)
		injectClient(m, addr, &mockCachefsMetaClient{
			loadMetaFn: func(ctx context.Context, req *pb.LoadMetaReq) (*pb.LoadMetaRes, error) {
				return nil, errors.New("simulated load error")
			},
		})
		m.metaSource[addr] = struct{}{}
		// Feed signals so each WAIT pass succeeds; load always fails, causing
		// goto WAIT until the ctx deadline fires.
		// Use a dedicated done channel to avoid racing with Stop() on m.stopCh.
		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case m.metaSourceCh <- struct{}{}:
					m.metaLock.Lock()
					m.metaSource[addr] = struct{}{}
					m.metaLock.Unlock()
					time.Sleep(10 * time.Millisecond)
				case <-done:
					return
				}
			}
		}()
		start := time.Now()
		err := m.waitAndLoadMeta()
		elapsed := time.Since(start)
		if !assert.Error(t, err) {
			return
		}
		assert.Less(t, elapsed, 3*time.Second,
			"should time out and not loop forever, took: %s", elapsed)
		assert.False(t, m.metaReady.Load())
	})

	t.Run("failure does not store dumpedMeta", func(t *testing.T) {
		m := newTestMeta(1)
		err := m.waitAndLoadMeta()
		if !assert.Error(t, err) {
			return
		}
		assert.Nil(t, m.dumpedMeta.Load(),
			"dumpedMeta must remain nil after failed waitAndLoadMeta")
	})

	t.Run("not peer meta nor consumer", func(t *testing.T) {
		m := newTestMeta(0) // PeerMetaTimeout == 0 → PeerMeta() false
		err := m.waitAndLoadMeta()
		if !assert.Error(t, err) {
			return
		}
		assert.Contains(t, err.Error(), "Not in sourceless meta consumer mode")
	})

	t.Run("local addr in candidates is skipped", func(t *testing.T) {
		const localAddr = "local:1234"
		const remoteAddr = "remote:5678"
		m := newTestMeta(5)
		m.localAddr = localAddr
		payload := minimalDumpedPayload(t)
		if payload == nil {
			return
		}
		// Local address client must never be called.
		injectClient(m, localAddr, &mockCachefsMetaClient{
			loadMetaFn: func(ctx context.Context, req *pb.LoadMetaReq) (*pb.LoadMetaRes, error) {
				t.Error("LoadMeta must not be called on local address")
				return nil, errors.New("should not be called")
			},
		})
		// Remote address succeeds.
		injectClient(m, remoteAddr, &mockCachefsMetaClient{
			loadMetaFn: func(ctx context.Context, req *pb.LoadMetaReq) (*pb.LoadMetaRes, error) {
				return &pb.LoadMetaRes{Meta: payload}, nil
			},
		})
		m.metaSource[localAddr] = struct{}{}
		m.metaSource[remoteAddr] = struct{}{}
		m.metaSourceCh <- struct{}{}
		err := m.waitAndLoadMeta()
		if !assert.NoError(t, err) {
			return
		}
		assert.True(t, m.metaReady.Load(), "metaReady should be true after loading from remote")
	})

	t.Run("local addr only in candidates falls through to WAIT", func(t *testing.T) {
		const localAddr = "local:1234"
		m := newTestMeta(1)
		m.localAddr = localAddr
		injectClient(m, localAddr, &mockCachefsMetaClient{
			loadMetaFn: func(ctx context.Context, req *pb.LoadMetaReq) (*pb.LoadMetaRes, error) {
				t.Error("LoadMeta must not be called on local address")
				return nil, errors.New("should not be called")
			},
		})
		m.metaSource[localAddr] = struct{}{}
		m.metaSourceCh <- struct{}{}
		// All candidates are local, so nothing loads; PeerMetaTimeout fires.
		start := time.Now()
		err := m.waitAndLoadMeta()
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded),
			"expected DeadlineExceeded, got: %v", err)
		assert.Less(t, time.Since(start), 3*time.Second)
		assert.False(t, m.metaReady.Load())
	})
}

// newLoadMetaServer builds a minimal metaData for LoadMeta server-side tests.
func newLoadMetaServer(ready bool, serfGroup string) *metaData {
	m := &metaData{
		serfGroup: serfGroup,
		loadMetaCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_loadmeta_count",
		}),
	}
	m.metaReady.Store(ready)
	return m
}

func TestLoadMetaServer(t *testing.T) {
	t.Run("meta not ready returns error", func(t *testing.T) {
		m := newLoadMetaServer(false, "grp1")
		_, err := m.LoadMeta(context.Background(), &pb.LoadMetaReq{
			SerfGroup: "grp1",
			Addr:      "requester:1234",
		})
		if !assert.Error(t, err) {
			return
		}
		assert.Contains(t, err.Error(), "meta not ready")
	})

	t.Run("serf group mismatch returns error", func(t *testing.T) {
		m := newLoadMetaServer(true, "grp1")
		m.dumpedMeta.Store(&dumpedCachefs{})
		_, err := m.LoadMeta(context.Background(), &pb.LoadMetaReq{
			SerfGroup: "grp-other",
			Addr:      "requester:1234",
		})
		if !assert.Error(t, err) {
			return
		}
		assert.Contains(t, err.Error(), "serf group mismatch")
	})

	t.Run("success returns serialized meta", func(t *testing.T) {
		m := newLoadMetaServer(true, "grp1")
		// Pre-populate dumpedMeta so dumpMeta() returns it without traversal.
		const wantNextInode = uint64(99)
		m.dumpedMeta.Store(&dumpedCachefs{NextInode: wantNextInode})
		resp, err := m.LoadMeta(context.Background(), &pb.LoadMetaReq{
			SerfGroup: "grp1",
			Addr:      "requester:1234",
		})
		if !assert.NoError(t, err) {
			return
		}
		assert.NotNil(t, resp)
		var got dumpedCachefs
		if !assert.NoError(t, json.Unmarshal(resp.Meta, &got)) {
			return
		}
		assert.Equal(t, wantNextInode, got.NextInode)
	})
}

// mockMetaServer serves CachefsMeta.LoadMeta with a canned payload.
type mockMetaServer struct {
	pb.UnimplementedCachefsMetaServer
	testMeta []byte
}

func (s *mockMetaServer) LoadMeta(ctx context.Context, req *pb.LoadMetaReq) (*pb.LoadMetaRes, error) {
	return &pb.LoadMetaRes{Meta: s.testMeta}, nil
}

// TestLoadMetaRoundTrip drives metaClient.loadMeta against a real gRPC server,
// which the stub client used above cannot cover.
func TestLoadMetaRoundTrip(t *testing.T) {
	testData := &dumpedCachefs{
		Symlinks: map[uint64][]byte{2: []byte("new")},
	}
	testMetaBytes, err := json.Marshal(testData)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	grpcServer := grpc.NewServer()
	pb.RegisterCachefsMetaServer(grpcServer, &mockMetaServer{testMeta: testMetaBytes})

	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	conn, err := grpc.Dial(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := &metaClient{
		grpcClient: pb.NewCachefsMetaClient(conn),
		conn:       conn,
		addr:       "test-addr",
		atime:      time.Now(),
	}

	loaded, err := client.loadMeta(context.Background(), "test-group")
	assert.NoError(t, err)
	assert.NotNil(t, loaded)
	assert.Equal(t, testData.Symlinks, loaded.Symlinks)
}
