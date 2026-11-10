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

package chunk

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/stretchr/testify/assert"
)

func newTestStore() *store {
	store := &store{
		counterReg: metric.NewCounterRegistry(),
	}
	reg, _ := metric.InitRegister("test", "", "")
	store.InitMetrics(reg)
	return store
}

// serverStore builds a store carrying a freshly-bound cacheroot rpcsvc, for tests
// that construct a cacheroot with server mode (cacherootPort > 0) enabled but do
// not actually serve requests. The caller must Stop() the returned rpcsvc.
func serverStore(t *testing.T) (*store, rpcsvc.RpcService) {
	svc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: "127.0.0.1:0"})
	assert.NoError(t, err)
	return &store{cacherootRpc: svc}, svc
}

func TestTrackerInfo(t *testing.T) {
	tests := []struct {
		info   *trackerInfo
		expect string
	}{
		{
			info: &trackerInfo{
				nodes: []string{},
				group: "",
			},
			expect: "nodes: [], serf group: ",
		},
		{
			info: &trackerInfo{
				nodes: []string{"localhost:8811"},
				group: "",
			},
			expect: "nodes: [localhost:8811], serf group: ",
		},
		{
			info: &trackerInfo{
				nodes: []string{"localhost:8811", "h:p"},
				group: "group",
			},
			expect: "nodes: [localhost:8811 h:p], serf group: group",
		},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expect, tc.info.String())
	}
}

func TestNewCacheRoot(t *testing.T) {
	type test struct {
		cfg *remoteCacheConfig
		err error
	}
	tests := []*test{
		{
			cfg: &remoteCacheConfig{},
			err: nil,
		},
		{
			cfg: &remoteCacheConfig{
				cacherootAddrs: []string{"localhost:8811"},
			},
			err: nil,
		},
		{
			cfg: &remoteCacheConfig{
				cacherootPort: 8823,
			},
			err: nil,
		},
		{
			cfg: &remoteCacheConfig{
				cacherootAddrs: []string{"localhost:8811"},
				cacherootPort:  8824,
			},
			err: nil,
		},
		{
			cfg: &remoteCacheConfig{
				cacherootAddrs: []string{"localhost:8811"},
				cacherootPort:  0,
			},
			err: nil,
		},
		{
			cfg: &remoteCacheConfig{
				cacherootAddrs: []string{"localhost:8811"},
				cacherootPort:  -1,
			},
			err: nil,
		},
		{
			cfg: &remoteCacheConfig{
				cacherootAddrs: []string{"localhost:8811", "127.0.0.1:8822"},
				cacherootPort:  8825,
			},
			err: nil,
		},
	}

	assert := assert.New(t)
	for _, tc := range tests {
		peer := &peerCache{
			config: tc.cfg,
		}
		// The cacheroot server registers on a shared rpcsvc injected via the
		// store; provide one whenever server mode is enabled.
		var svc rpcsvc.RpcService
		if tc.cfg.cacherootServerEnabled() {
			var serr error
			svc, serr = rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: "127.0.0.1:0"})
			assert.NoError(serr)
			peer.store = &store{cacherootRpc: svc}
		}
		cacheroot, err := newCacheRoot(peer, nil)
		if tc.err == nil {
			assert.NoError(err)
			if err == nil {
				cacheroot.stop()
			}
		} else {
			assert.Error(err)
		}
		if svc != nil {
			svc.Stop()
		}
	}

	// Sleep for a while to let go routines exit
	time.Sleep(time.Millisecond * 500)
}

func TestTrackerInfoOp(t *testing.T) {
	assert := assert.New(t)
	trackerInfoTimeout = time.Second
	st, svc := serverStore(t)
	defer svc.Stop()
	peer := &peerCache{
		store: st,
		config: &remoteCacheConfig{
			localIP:        "localhost",
			cacherootPort:  8830,
			cacherootAddrs: []string{"localhost:8830"},
		},
	}
	cacheroot, err := newCacheRoot(peer, nil)
	assert.NoError(err)
	cacherootMgr, ok := cacheroot.(*cacherootManager)
	assert.Equal(true, ok)

	// No info
	_, err = cacherootMgr.lookupInfo("nosuchtarget", 0)
	assert.Equal(errTrackerNotFound, err)

	// Insert and lookup info
	info := &trackerInfo{
		nodes: []string{"localhost:8811"},
		group: "testgroup",
	}
	cacherootMgr.insertInfo("testtarget", 0, info)
	got, err := cacherootMgr.lookupInfo("testtarget", 0)
	assert.NoError(err)
	assert.Equal(info, got)

	// Sleep 5s and let info expire
	time.Sleep(trackerInfoTimeout)
	got, err = cacherootMgr.lookupInfo("testtarget", 0)
	assert.Equal(errTrackerTimeout, err)
	// NotFound after timeout
	got, err = cacherootMgr.lookupInfo("testtarget", 0)
	assert.Equal(errTrackerNotFound, err)

	// Insert again, and let gc to delete info
	cacherootMgr.insertInfo("testtarget", 0, info)
	got, err = cacherootMgr.lookupInfo("testtarget", 0)
	assert.NoError(err)
	assert.Equal(info, got)
	time.Sleep(trackerInfoTimeout)
	go cacherootMgr.gcTrackerInfo()
	time.Sleep(time.Microsecond * 50)
	got, err = cacherootMgr.lookupInfo("testtarget", 0)
	assert.Equal(errTrackerNotFound, err)
}

func TestCacheRootQuery(t *testing.T) {
	assert := assert.New(t)
	addr := "localhost:8831"
	group := "testgroup"
	ctx := context.Background()
	st := newTestStore()
	// The cacheroot server registers on a shared rpcsvc injected via the store.
	svc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: addr})
	assert.NoError(err)
	st.cacherootRpc = svc
	peer := &peerCache{
		store: st,
		config: &remoteCacheConfig{
			blockSize:      4 << 20,
			localIP:        "localhost",
			cacherootPort:  8831,
			cacherootAddrs: []string{addr},
			serfGroup:      group,
		},
	}
	cacheroot, err := newCacheRoot(peer, nil)
	assert.NoError(err)
	cacherootMgr, ok := cacheroot.(*cacherootManager)
	assert.Equal(true, ok)
	svc.Serve()
	defer svc.Stop()

	target := "testtarget"
	blk := uint32(0)
	nodes, grp, err := cacheroot.query(ctx, target, blk)
	assert.Equal(errTrackerNotFound, err)

	// Insert and query again
	nodes = nil
	grp = ""
	err = nil
	info := &trackerInfo{
		nodes: []string{"localhost:8811"},
		group: "testgroup",
	}
	cacherootMgr.insertInfo(target, blk, info)
	nodes, grp, err = cacheroot.query(ctx, target, blk)
	assert.Equal([]string{"localhost:8811"}, nodes)
	assert.Equal(group, grp)
	assert.NoError(err)
	cacheroot.stop()
}

func TestTrackerInfoWithEmptyNodes(t *testing.T) {
	assert := assert.New(t)

	info := &trackerInfo{
		nodes: []string{},
		group: "testgroup",
	}

	// Test String() method with empty nodes
	expected := "nodes: [], serf group: testgroup"
	assert.Equal(expected, info.String())
}

func TestTrackerInfoWithMultipleNodes(t *testing.T) {
	assert := assert.New(t)

	info := &trackerInfo{
		nodes: []string{"node1:8080", "node2:8081", "node3:8082"},
		group: "testgroup",
	}

	// Test String() method with multiple nodes
	expected := "nodes: [node1:8080 node2:8081 node3:8082], serf group: testgroup"
	assert.Equal(expected, info.String())
}

func TestCacherootManagerLookupInfo(t *testing.T) {
	assert := assert.New(t)

	st, svc := serverStore(t)
	defer svc.Stop()
	peer := &peerCache{
		store: st,
		config: &remoteCacheConfig{
			localIP:       "localhost",
			cacherootPort: 8840,
		},
	}
	cacheroot, err := newCacheRoot(peer, nil)
	assert.NoError(err)
	cacherootMgr, ok := cacheroot.(*cacherootManager)
	assert.Equal(true, ok)

	// Test lookup non-existent info
	_, err = cacherootMgr.lookupInfo("nonexistent", 0)
	assert.Equal(errTrackerNotFound, err)

	// Insert info and test successful lookup
	info := &trackerInfo{
		nodes: []string{"localhost:8811"},
		group: "testgroup",
		ctime: time.Now(),
	}
	cacherootMgr.insertInfo("testtarget", 10, info)

	result, err := cacherootMgr.lookupInfo("testtarget", 10)
	assert.NoError(err)
	assert.Equal(info, result)
	assert.Equal([]string{"localhost:8811"}, result.nodes)
	assert.Equal("testgroup", result.group)
}

func TestCacherootManagerInsertInfo(t *testing.T) {
	assert := assert.New(t)

	st, svc := serverStore(t)
	defer svc.Stop()
	peer := &peerCache{
		store: st,
		config: &remoteCacheConfig{
			localIP:       "localhost",
			cacherootPort: 8842,
		},
	}
	cacheroot, err := newCacheRoot(peer, nil)
	assert.NoError(err)
	cacherootMgr, ok := cacheroot.(*cacherootManager)
	assert.Equal(true, ok)

	// Insert info
	info := &trackerInfo{
		nodes: []string{"localhost:8811", "localhost:8812"},
		group: "testgroup2",
	}
	cacherootMgr.insertInfo("testtarget2", 30, info)

	// Verify the info was inserted with current time
	result, err := cacherootMgr.lookupInfo("testtarget2", 30)
	assert.NoError(err)
	assert.Equal([]string{"localhost:8811", "localhost:8812"}, result.nodes)
	assert.Equal("testgroup2", result.group)
	// Check that ctime was updated to current time
	assert.True(result.ctime.Add(time.Second).After(time.Now()))
}

func TestCacherootManagerQueryWithEmptyNodes(t *testing.T) {
	assert := assert.New(t)

	st := newTestStore()
	svc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: "127.0.0.1:0"})
	assert.NoError(err)
	defer svc.Stop()
	st.cacherootRpc = svc
	peer := &peerCache{
		store: st,
		config: &remoteCacheConfig{
			localIP:        "localhost",
			cacherootPort:  8843,
			cacherootAddrs: []string{"localhost:8843"}, // This should fail to connect
		},
	}
	cacheroot, err := newCacheRoot(peer, nil)
	assert.NoError(err)
	cacherootMgr, ok := cacheroot.(*cacherootManager)
	assert.Equal(true, ok)

	// Insert info with empty nodes - this should be handled properly
	info := &trackerInfo{
		nodes: []string{}, // Empty nodes
		group: "testgroup",
	}
	cacherootMgr.insertInfo("emptytarget", 40, info)

	// This should return errTrackerNotFound because nodes are empty
	_, _, err = cacheroot.query(context.Background(), "emptytarget", 40)
	assert.Equal(errTrackerNotFound, err)
}

func TestCacherootManagerStop(t *testing.T) {
	assert := assert.New(t)

	st, svc := serverStore(t)
	defer svc.Stop()
	peer := &peerCache{
		store: st,
		config: &remoteCacheConfig{
			localIP:       "localhost",
			cacherootPort: 8844,
		},
	}
	cacheroot, err := newCacheRoot(peer, nil)
	assert.NoError(err)

	// Test stop functionality
	err = cacheroot.stop()
	assert.NoError(err)
}

func TestCacherootManagerMultipleTargets(t *testing.T) {
	assert := assert.New(t)

	st, svc := serverStore(t)
	defer svc.Stop()
	peer := &peerCache{
		store: st,
		config: &remoteCacheConfig{
			localIP:       "localhost",
			cacherootPort: 8845,
		},
	}
	cacheroot, err := newCacheRoot(peer, nil)
	assert.NoError(err)
	cacherootMgr, ok := cacheroot.(*cacherootManager)
	assert.Equal(true, ok)

	// Insert info for multiple targets
	info1 := &trackerInfo{
		nodes: []string{"localhost:8811"},
		group: "group1",
	}
	info2 := &trackerInfo{
		nodes: []string{"localhost:8812"},
		group: "group2",
	}

	cacherootMgr.insertInfo("target1", 50, info1)
	cacherootMgr.insertInfo("target2", 60, info2)

	// Verify both targets can be looked up correctly
	result1, err := cacherootMgr.lookupInfo("target1", 50)
	assert.NoError(err)
	assert.Equal([]string{"localhost:8811"}, result1.nodes)
	assert.Equal("group1", result1.group)

	result2, err := cacherootMgr.lookupInfo("target2", 60)
	assert.NoError(err)
	assert.Equal([]string{"localhost:8812"}, result2.nodes)
	assert.Equal("group2", result2.group)
}

func TestCacherootManagerConcurrentAccess(t *testing.T) {
	assert := assert.New(t)

	st, svc := serverStore(t)
	defer svc.Stop()
	peer := &peerCache{
		store: st,
		config: &remoteCacheConfig{
			localIP:       "localhost",
			cacherootPort: 8850,
		},
	}
	cacheroot, err := newCacheRoot(peer, nil)
	assert.NoError(err)
	cacherootMgr, ok := cacheroot.(*cacherootManager)
	assert.Equal(true, ok)

	// Test concurrent access to the same target and block
	done := make(chan bool, 10)

	// Insert initial info
	initialInfo := &trackerInfo{
		nodes: []string{"localhost:8811"},
		group: "concurrent-group",
	}
	cacherootMgr.insertInfo("concurrent-target", 100, initialInfo)

	// Run multiple goroutines that access the same data
	for i := 0; i < 10; i++ {
		go func(id int) {
			// Query the same target and block from multiple goroutines
			for j := 0; j < 10; j++ {
				result, err := cacherootMgr.lookupInfo("concurrent-target", 100)
				assert.NoError(err)
				assert.Equal([]string{"localhost:8811"}, result.nodes)
				assert.Equal("concurrent-group", result.group)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify the data is still correct after concurrent access
	result, err := cacherootMgr.lookupInfo("concurrent-target", 100)
	assert.NoError(err)
	assert.Equal([]string{"localhost:8811"}, result.nodes)
	assert.Equal("concurrent-group", result.group)
}

func TestCacherootManagerConcurrentInsertAndQuery(t *testing.T) {
	assert := assert.New(t)

	st, svc := serverStore(t)
	defer svc.Stop()
	peer := &peerCache{
		store: st,
		config: &remoteCacheConfig{
			localIP:       "localhost",
			cacherootPort: 8851,
		},
	}
	cacheroot, err := newCacheRoot(peer, nil)
	assert.NoError(err)
	cacherootMgr, ok := cacheroot.(*cacherootManager)
	assert.Equal(true, ok)

	done := make(chan bool, 20)

	// Run goroutines that insert data
	for i := 0; i < 10; i++ {
		go func(id int) {
			info := &trackerInfo{
				nodes: []string{fmt.Sprintf("node-%d:%d", id, 8000+id)},
				group: fmt.Sprintf("group-%d", id),
			}
			cacherootMgr.insertInfo(fmt.Sprintf("target-%d", id), uint32(id), info)
			done <- true
		}(i)
	}

	// Run goroutines that query data
	for i := 0; i < 10; i++ {
		go func(id int) {
			// Wait a bit for the insert to happen
			time.Sleep(time.Millisecond * 10)
			result, err := cacherootMgr.lookupInfo(fmt.Sprintf("target-%d", id), uint32(id))
			if err == nil {
				// If the target exists, verify its content
				expectedNode := fmt.Sprintf("node-%d:%d", id, 8000+id)
				expectedGroup := fmt.Sprintf("group-%d", id)
				assert.Equal([]string{expectedNode}, result.nodes)
				assert.Equal(expectedGroup, result.group)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 20; i++ {
		<-done
	}
}

func TestCacherootManagerConcurrentQuery(t *testing.T) {
	assert := assert.New(t)

	st, svc := serverStore(t)
	defer svc.Stop()
	peer := &peerCache{
		store: st,
		config: &remoteCacheConfig{
			localIP:        "localhost",
			cacherootPort:  8852,
			cacherootAddrs: []string{"nonexistent-server:9999"}, // This will fail, so it will use local cache
		},
	}
	cacheroot, err := newCacheRoot(peer, nil)
	assert.NoError(err)
	cacherootMgr, ok := cacheroot.(*cacherootManager)
	assert.Equal(true, ok)

	// Insert info that will be queried concurrently
	info := &trackerInfo{
		nodes: []string{"localhost:8811"},
		group: "test-group",
	}
	cacherootMgr.insertInfo("shared-target", 200, info)

	done := make(chan bool, 5)

	// Run multiple concurrent queries
	for i := 0; i < 5; i++ {
		go func() {
			nodes, group, err := cacheroot.query(context.Background(), "shared-target", 200)
			assert.NoError(err)
			assert.Equal([]string{"localhost:8811"}, nodes)
			assert.Equal("test-group", group)
			done <- true
		}()
	}

	// Wait for all concurrent queries to complete
	for i := 0; i < 5; i++ {
		<-done
	}
}

func TestCacherootManagerEmptyNodesCleanup(t *testing.T) {
	assert := assert.New(t)

	st, svc := serverStore(t)
	defer svc.Stop()
	peer := &peerCache{
		store: st,
		config: &remoteCacheConfig{
			localIP:       "localhost",
			cacherootPort: 8853,
		},
	}
	cacheroot, err := newCacheRoot(peer, nil)
	assert.NoError(err)
	cacherootMgr, ok := cacheroot.(*cacherootManager)
	assert.Equal(true, ok)

	// Insert info with empty nodes
	info := &trackerInfo{
		nodes: []string{}, // Empty nodes should cause cleanup
		group: "test-group",
	}
	cacherootMgr.insertInfo("empty-nodes-target", 300, info)

	// Try to lookup - should return errTrackerNotFound because nodes are empty
	_, err = cacherootMgr.lookupInfo("empty-nodes-target", 300)
	assert.Equal(errTrackerNotFound, err)

	// Verify the entry was cleaned up after access
	_, err = cacherootMgr.lookupInfo("empty-nodes-target", 300)
	assert.Equal(errTrackerNotFound, err)
}
