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

package rpcsvc

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestNewRpcService verifies service creation with listener and gRPC server
func TestNewRpcService(t *testing.T) {
	cfg := &RpcConfig{Addr: "127.0.0.1:4872"}

	svc, err := NewRpcService(cfg)
	if err != nil {
		t.Errorf("NewRpcService failed: %v", err)
		return
	}
	svc.Serve()
	defer svc.Stop()

	// Sleep for a while to let serve run
	time.Sleep(time.Second)
	rpcSvc, ok := svc.(*rpcService)
	if !ok {
		t.Error("expected *rpcService")
	}
	if rpcSvc.listener == nil {
		t.Error("expected listener to be created")
	}
	if rpcSvc.grpcServer == nil {
		t.Error("expected grpcServer to be created")
	}
	if rpcSvc.config.Addr == "" {
		t.Error("config.Addr should not be empty")
	}
	assert.Equal(t, rpcSvc.grpcServer, svc.GetServer())
	assert.Equal(t, rpcSvc.config.Addr, svc.Addr())
}

// TestStop ensures Stop is safe to call multiple times
func TestStop(t *testing.T) {
	svc, err := NewRpcService(&RpcConfig{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Errorf("NewRpcService failed: %s", err)
		return
	}
	svc.Serve()

	// Sleep for a while to let serve run
	time.Sleep(time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			svc.Stop()
		}()
	}
	wg.Wait()

	svc.Stop() // Call again

	rpcSvc, ok := svc.(*rpcService)
	if !ok {
		t.Error("expected *rpcService")
	}
	if rpcSvc.grpcServer != nil {
		t.Error("grpcServer should be nil after Stop")
	}
	if rpcSvc.listener != nil {
		t.Error("listener should be nil after Stop")
	}
}

// TestServeAfterStop verifies Serve() is a no-op when the service is already
// stopped: the snapshotted server and listener are nil, and the old code would
// have spawned a goroutine that dereferences them.
func TestServeAfterStop(t *testing.T) {
	svc, err := NewRpcService(&RpcConfig{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Errorf("NewRpcService failed: %s", err)
		return
	}
	svc.Serve()
	svc.Stop()
	svc.Serve() // must not panic
}

// TestServeConcurrentWithStop verifies Serve() and Stop() racing does not
// crash. Stop() nils grpcServer and listener while Serve()'s goroutine reads
// both; before the fix the goroutine could dereference nil. Run with -race to
// actually catch the data race; the crash itself is a nil deref.
func TestServeConcurrentWithStop(t *testing.T) {
	svc, err := NewRpcService(&RpcConfig{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Errorf("NewRpcService failed: %s", err)
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			svc.Serve()
		}()
		go func() {
			defer wg.Done()
			svc.Stop()
		}()
	}
	wg.Wait()
	svc.Stop()
}

// TestGetServerAddrConcurrentWithStop verifies GetServer() and Addr() are
// race-free against Stop(): both snapshot under the lock and return nil/""
// once stopped. Before the fix both read the fields without the lock while
// Stop() nils them. Run with -race to catch the data race.
func TestGetServerAddrConcurrentWithStop(t *testing.T) {
	svc, err := NewRpcService(&RpcConfig{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Errorf("NewRpcService failed: %s", err)
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			svc.Serve()
		}()
		go func() {
			defer wg.Done()
			svc.GetServer()
		}()
		go func() {
			defer wg.Done()
			svc.Addr()
		}()
		go func() {
			defer wg.Done()
			svc.Stop()
		}()
	}
	wg.Wait()
	svc.Stop()

	if svc.GetServer() != nil {
		t.Error("GetServer should return nil after Stop")
	}
	if svc.Addr() != "" {
		t.Error("Addr should return empty string after Stop")
	}
}
