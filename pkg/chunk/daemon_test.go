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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/transfer"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockStore struct {
	mock.Mock
}

func (m *MockStore) Members() []string {
	return []string{"member1", "member2"}
}

func (m *MockStore) Stop() {
}

func findAvailablePort() int {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func NewStoreConfig() (*StoreConfig, error) {
	tempDir := os.TempDir()
	ip := "127.0.0.1"
	cachePort := findAvailablePort()
	cacheAddr := fmt.Sprintf("%s:%d", ip, cachePort)
	config := &StoreConfig{
		BlockSize:     4 * 1024 * 1024,
		CacheDir:      fmt.Sprintf("%s/cache_test_dir", tempDir), // /tmp/cachefs
		CacheSize:     1 * 1024 * 1024 * 1024,
		FreeSpace:     0.1,
		Cleanup:       true,
		CacheEviction: HashCleanup,
		Transport:     transfer.TCPTransport,

		UploadLimit:   10 * 1024 * 1024,
		DownloadLimit: 10 * 1024 * 1024,
		CacheAddr:     cacheAddr,
		CacheIP:       ip,
		CachePort:     cachePort,
		PeerTimeout:   30,

		Socket: fmt.Sprintf("%s/cachefs.sock", tempDir),
	}

	if err := os.MkdirAll(config.CacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %v", err)
	}

	return config, nil
}

func TestNewDaemon(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	reg := prometheus.NewRegistry()
	ss := serf.NewSerfService(serf.DefaultSerfConfig(), reg)

	rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: mockStoreConfig.CacheAddr})
	assert.NoError(t, err, "new rpc service")

	d := NewDaemon(mockStoreConfig, rpc, ss, reg)

	assert.NotNil(t, d)
	assert.IsType(t, &daemon{}, d)

	defer os.RemoveAll(mockStoreConfig.CacheDir)
	time.Sleep(100 * time.Millisecond)
	d.Stop()
}

func TestDaemon_Serve(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	os.Remove(mockStoreConfig.Socket)

	ss := serf.NewSerfService(serf.DefaultSerfConfig(), nil)
	rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: mockStoreConfig.CacheAddr})
	assert.NoError(t, err, "new rpc service")
	d := NewDaemon(mockStoreConfig, rpc, ss, nil)

	defer func() {
		d.Stop()
		os.RemoveAll(mockStoreConfig.CacheDir)
	}()

	go func() {
		err := d.Serve()
		assert.NoError(t, err)
	}()

	time.Sleep(100 * time.Millisecond)

	_, err = os.Stat(mockStoreConfig.Socket)
	assert.False(t, os.IsNotExist(err))
}

func TestDaemon_Stop(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	ss := serf.NewSerfService(serf.DefaultSerfConfig(), nil)
	rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: mockStoreConfig.CacheAddr})
	assert.NoError(t, err, "new rpc service")
	d := NewDaemon(mockStoreConfig, rpc, ss, nil)

	defer func() {
		os.RemoveAll(mockStoreConfig.CacheDir)
	}()

	d.Serve()
	time.Sleep(1 * time.Second)
	d.Stop()

	_, err = os.Stat(mockStoreConfig.Socket)
	assert.True(t, os.IsNotExist(err))
}

func TestGetCacheConnKey(t *testing.T) {
	target := "localhost"
	block := uint32(1024)

	key := GetCacheConnKey(target, block)
	assert.Equal(t, "localhost:1024", key)
}

func TestDaemon_CmtBlock(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)
	mockStoreConfig.BlockSize = 4096

	mockBCache := new(MockLocalCache)

	store := &store{
		config:     mockStoreConfig,
		bcache:     mockBCache,
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}
	d := &daemon{
		store: store,
	}

	t.Run("block len too large", func(t *testing.T) {
		req := &Req{
			Info: []*ReqInfo{{
				Target:    "target1",
				BlockNum:  1,
				BlockSize: 4096,
				Size:      8192,
			}},
		}
		resp, status := d.CmtBlock(req)
		assert.Equal(t, STS_FAIL, status)
		assert.Equal(t, "cache data size(8192) greater than block size(4096)", resp.Message)
		mockBCache.AssertNotCalled(t, "commitBlockFile")
	})

	t.Run("block size mismatch", func(t *testing.T) {
		req := &Req{
			Info: []*ReqInfo{{
				Target:    "target2",
				BlockNum:  2,
				BlockSize: 2048,
				Size:      2048,
			}},
		}
		resp, status := d.CmtBlock(req)
		assert.Equal(t, STS_FAIL, status)
		assert.Equal(t, "block size mismatch: 2048 != 4096", resp.Message)
		mockBCache.AssertNotCalled(t, "commitBlockFile")
	})

	t.Run("commitBlockFile error", func(t *testing.T) {
		mockBCache.On("commitBlockFile", "target3", uint32(3), true).
			Return(errors.New("mock error")).Once()

		req := &Req{
			Info: []*ReqInfo{{
				Target:    "target3",
				BlockNum:  3,
				BlockSize: 4096,
				Size:      4096,
			}},
		}
		resp, status := d.CmtBlock(req)
		assert.Equal(t, STS_FAIL, status)
		assert.Equal(t, "commit cache file: mock error", resp.Message)
		mockBCache.AssertExpectations(t)
	})

	t.Run("successful block commit", func(t *testing.T) {
		mockBCache.On("commitBlockFile", "target4", uint32(4), true).
			Return(nil).Once()

		req := &Req{
			Info: []*ReqInfo{{
				Target:    "target4",
				BlockNum:  4,
				BlockSize: 4096,
				Size:      4096,
			}},
		}
		resp, status := d.CmtBlock(req)
		assert.Equal(t, STS_OK, status)
		assert.Equal(t, "", resp.Message)
		mockBCache.AssertExpectations(t)
	})
}

func TestDaemon_PutBlock(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)
	mockStoreConfig.BlockSize = 4096
	mockBCache := new(MockLocalCache)

	store := &store{
		config:     mockStoreConfig,
		bcache:     mockBCache,
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}
	d := &daemon{
		store: store,
	}

	t.Run("block_len_too_large", func(t *testing.T) {
		req := &Req{
			Info: []*ReqInfo{{
				Target:    "target1",
				BlockNum:  1,
				BlockSize: 4096,
				Size:      8192,
			}},
		}
		resp, file, status := d.PutBlock(req)
		assert.Nil(t, file)
		assert.Equal(t, STS_FAIL, status)
		assert.Equal(t, "cache data size(8192) greater than block size(4096)", resp.Message)
	})

	t.Run("block_size_mismatch", func(t *testing.T) {
		req := &Req{
			Info: []*ReqInfo{{
				Target:    "target2",
				BlockNum:  2,
				BlockSize: 2048,
				Size:      2048,
			}},
		}
		resp, file, status := d.PutBlock(req)
		assert.Nil(t, file)
		assert.Equal(t, STS_FAIL, status)
		assert.Equal(t, "block size mismatch: 2048 != 4096", resp.Message)
	})

	t.Run("already_cached", func(t *testing.T) {
		mockBCache.On("check", "target3", uint32(3)).Return(true).Once()

		req := &Req{
			Info: []*ReqInfo{{
				Target:    "target3",
				BlockNum:  3,
				BlockSize: 4096,
				Size:      4096,
			}},
		}
		resp, file, status := d.PutBlock(req)
		assert.Nil(t, file)
		assert.Equal(t, STS_EXISTS, status)
		assert.Equal(t, uint64(4096), resp.Size)
		mockBCache.AssertCalled(t, "check", "target3", uint32(3))
	})

	t.Run("prepare_cache_file_fail", func(t *testing.T) {
		mockBCache.On("check", "target4", uint32(4)).Return(false).Once()
		mockBCache.On("prepareBlockFile", "target4", uint32(4), uint32(4096)).
			Return(nil, errors.New("mock prepare error")).Once()

		req := &Req{
			Info: []*ReqInfo{{
				Target:    "target4",
				BlockNum:  4,
				BlockSize: 4096,
				Size:      4096,
			}},
		}
		resp, file, status := d.PutBlock(req)
		assert.Nil(t, file)
		assert.Equal(t, STS_FAIL, status)
		assert.Equal(t, "prepare cache file: mock prepare error", resp.Message)
	})

	t.Run("prepare_cache_file_success", func(t *testing.T) {
		mockBCache.On("check", "target5", uint32(5)).Return(false).Once()
		mockFile := new(os.File)
		mockBCache.On("prepareBlockFile", "target5", uint32(5), uint32(4096)).
			Return(mockFile, nil).Once()

		req := &Req{
			Info: []*ReqInfo{{
				Target:    "target5",
				BlockNum:  5,
				BlockSize: 4096,
				Size:      4096,
			}},
		}
		resp, file, status := d.PutBlock(req)
		assert.NotNil(t, file)
		assert.Equal(t, STS_OK, status)
		assert.Equal(t, "", resp.Message)
		assert.Equal(t, uint64(4096), resp.Size)
		mockBCache.AssertCalled(t, "prepareBlockFile", "target5", uint32(5), uint32(4096))
	})
}

func TestDaemon_HandleRequest(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)
	mockStoreConfig.BlockSize = 4096
	mockBCache := new(MockLocalCache)

	store := &store{
		config:     mockStoreConfig,
		bcache:     mockBCache,
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}

	mockConn := new(MockUnixConn)
	mockKey := uint64(12345)
	d := &daemon{
		store: store,
		connections: map[uint64]unixConn{
			mockKey: mockConn,
		},
	}

	t.Run("getConnection_fail", func(t *testing.T) {
		mockReq := &Req{
			Header: &MsgHdr{
				Id:   1,
				Type: REQ_PUTBLOCK,
			},
			Info: []*ReqInfo{
				{
					Target:    "target1",
					BlockNum:  1,
					BlockSize: 4096,
					Size:      4096,
				},
			},
		}
		respCh := make(chan *daemonResp)

		d.handleRequest(uint64(1234), mockReq, respCh)

		select {
		case resp := <-respCh:
			t.Errorf("Expected no response, but got: %v", resp)
		default:
		}
	})

	t.Run("test_PutBlock", func(t *testing.T) {
		mockReq := &Req{
			Header: &MsgHdr{
				Type: REQ_PUTBLOCK,
			},
			Info: []*ReqInfo{
				{
					Target:    "target2",
					BlockNum:  2,
					BlockSize: 4096,
					Size:      4096,
				},
			},
		}
		respCh := make(chan *daemonResp)

		mockBCache.On("check", "target2", uint32(2)).Return(false).Once()
		mockFile, err := os.CreateTemp("", "mockFile")
		defer os.Remove(mockFile.Name())
		assert.NoError(t, err)
		mockBCache.On("prepareBlockFile", "target2", uint32(2), uint32(4096)).
			Return(mockFile, nil).Once()

		go d.handleRequest(mockKey, mockReq, respCh)

		resp := <-respCh
		assert.NotNil(t, resp)
		assert.Equal(t, RESP_PUTBLOCK, resp.resp.Header.Type)
		assert.Equal(t, STS_OK, resp.resp.Header.Status)
		assert.NoError(t, resp.resp.File.Close())

		mockBCache.AssertCalled(t, "prepareBlockFile", "target2", uint32(2), uint32(4096))
	})

	t.Run("test_GetBlock_blklen_too_large", func(t *testing.T) {
		blk := uint32(3)
		blklen := uint32(4096)
		target := "target3"
		mockReq := &Req{
			Header: &MsgHdr{
				Type: REQ_GETBLOCK,
			},
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  blk,
					BlockSize: 4097,
					Size:      blklen,
				},
			},
		}
		respCh := make(chan *daemonResp)

		go d.handleRequest(mockKey, mockReq, respCh)

		resp := <-respCh
		assert.NotNil(t, resp)
		assert.Equal(t, STS_FAIL, resp.resp.Header.Status)
	})

	t.Run("test_CmtBlock_blklen_too_large", func(t *testing.T) {
		blk := uint32(3)
		blklen := uint32(4096)
		target := "target4"
		mockReq := &Req{
			Header: &MsgHdr{
				Type: REQ_CMTBLOCK,
			},
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  blk,
					BlockSize: 4097,
					Size:      blklen,
				},
			},
		}
		respCh := make(chan *daemonResp)

		go d.handleRequest(mockKey, mockReq, respCh)

		resp := <-respCh
		assert.NotNil(t, resp)
		assert.Equal(t, STS_FAIL, resp.resp.Header.Status)
	})

	t.Run("handle_invalid_request_type", func(t *testing.T) {
		mockReq := &Req{
			Header: &MsgHdr{
				Type: uint8(255),
			},
		}
		respCh := make(chan *daemonResp)

		assert.Panics(t, func() {
			d.handleRequest(mockKey, mockReq, respCh)
		})
	})
}

func TestGetBlock(t *testing.T) {
	mockLocalCache := new(MockLocalCache)
	mockRemoteCache := new(MockRemoteCache)
	mockHist := new(MockHistogram)

	mockHist.On("Observe", mock.Anything).Return()

	storeConfig := &StoreConfig{
		BlockSize: 4096,
	}
	store := &store{
		config:             storeConfig,
		bcache:             mockLocalCache,
		rcache:             mockRemoteCache,
		remoteHits:         metric.NewCounter(prometheus.CounterOpts{}),
		remoteMiss:         metric.NewCounter(prometheus.CounterOpts{}),
		remoteHitBytes:     metric.NewCounter(prometheus.CounterOpts{}),
		remoteMissBytes:    metric.NewCounter(prometheus.CounterOpts{}),
		remoteReadHist:     mockHist,
		remoteReadInFlight: 0,
		pendingOps:         utils.NewPendingOpsManager(500 * time.Millisecond),
	}
	daemon := &daemon{
		store:          store,
		cacheHits:      prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheMiss:      prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheHitBytes:  prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheMissBytes: prometheus.NewCounter(prometheus.CounterOpts{}),
	}

	target := "test-target"

	t.Run("Block length too large", func(t *testing.T) {
		req := &Req{
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  1,
					BlockSize: 4096,
					Size:      8192,
				},
			},
		}

		expectedResp := &RespInfo{
			Size:    8192,
			Message: "requested size(8192) greater than block size(4096)",
		}

		resp, file, code := daemon.GetBlock(req)
		assert.Equal(t, expectedResp, resp)
		assert.Nil(t, file)
		assert.Equal(t, STS_FAIL, code)
	})

	t.Run("Block size mismatch", func(t *testing.T) {
		req := &Req{
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  1,
					BlockSize: 2048,
					Size:      1024,
				},
			},
		}

		expectedResp := &RespInfo{
			Size:    1024,
			Message: "block size mismatch: 2048 != 4096",
		}

		resp, file, code := daemon.GetBlock(req)
		assert.Equal(t, expectedResp, resp)
		assert.Nil(t, file)
		assert.Equal(t, STS_FAIL, code)
	})

	t.Run("Cache not found and loadRemote fails", func(t *testing.T) {
		req := &Req{
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  1,
					BlockSize: 4096,
					Size:      1024,
				},
			},
		}

		mockLocalCache.On("getBlockFile", target, uint32(1)).Return(nil, errors.New("not found")).Once()
		mockRemoteCache.On("load", mock.Anything, target, uint32(1), uint32(1024)).Return(nil, errors.New("remote load error")).Once()

		expectedResp := &RespInfo{
			Size:    1024,
			Message: "cache not found",
		}

		resp, file, code := daemon.GetBlock(req)
		assert.Equal(t, expectedResp, resp)
		assert.Nil(t, file)
		assert.Equal(t, STS_NOTFOUND, code)

		mockLocalCache.AssertExpectations(t)
	})

	t.Run("Success to GetBlock", func(t *testing.T) {
		ctx := context.Background()
		blk := uint32(1)
		blklen := uint32(1024)
		page := &Page{
			Data: make([]byte, blklen),
		}

		mockFile0, err := os.CreateTemp("", "testfile")
		if err != nil {
			log.Fatalf("failed to create temp file: %v", err)
		}
		defer os.Remove(mockFile0.Name())

		mockFile, err := os.CreateTemp("", "testfile")
		if err != nil {
			t.Errorf("failed to create temp file: %v", err)
			return
		}
		defer os.Remove(mockFile.Name())

		_, err = mockFile.Write(bytes.Repeat([]byte{0}, int(blklen)))
		if err != nil {
			t.Errorf("failed to write to temp file: %v", err)
			return
		}
		mockFile.Seek(0, 0)

		req := &Req{
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  blk,
					BlockSize: 4096,
					Size:      blklen,
				},
			},
		}

		mockLocalCache.On("getBlockFile", target, blk).Return(nil, errors.New("not found")).Once()
		mockRemoteCache.On("load", ctx, target, blk, blklen).Return(page, nil).Once()
		mockLocalCache.On("prepareBlockFile", target, blk, blklen).Return(mockFile0, nil).Once()
		mockLocalCache.On("commitBlockFile", target, blk, true).Return(nil).Once()
		mockLocalCache.On("getBlockFile", target, blk).Return(mockFile, nil).Once()

		resp, file, code := daemon.GetBlock(req)

		assert.NotNil(t, resp)
		assert.Equal(t, STS_OK, code)
		assert.Equal(t, mockFile, file)

		mockLocalCache.AssertExpectations(t)
		mockRemoteCache.AssertExpectations(t)
	})

	t.Run("Fail at last", func(t *testing.T) {
		ctx := context.Background()
		blk := uint32(1)
		blklen := uint32(1024)
		page := &Page{
			Data: make([]byte, blklen),
		}

		mockFile, err := os.CreateTemp("", "testfile")
		if err != nil {
			t.Errorf("failed to create temp file: %v", err)
			return
		}
		defer os.Remove(mockFile.Name())

		_, err = mockFile.Write(bytes.Repeat([]byte{0}, int(blklen)))
		if err != nil {
			t.Errorf("failed to write to temp file: %v", err)
			return
		}
		mockFile.Seek(0, 0)

		req := &Req{
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  blk,
					BlockSize: 4096,
					Size:      blklen,
				},
			},
		}

		mockLocalCache.On("getBlockFile", target, blk).Return(nil, errors.New("not found")).Once()
		mockRemoteCache.On("load", ctx, target, blk, blklen).Return(page, nil).Once()
		mockLocalCache.On("prepareBlockFile", target, blk, blklen).Return(mockFile, nil).Once()
		mockLocalCache.On("commitBlockFile", target, blk, true).Return(nil).Once()
		mockLocalCache.On("getBlockFile", target, blk).Return(mockFile, nil).Once()

		resp, file, code := daemon.GetBlock(req)

		assert.NotNil(t, resp)
		assert.Equal(t, STS_FAIL, code)
		assert.Nil(t, file)

		mockLocalCache.AssertExpectations(t)
		mockRemoteCache.AssertExpectations(t)

		mockFile1, err := os.CreateTemp("", "testfile1")
		if err != nil {
			t.Errorf("failed to create temp file: %v", err)
			return
		}
		defer os.Remove(mockFile1.Name())

		_, err = mockFile1.Write(bytes.Repeat([]byte{0}, int(blklen+1)))
		if err != nil {
			t.Errorf("failed to write to temp file: %v", err)
			return
		}
		mockFile1.Seek(0, 0)

		mockLocalCache.On("getBlockFile", target, blk).Return(nil, errors.New("not found")).Once()
		mockRemoteCache.On("load", ctx, target, blk, blklen).Return(page, nil).Once()
		mockLocalCache.On("prepareBlockFile", target, blk, blklen).Return(nil, nil).Once()
		mockLocalCache.On("getBlockFile", target, blk).Return(mockFile1, nil).Once()

		resp, file, code = daemon.GetBlock(req)

		assert.NotNil(t, resp)
		assert.Equal(t, STS_FAIL, code)
		assert.Nil(t, file)

		mockLocalCache.AssertExpectations(t)
		mockRemoteCache.AssertExpectations(t)
	})

	t.Run("Cache not found, loadRemote succeeds, but getBlockFile 2nd failed", func(t *testing.T) {
		ctx := context.Background()
		blk := uint32(1)
		blklen := uint32(1024)
		page := &Page{
			Data: make([]byte, blklen),
		}
		mockFile, err := os.CreateTemp("", "testfile")
		if err != nil {
			log.Fatalf("failed to create temp file: %v", err)
		}
		defer os.Remove(mockFile.Name())

		req := &Req{
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  blk,
					BlockSize: 4096,
					Size:      blklen,
				},
			},
		}

		mockLocalCache.On("getBlockFile", target, blk).Return(nil, errors.New("not found")).Twice()
		mockRemoteCache.On("load", ctx, target, blk, blklen).Return(page, nil).Once()
		mockLocalCache.On("prepareBlockFile", target, blk, blklen).Return(mockFile, nil).Once()
		mockLocalCache.On("commitBlockFile", target, blk, true).Return(nil).Once()

		expectedResp := &RespInfo{
			Size: uint64(blklen),
		}

		resp, file, code := daemon.GetBlock(req)
		assert.Equal(t, expectedResp, resp)
		assert.Nil(t, file)
		assert.Equal(t, STS_NOTFOUND, code)

		mockLocalCache.AssertExpectations(t)
		mockRemoteCache.AssertExpectations(t)
	})
}

func TestFillBlock(t *testing.T) {
	mockLocalCache := new(MockLocalCache)
	mockRemoteCache := new(MockRemoteCache)
	mockHist := new(MockHistogram)

	mockHist.On("Observe", mock.Anything).Return()

	storeConfig := &StoreConfig{
		BlockSize: 4096,
	}
	store := &store{
		config:             storeConfig,
		bcache:             mockLocalCache,
		rcache:             mockRemoteCache,
		remoteHits:         metric.NewCounter(prometheus.CounterOpts{}),
		remoteMiss:         metric.NewCounter(prometheus.CounterOpts{}),
		remoteHitBytes:     metric.NewCounter(prometheus.CounterOpts{}),
		remoteMissBytes:    metric.NewCounter(prometheus.CounterOpts{}),
		remoteReadHist:     mockHist,
		remoteReadInFlight: 0,
		pendingOps:         utils.NewPendingOpsManager(500 * time.Millisecond),
	}
	daemon := &daemon{
		store:          store,
		cacheHits:      prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheMiss:      prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheHitBytes:  prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheMissBytes: prometheus.NewCounter(prometheus.CounterOpts{}),
	}

	target := "test-target"

	t.Run("Block length too large", func(t *testing.T) {
		req := &Req{
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  1,
					BlockSize: 4096,
					Size:      8192,
				},
			},
		}

		expectedResp := &RespInfo{
			Size:    8192,
			Message: "requested size(8192) greater than block size(4096)",
		}

		resp, code := daemon.FillBlock(req)
		assert.Equal(t, expectedResp, resp)
		assert.Equal(t, STS_FAIL, code)
	})

	t.Run("Block size mismatch", func(t *testing.T) {
		req := &Req{
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  1,
					BlockSize: 2048,
					Size:      1024,
				},
			},
		}

		expectedResp := &RespInfo{
			Size:    1024,
			Message: "block size mismatch: 2048 != 4096",
		}

		resp, code := daemon.FillBlock(req)
		assert.Equal(t, expectedResp, resp)
		assert.Equal(t, STS_FAIL, code)
	})

	t.Run("Cache not found and loadRemote not found", func(t *testing.T) {
		req := &Req{
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  1,
					BlockSize: 4096,
					Size:      1024,
				},
			},
		}

		mockLocalCache.On("check", target, uint32(1)).Return(false).Once()
		mockRemoteCache.On("load", mock.Anything, target, uint32(1), uint32(1024)).Return(nil, ErrNotFound).Once()

		expectedResp := &RespInfo{
			Size:    1024,
			Message: "cache not found",
		}

		resp, code := daemon.FillBlock(req)
		assert.Equal(t, expectedResp, resp)
		assert.Equal(t, STS_NOTFOUND, code)

		mockLocalCache.AssertExpectations(t)
		mockRemoteCache.AssertExpectations(t)
	})

	t.Run("Cache not found and loadRemote fails", func(t *testing.T) {
		req := &Req{
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  1,
					BlockSize: 4096,
					Size:      1024,
				},
			},
		}

		mockLocalCache.On("check", target, uint32(1)).Return(false).Once()
		mockRemoteCache.On("load", mock.Anything, target, uint32(1), uint32(1024)).Return(nil, errors.New("loadRemote fails")).Once()

		expectedResp := &RespInfo{
			Size:    1024,
			Message: "loadRemote fails",
		}

		resp, code := daemon.FillBlock(req)
		assert.Equal(t, expectedResp, resp)
		assert.Equal(t, STS_FAIL, code)

		mockLocalCache.AssertExpectations(t)
		mockRemoteCache.AssertExpectations(t)
	})

	t.Run("Success to FillBlock", func(t *testing.T) {
		blk := uint32(1)
		blklen := uint32(1024)
		req := &Req{
			Info: []*ReqInfo{
				{
					Target:    target,
					BlockNum:  blk,
					BlockSize: 4096,
					Size:      blklen,
				},
			},
		}

		mockLocalCache.On("check", target, blk).Return(true).Once()
		resp, code := daemon.FillBlock(req)

		assert.NotNil(t, resp)
		assert.Equal(t, STS_OK, code)

		mockLocalCache.AssertExpectations(t)
	})

}

type MockUnixConn struct {
	mock.Mock
}

func (m *MockUnixConn) ReadMsgUnix(b, oob []byte) (n, oobn int, flags int, addr *net.UnixAddr, err error) {
	args := m.Called(b, oob)
	return args.Int(0), args.Int(1), 0, nil, args.Error(2)
}

func (m *MockUnixConn) WriteMsgUnix(b, oob []byte, addr *net.UnixAddr) (n, oobn int, err error) {
	args := m.Called(b, oob, addr)
	return args.Int(0), args.Int(1), args.Error(2)
}

func (m *MockUnixConn) Read(b []byte) (n int, err error) {
	args := m.Called(b)
	return args.Int(0), args.Error(1)
}

func (m *MockUnixConn) Write(b []byte) (n int, err error) {
	args := m.Called(b)
	return args.Int(0), args.Error(1)
}

func (m *MockUnixConn) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockUnixConn) LocalAddr() net.Addr {
	return &net.UnixAddr{Name: "/tmp/mock.sock", Net: "unix"}
}

func (m *MockUnixConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{Name: "/tmp/mock.sock", Net: "unix"}
}

func (m *MockUnixConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *MockUnixConn) SetReadDeadline(t time.Time) error {
	args := m.Called(t)
	return args.Error(0)
}

func (m *MockUnixConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestReadHeader(t *testing.T) {
	hdrBuf := make([]byte, HeaderSize)
	hdrBuf[0] = 0x01
	hdrBuf[1] = 0x00
	hdrBuf[2] = 0x00
	hdrBuf[3] = 0x00

	t.Run("Normal case", func(t *testing.T) {
		mockConn := &MockUnixConn{}

		mockConn.On("ReadMsgUnix", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			b := args.Get(0).([]byte)
			copy(b, hdrBuf)
		}).Return(HeaderSize, 0, nil)

		header, file, err := readHeader(mockConn)

		require.NoError(t, err)
		require.NotNil(t, header)
		require.Nil(t, file)
		require.Equal(t, uint8(0x01), header.Type)
		require.Equal(t, uint8(0x00), header.Status)
		require.Equal(t, uint16(0x0000), header.Length)

		mockConn.AssertCalled(t, "ReadMsgUnix", mock.Anything, mock.Anything)
	})

	t.Run("ReadMsgUnix error", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		mockConn.On("ReadMsgUnix", mock.Anything, mock.Anything).Return(0, 0, fmt.Errorf("mock read error"))

		header, file, err := readHeader(mockConn)

		require.Error(t, err)
		require.Nil(t, header)
		require.Nil(t, file)

		mockConn.AssertCalled(t, "ReadMsgUnix", mock.Anything, mock.Anything)
	})

	t.Run("Invalid header size", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		mockConn.On("ReadMsgUnix", mock.Anything, mock.Anything).Return(HeaderSize-1, 0, nil)

		header, file, err := readHeader(mockConn)

		require.Error(t, err)
		require.Nil(t, header)
		require.Nil(t, file)
		require.Contains(t, err.Error(), "read header expect")

		mockConn.AssertCalled(t, "ReadMsgUnix", mock.Anything, mock.Anything)
	})
}

func TestReadRequest(t *testing.T) {
	reqBody := `[{"target": "cacheTarget", "block_num": 123, "size": 1024, "block_size": 256}]`
	reqBodyBytes := []byte(reqBody)
	hdrBuf := make([]byte, HeaderSize)
	hdrBuf[0] = REQ_GETBLOCK
	hdrBuf[1] = STS_OK
	hdrBuf[2] = byte(len(reqBodyBytes) & 0xFF)
	hdrBuf[3] = byte((len(reqBodyBytes) >> 8) & 0xFF)
	hdrBuf[4] = 0x01
	hdrBuf[5] = 0x00
	hdrBuf[6] = 0x00
	hdrBuf[7] = 0x00

	t.Run("Normal case", func(t *testing.T) {
		mockConn := &MockUnixConn{}

		mockConn.On("ReadMsgUnix", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			b := args.Get(0).([]byte)
			copy(b, hdrBuf)
		}).Return(HeaderSize, 0, nil)

		mockConn.On("Read", mock.Anything).Run(func(args mock.Arguments) {
			b := args.Get(0).([]byte)
			copy(b, reqBodyBytes)
		}).Return(len(reqBodyBytes), nil)

		req, err := readRequest(mockConn)
		assert.NoError(t, err)
		assert.NotNil(t, req)
	})

	t.Run("Not request", func(t *testing.T) {
		hdrBuf := make([]byte, HeaderSize)
		hdrBuf[0] = 0
		mockConn := &MockUnixConn{}

		mockConn.On("ReadMsgUnix", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			b := args.Get(0).([]byte)
			copy(b, hdrBuf)
		}).Return(HeaderSize, 0, nil)

		mockConn.On("Read", mock.Anything).Run(func(args mock.Arguments) {
			b := args.Get(0).([]byte)
			copy(b, reqBodyBytes)
		}).Return(len(reqBodyBytes), nil)

		req, err := readRequest(mockConn)
		assert.Error(t, err)
		assert.Nil(t, req)
	})

	t.Run("ReadHeader error", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		mockConn.On("ReadMsgUnix", mock.Anything, mock.Anything).Return(0, 0, fmt.Errorf("mock header read error"))

		req, err := readRequest(mockConn)

		require.Error(t, err)
		require.Nil(t, req)
		require.Contains(t, err.Error(), "mock header read error")
	})

	t.Run("Invalid request type", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		invalidHdrBuf := make([]byte, HeaderSize)
		invalidHdrBuf[0] = 0xFF
		mockConn.On("ReadMsgUnix", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			b := args.Get(0).([]byte)
			copy(b, invalidHdrBuf)
		}).Return(HeaderSize, 0, nil)

		req, err := readRequest(mockConn)

		require.Error(t, err)
		require.Nil(t, req)
		require.Contains(t, err.Error(), "Invalid header:")
	})

	t.Run("Read body error", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		mockConn.On("ReadMsgUnix", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			b := args.Get(0).([]byte)
			copy(b, hdrBuf)
		}).Return(HeaderSize, 0, nil)

		mockConn.On("Read", mock.Anything).Return(0, fmt.Errorf("mock body read error"))

		req, err := readRequest(mockConn)

		require.Error(t, err)
		require.Nil(t, req)
		require.Contains(t, err.Error(), "mock body read error")
	})

	t.Run("Body length mismatch", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		mockConn.On("ReadMsgUnix", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			b := args.Get(0).([]byte)
			copy(b, hdrBuf)
		}).Return(HeaderSize, 0, nil)

		mockConn.On("Read", mock.Anything).Run(func(args mock.Arguments) {
			b := args.Get(0).([]byte)
			copy(b, reqBodyBytes[:len(reqBodyBytes)-1])
		}).Return(len(reqBodyBytes)-1, nil)

		req, err := readRequest(mockConn)

		require.Error(t, err)
		require.Nil(t, req)
		require.Contains(t, err.Error(), "read request expect")
	})
}

func TestWriteResponse(t *testing.T) {
	mockConn := new(MockUnixConn)

	header := &MsgHdr{
		Type:   1,
		Status: 0,
		Id:     12345,
	}

	info := &RespInfo{
		Size:    1024,
		Message: "Success",
	}

	resp := &Resp{
		Header: header,
		Info:   info,
		File:   nil,
	}

	payload, _ := json.Marshal(&info)
	length := len(payload)
	header.Length = uint16(length)

	bufSize := HeaderSize + length
	expectedBuf := make([]byte, bufSize)
	if err := header.ToBytes(expectedBuf[:HeaderSize]); err != nil {
		t.Errorf("failed to convert header to bytes: %v", err)
		return
	}
	copy(expectedBuf[HeaderSize:], payload)

	mockConn.On("WriteMsgUnix", mock.MatchedBy(func(b []byte) bool {
		return assert.Equal(t, expectedBuf, b)
	}), mock.MatchedBy(func(oob []byte) bool {
		return len(oob) == 0
	}), mock.MatchedBy(func(addr *net.UnixAddr) bool {
		return addr == nil
	})).Return(len(expectedBuf), 0, nil)

	err := writeResponse(mockConn, resp)
	assert.NoError(t, err)

	mockConn.AssertExpectations(t)
}

func TestGetConnection(t *testing.T) {
	mockConn := new(MockUnixConn)
	testKey := uint64(12345)
	daemon := &daemon{
		connections: map[uint64]unixConn{
			testKey: mockConn,
		},
	}

	t.Run("Existing connection", func(t *testing.T) {
		conn, ok := daemon.getConnection(testKey)
		assert.True(t, ok, "Expected connection to be found")
		assert.Equal(t, mockConn, conn, "Expected connection to match mockConn")
	})

	t.Run("Non-existing connection", func(t *testing.T) {
		nonExistentKey := uint64(67890)
		conn, ok := daemon.getConnection(nonExistentKey)
		assert.False(t, ok, "Expected connection not to be found")
		assert.Nil(t, conn, "Expected connection to be nil")
	})
}

func TestDelConnection(t *testing.T) {
	mockConn := new(MockUnixConn)
	d := &daemon{
		connections: make(map[uint64]unixConn),
		connMu:      sync.Mutex{},
	}
	key := uint64(12345)
	d.connections[key] = mockConn

	mockConn.On("Close").Return(nil)

	t.Run("delete existing connection", func(t *testing.T) {
		d.delConnection(key)
		assert.NotContains(t, d.connections, key, "Connection should be removed")
		mockConn.AssertCalled(t, "Close")
	})

	t.Run("delete non-existent connection", func(t *testing.T) {
		d.delConnection(key)
		assert.NotContains(t, d.connections, key, "No connection should exist")
		mockConn.AssertNotCalled(t, "Close", "Close should not be called for non-existent connection")
	})
}

func TestHandleNewConn(t *testing.T) {
	d := &daemon{
		connections: make(map[uint64]unixConn),
		connMu:      sync.Mutex{},
		stopCh:      make(chan struct{}),
	}

	t.Run("failed to execute the second Read()", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		initMsg := make([]byte, InitKeySize)
		binary.LittleEndian.PutUint64(initMsg[:InitKeySize], 12345)

		mockConn.On("SetReadDeadline", mock.Anything).Return(nil).Twice()
		mockConn.On("Read", mock.MatchedBy(func(b []byte) bool {
			return len(b) == InitKeySize
		})).Run(func(args mock.Arguments) {
			copy(args.Get(0).([]byte), initMsg)
		}).Return(len(initMsg), nil).Once()
		mockConn.On("ReadMsgUnix", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			copy(args.Get(0).([]byte), initMsg)
		}).Return(len(initMsg), 0, nil).Once()
		mockConn.On("Close").Return(nil)

		d.handleNewConn(mockConn)

		d.connMu.Lock()
		conn, exists := d.connections[12345]
		d.connMu.Unlock()

		assert.False(t, exists, "Connection should not exist")
		assert.Nil(t, conn, "conn should be nil")
		mockConn.AssertExpectations(t)
	})

	t.Run("second SetReadDeadline error", func(t *testing.T) {
		mockConn := &MockUnixConn{}

		mockConn.On("SetReadDeadline", mock.Anything).Return(nil).Once()
		mockConn.On("Read", mock.Anything).Return(InitKeySize, nil)
		mockConn.On("SetReadDeadline", mock.Anything).Return(errors.New("deadline error")).Once()
		mockConn.On("Close").Return(nil)

		d.handleNewConn(mockConn)

		mockConn.AssertExpectations(t)
	})

	t.Run("read deadline error", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		mockConn.On("SetReadDeadline", mock.Anything).Return(errors.New("deadline error")).Once()
		mockConn.On("Close").Return(nil)

		d.handleNewConn(mockConn)

		mockConn.AssertExpectations(t)
	})

	t.Run("read message error", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		mockConn.On("SetReadDeadline", mock.Anything).Return(nil).Once()
		mockConn.On("Read", mock.Anything).Return(0, errors.New("read error")).Once()
		mockConn.On("Close").Return(nil)

		d.handleNewConn(mockConn)

		mockConn.AssertExpectations(t)
	})

	t.Run("message size mismatch", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		mockConn.On("SetReadDeadline", mock.Anything).Return(nil)
		mockConn.On("Read", mock.Anything).Return(InitKeySize, nil).Once()
		mockConn.On("Close").Return(nil).Once()

		d.handleNewConn(mockConn)

		mockConn.AssertExpectations(t)
	})

	t.Run("duplicate connection", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		key := uint64(12345)
		initMsg := make([]byte, InitKeySize)
		binary.LittleEndian.PutUint64(initMsg[:InitKeySize], key)

		mockConn.On("SetReadDeadline", mock.Anything).Return(nil).Once()
		mockConn.On("Read", mock.Anything).Run(func(args mock.Arguments) {
			copy(args.Get(0).([]byte), initMsg)
		}).Return(len(initMsg), nil).Once()
		mockConn.On("Close").Return(nil).Once()

		d.connMu.Lock()
		d.connections[key] = mockConn
		d.connMu.Unlock()

		d.handleNewConn(mockConn)

		mockConn.AssertExpectations(t)
	})

	t.Run("read n != InitKeySize error", func(t *testing.T) {
		mockConn := &MockUnixConn{}
		mockConn.On("SetReadDeadline", mock.Anything).Return(nil).Once()
		mockConn.On("Read", mock.Anything).Return(InitKeySize+1, nil)
		mockConn.On("Close").Return(nil)

		d.handleNewConn(mockConn)

		mockConn.AssertExpectations(t)
	})
}
