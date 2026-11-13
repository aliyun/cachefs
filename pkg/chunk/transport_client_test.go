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

package chunk

import (
	"context"
	"testing"
	"time"

	pb "github.com/aliyun/cachefs/pkg/rpc/cachefs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type MockCacheClient struct{ mock.Mock }

func (m *MockCacheClient) Load(ctx context.Context, opts ...grpc.CallOption) (pb.CachefsCache_LoadClient, error) {
	args := m.Called(ctx)
	return args.Get(0).(pb.CachefsCache_LoadClient), args.Error(1)
}

func (m *MockCacheClient) ExchangeTracker(ctx context.Context, req *pb.ExchangeTrackerRequest, opts ...grpc.CallOption) (*pb.ExchangeTrackerResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*pb.ExchangeTrackerResponse), args.Error(1)
}

type MockLoadClient struct{ mock.Mock }

func (m *MockLoadClient) Send(req *pb.LoadRequest) error { return m.Called(req).Error(0) }
func (m *MockLoadClient) Recv() (*pb.LoadResponse, error) {
	args := m.Called()
	return args.Get(0).(*pb.LoadResponse), args.Error(1)
}
func (m *MockLoadClient) Header() (metadata.MD, error) { return nil, nil }
func (m *MockLoadClient) Trailer() metadata.MD         { return nil }
func (m *MockLoadClient) CloseSend() error             { return nil }
func (m *MockLoadClient) Context() context.Context     { return context.Background() }
func (m *MockLoadClient) SendMsg(interface{}) error    { return nil }
func (m *MockLoadClient) RecvMsg(interface{}) error    { return nil }

func TestTransportClientLoadUsesFallbackServiceID(t *testing.T) {
	grpcClient, stream := new(MockCacheClient), new(MockLoadClient)
	primary, fallback := &mockTransferService{}, &mockTransferService{}
	trans := &transport{config: &remoteCacheConfig{blockSize: 4}, service: primary, fallbackService: fallback, rdmaDisabled: true}
	client := &transportClient{transport: trans, grpcClient: grpcClient}
	grpcClient.On("Load", mock.Anything).Return(stream, nil).Once()
	stream.On("Send", mock.MatchedBy(func(req *pb.LoadRequest) bool {
		return req.GetSupportsFallback() && req.GetUseFallback()
	})).Return(nil).Once()
	stream.On("Recv").Return(&pb.LoadResponse{Addr: "rdma:7712", FallbackAddr: "tcp:7713", SvcId: 8, ReqId: 9}, nil).Once()
	stream.On("Send", &pb.LoadRequest{}).Return(nil).Once()
	page := NewPage(make([]byte, 4))
	defer page.Release()
	require.NoError(t, client.load(context.Background(), "target", 0, page, "", false))
	assert.Empty(t, primary.downloads)
	assert.Equal(t, []transferDownload{{addr: "tcp:7713", svcID: 8, reqID: 9}}, fallback.downloads)
}

func TestTransportClientLoadCacherootDoesNotRequestFallback(t *testing.T) {
	grpcClient, stream := new(MockCacheClient), new(MockLoadClient)
	cacheroot := &mockTransferService{}
	trans := &transport{
		config:  &remoteCacheConfig{blockSize: 4, transport: "rdma", rdmaGroup: "region-a"},
		service: &mockTransferService{}, fallbackService: &mockTransferService{}, cacherootService: cacheroot,
	}
	client := &transportClient{transport: trans, grpcClient: grpcClient}
	grpcClient.On("Load", mock.Anything).Return(stream, nil).Once()
	stream.On("Send", mock.MatchedBy(func(req *pb.LoadRequest) bool {
		return !req.GetSupportsFallback() && !req.GetUseFallback()
	})).Return(nil).Once()
	stream.On("Recv").Return(&pb.LoadResponse{Addr: "rdma:7712", SvcId: 3, ReqId: 9}, nil).Once()
	stream.On("Send", &pb.LoadRequest{}).Return(nil).Once()
	page := NewPage(make([]byte, 4))
	defer page.Release()
	require.NoError(t, client.load(context.Background(), "target", 0, page, "", true))
	assert.Equal(t, []transferDownload{{addr: "rdma:7712", svcID: 3, reqID: 9}}, cacheroot.downloads)
}

func TestTransportClientLoadInitialFallbackRoutesSingleService(t *testing.T) {
	grpcClient, stream := new(MockCacheClient), new(MockLoadClient)
	tcp := &mockTransferService{}
	trans := &transport{config: &remoteCacheConfig{blockSize: 4}, service: tcp, fallbackService: tcp, rdmaDisabled: true}
	client := &transportClient{transport: trans, grpcClient: grpcClient}
	grpcClient.On("Load", mock.Anything).Return(stream, nil).Once()
	stream.On("Send", mock.MatchedBy(func(req *pb.LoadRequest) bool {
		return req.GetSupportsFallback() && req.GetUseFallback()
	})).Return(nil).Once()
	stream.On("Recv").Return(&pb.LoadResponse{FallbackAddr: "tcp:7713", SvcId: 8, ReqId: 9}, nil).Once()
	stream.On("Send", &pb.LoadRequest{}).Return(nil).Once()
	page := NewPage(make([]byte, 4))
	defer page.Release()
	require.NoError(t, client.load(context.Background(), "target", 0, page, "", false))
	assert.Equal(t, []transferDownload{{addr: "tcp:7713", svcID: 8, reqID: 9}}, tcp.downloads)
}

func TestTransportClientRdmaGroupWithoutLocalFallback(t *testing.T) {
	grpcClient := new(MockCacheClient)
	primary := &mockTransferService{}
	trans := &transport{config: &remoteCacheConfig{blockSize: 4, transport: "rdma", rdmaGroup: "region-a"}, service: primary}
	client := &transportClient{transport: trans, grpcClient: grpcClient}
	page := NewPage(make([]byte, 4))
	defer page.Release()
	err := client.load(context.Background(), "target", 0, page, "", false)
	assert.EqualError(t, err, "fallback to TCP not available: no local TCP service")
	grpcClient.AssertNotCalled(t, "Load", mock.Anything)
	assert.Empty(t, primary.downloads)
}

func TestRdmaGroupCandidatesMissThenTCP(t *testing.T) {
	cfg := &remoteCacheConfig{localAddr: "A", blockSize: 4, transport: "rdma", rdmaGroup: "region-a", rdmaFallbackTcp: true, peerTimeout: time.Second}
	primary, fallback := &mockTransferService{}, &mockTransferService{}
	s := &store{
		serf:                         &MockSerfService{},
		remoteFallbackRdmaGroupHits:  prometheus.NewCounter(prometheus.CounterOpts{Name: "cross_group_hits"}),
		remoteFallbackRdmaGroupBytes: prometheus.NewCounter(prometheus.CounterOpts{Name: "cross_group_bytes"}),
	}
	trans := &transport{config: cfg, store: s, service: primary, fallbackService: fallback, clients: make(map[string]*transportClient), rdmaPeers: map[string]struct{}{"B": {}}}
	peer := &peerCache{config: cfg, serf: s.serf, transport: trans, store: s,
		metaMap: map[string]map[uint32]map[string]struct{}{"target": {0: {"B": {}, "C": {}}}},
	}
	var attempted []string
	for _, addr := range []string{"B", "C"} {
		grpcClient, stream := new(MockCacheClient), new(MockLoadClient)
		trans.clients[addr] = &transportClient{transport: trans, peerAddr: addr, grpcClient: grpcClient}
		grpcClient.On("Load", mock.Anything).Return(stream, nil)
		stream.On("Send", mock.MatchedBy(func(req *pb.LoadRequest) bool {
			return req.GetTarget() == "target" && req.GetSupportsFallback() && req.GetUseFallback() == (addr == "C")
		})).Run(func(mock.Arguments) { attempted = append(attempted, addr) }).Return(nil)
		if addr == "B" {
			stream.On("Recv").Return((*pb.LoadResponse)(nil), status.Error(codes.NotFound, "miss")).Once()
			stream.On("Recv").Return(&pb.LoadResponse{Addr: "B-rdma", SvcId: 7, ReqId: 12}, nil).Once()
		} else {
			stream.On("Recv").Return(&pb.LoadResponse{Addr: "C-tcp", FallbackAddr: "C-tcp", SvcId: 8, ReqId: 13}, nil).Once()
		}
		stream.On("Send", &pb.LoadRequest{}).Return(nil).Once()
		t.Cleanup(func() { grpcClient.AssertExpectations(t); stream.AssertExpectations(t) })
	}
	assert.Equal(t, []string{"B", "C"}, peer.loadCandidates("target", 0))
	page, err := peer.load(context.Background(), "target", 0, 4)
	require.NoError(t, err)
	page.Release()
	assert.Equal(t, []string{"B", "C"}, attempted)
	assert.Empty(t, primary.downloads)
	assert.Equal(t, []transferDownload{{addr: "C-tcp", svcID: 8, reqID: 13}}, fallback.downloads)
	assert.EqualValues(t, 1, testutil.ToFloat64(s.remoteFallbackRdmaGroupHits))
	assert.EqualValues(t, 4, testutil.ToFloat64(s.remoteFallbackRdmaGroupBytes))
	assert.False(t, trans.rdmaDisabled)
	page = NewPage(make([]byte, 4))
	defer page.Release()
	require.NoError(t, trans.load(context.Background(), "B", "target", 0, 4, page, "", false))
	assert.Equal(t, []transferDownload{{addr: "B-rdma", svcID: 7, reqID: 12}}, primary.downloads)
	assert.Len(t, fallback.downloads, 1)
}
