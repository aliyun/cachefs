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

	pb "github.com/aliyun/cachefs/pkg/rpc/cachefs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
	trans := &transport{config: &remoteCacheConfig{blockSize: 4}, service: primary, fallbackService: fallback}
	client := &transportClient{transport: trans, grpcClient: grpcClient}
	grpcClient.On("Load", mock.Anything).Return(stream, nil).Once()
	stream.On("Send", mock.MatchedBy(func(req *pb.LoadRequest) bool {
		return req.GetSupportsFallback() && req.GetUseFallback()
	})).Return(nil).Once()
	stream.On("Recv").Return(&pb.LoadResponse{Addr: "rdma:7712", FallbackAddr: "tcp:7713", SvcId: 8, ReqId: 9}, nil).Once()
	stream.On("Send", &pb.LoadRequest{}).Return(nil).Once()
	page := NewPage(make([]byte, 4))
	defer page.Release()
	require.NoError(t, client.load(context.Background(), "target", 0, page, "", false, true))
	assert.Empty(t, primary.downloads)
	assert.Equal(t, []transferDownload{{addr: "tcp:7713", svcID: 8, reqID: 9}}, fallback.downloads)
}

func TestTransportClientLoadCacherootDoesNotRequestFallback(t *testing.T) {
	grpcClient, stream := new(MockCacheClient), new(MockLoadClient)
	cacheroot := &mockTransferService{}
	trans := &transport{
		config:  &remoteCacheConfig{blockSize: 4, transport: "rdma"},
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
	require.NoError(t, client.load(context.Background(), "target", 0, page, "", true, trans.useFallback(true)))
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
	require.NoError(t, client.load(context.Background(), "target", 0, page, "", false, trans.useFallback(false)))
	assert.Equal(t, []transferDownload{{addr: "tcp:7713", svcID: 8, reqID: 9}}, tcp.downloads)
}
