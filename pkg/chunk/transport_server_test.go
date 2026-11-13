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
	"io"
	"testing"

	pb "github.com/aliyun/cachefs/pkg/rpc/cachefs"
	"github.com/aliyun/cachefs/pkg/transfer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type transportLoadTestStream struct {
	ctx       context.Context
	requests  []*pb.LoadRequest
	recvErr   error
	responses []*pb.LoadResponse
}

func (s *transportLoadTestStream) SetHeader(metadata.MD) error  { return nil }
func (s *transportLoadTestStream) SendHeader(metadata.MD) error { return nil }
func (s *transportLoadTestStream) SetTrailer(metadata.MD)       {}
func (s *transportLoadTestStream) Context() context.Context     { return s.ctx }
func (s *transportLoadTestStream) SendMsg(interface{}) error    { return nil }
func (s *transportLoadTestStream) RecvMsg(interface{}) error    { return nil }

func (s *transportLoadTestStream) Send(res *pb.LoadResponse) error {
	s.responses = append(s.responses, res)
	return nil
}

func (s *transportLoadTestStream) Recv() (*pb.LoadRequest, error) {
	if len(s.requests) == 0 {
		return nil, s.recvErr
	}
	req := s.requests[0]
	s.requests = s.requests[1:]
	return req, nil
}

func TestTransportLoadUnregistersUnclaimedTransfer(t *testing.T) {
	request := &pb.LoadRequest{
		Target:    "target",
		BlockNum:  0,
		BlockSize: 4,
		Len:       4,
		SerfGroup: "group",
	}
	tests := []struct {
		name     string
		requests []*pb.LoadRequest
		recvErr  error
	}{
		{"client does not download", []*pb.LoadRequest{request}, io.EOF},
		{"client finishes", []*pb.LoadRequest{request, {}}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := NewPage([]byte("data"))
			bcache := new(MockLocalCache)
			bcache.On("load", "target", uint32(0), false).Return(page, nil, nil).Once()
			service := &mockTransferService{}
			trans := &transport{
				config:  &remoteCacheConfig{blockSize: 4, serfGroup: "group"},
				store:   &store{config: &StoreConfig{}, bcache: bcache},
				service: service,
			}
			stream := &transportLoadTestStream{
				ctx:      context.Background(),
				requests: tt.requests,
				recvErr:  tt.recvErr,
			}

			err := trans.Load(stream)
			if tt.recvErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.recvErr)
			}
			assert.Len(t, stream.responses, 1)
			service.mu.Lock()
			assert.Equal(t, []transfer.RequestID{1}, service.unregisterReqIDs)
			service.mu.Unlock()
			bcache.AssertExpectations(t)
		})
	}
}

func TestTransportLoadAdvertisesFallback(t *testing.T) {
	tests := []struct {
		name             string
		rdmaDisabled     bool
		supportsFallback bool
		useFallback      bool
		hasFallback      bool
		primaryAddr      string
		transport        string
		wantAddr         string
		wantSvcID        uint32
		wantFallback     bool
		wantFallbackAddr string
		wantErrMessage   string
	}{
		{name: "normal RDMA", supportsFallback: true, hasFallback: true, wantAddr: "rdma:7712", wantSvcID: 7, wantFallbackAddr: "tcp:7713"},
		{name: "client fallback", supportsFallback: true, useFallback: true, hasFallback: true, wantAddr: "tcp:7713", wantSvcID: 8, wantFallback: true, wantFallbackAddr: "tcp:7713"},
		{name: "client requires unavailable TCP", supportsFallback: true, useFallback: true, wantErrMessage: "client requires tcp fallback but tcp fallback not enabled"},
		{name: "legacy client ignores use fallback", useFallback: true, wantAddr: "rdma:7712", wantSvcID: 7},
		{name: "legacy TCP client", primaryAddr: "tcp:7712", transport: transfer.TCPTransport, wantAddr: "tcp:7712", wantSvcID: 7},
		{name: "TCP primary satisfies client fallback", primaryAddr: "tcp:7712", transport: transfer.TCPTransport, supportsFallback: true, useFallback: true, wantAddr: "tcp:7712", wantSvcID: 7, wantFallbackAddr: "tcp:7712"},
		{name: "legacy client after initialization fallback", rdmaDisabled: true, hasFallback: true, wantErrMessage: "rdma initialization failed and client does not support tcp fallback"},
		{name: "capable client after initialization fallback", rdmaDisabled: true, supportsFallback: true, hasFallback: true, wantSvcID: 8, wantFallback: true, wantFallbackAddr: "tcp:7713"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := NewPage([]byte("data"))
			bcache := new(MockLocalCache)
			if tt.wantErrMessage == "" {
				bcache.On("load", "target", uint32(0), false).Return(page, nil, nil).Once()
			}
			primaryAddr := tt.primaryAddr
			if primaryAddr == "" {
				primaryAddr = "rdma:7712"
			}
			primary := &mockTransferService{addr: primaryAddr, serverID: 7}
			fallback := &mockTransferService{addr: "tcp:7713", serverID: 8}
			transportType := tt.transport
			if transportType == "" {
				transportType = transfer.RDMATransport
			}
			trans := &transport{
				config: &remoteCacheConfig{blockSize: 4, serfGroup: "group", transport: transportType},
				store:  &store{config: &StoreConfig{}, bcache: bcache}, service: primary, rdmaDisabled: tt.rdmaDisabled,
			}
			if tt.hasFallback {
				trans.fallbackService = fallback
			}
			stream := &transportLoadTestStream{
				ctx: context.Background(),
				requests: []*pb.LoadRequest{{
					Target: "target", BlockNum: 0, BlockSize: 4, Len: 4, SerfGroup: "group",
					SupportsFallback: tt.supportsFallback, UseFallback: tt.useFallback,
				}},
				recvErr: io.EOF,
			}
			err := trans.Load(stream)
			if tt.wantErrMessage != "" {
				assert.Equal(t, codes.FailedPrecondition, status.Code(err))
				assert.Equal(t, tt.wantErrMessage, status.Convert(err).Message())
				assert.Empty(t, stream.responses)
				assert.Zero(t, primary.registerCalls)
				assert.Zero(t, fallback.registerCalls)
				bcache.AssertNotCalled(t, "load", "target", uint32(0))
				return
			}
			assert.ErrorIs(t, err, io.EOF)
			require.Len(t, stream.responses, 1)
			res := stream.responses[0]
			assert.Equal(t, tt.wantAddr, res.GetAddr())
			assert.Equal(t, tt.wantFallbackAddr, res.GetFallbackAddr())
			assert.Equal(t, tt.wantSvcID, res.GetSvcId())
			if tt.wantFallback {
				assert.Zero(t, primary.registerCalls)
				assert.Equal(t, 1, fallback.registerCalls)
				assert.Equal(t, []transfer.RequestID{1}, fallback.unregisterReqIDs)
				assert.Empty(t, primary.unregisterReqIDs)
			} else {
				assert.Equal(t, 1, primary.registerCalls)
				assert.Zero(t, fallback.registerCalls)
				assert.Equal(t, []transfer.RequestID{1}, primary.unregisterReqIDs)
				assert.Empty(t, fallback.unregisterReqIDs)
			}
			bcache.AssertExpectations(t)
		})
	}
}
