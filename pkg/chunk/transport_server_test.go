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
	"google.golang.org/grpc/metadata"
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
