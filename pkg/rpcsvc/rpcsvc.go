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
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/aliyun/cachefs/pkg/utils"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	MaxRecvSize = 64 << 20 // 64MB
	MaxSendSize = 64 << 20

	ConnectTimeout = time.Second * 5
)

var (
	logger = utils.GetLogger("cachefs")

	SharedBufferPool = grpc.NewSharedBufferPool()
)

// DialOptions returns the standard gRPC client options shared by all cachefs
// clients: insecure transport, the shared recv buffer pool, the 64MB per-call
// message size limits, and the shared connect timeout.
func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithRecvBufferPool(SharedBufferPool),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxRecvSize),
			grpc.MaxCallSendMsgSize(MaxSendSize),
		),
		grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: ConnectTimeout}),
	}
}

// Dial dials addr with DialOptions(). Extra opts are appended after the defaults
// and therefore take precedence over them.
func Dial(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, append(DialOptions(), opts...)...)
}

type RpcConfig struct {
	// Name in logger, default to 'cachefs'
	LoggerName string

	// Address RPC service listens on, in IP:port format
	Addr string
}

type RpcService interface {
	// Return grpc Server instance, used for register server
	GetServer() *grpc.Server

	// Return address this service listens on
	Addr() string

	// Start serving RPC server
	Serve()

	// Stop this RPC service
	Stop()
}

type rpcService struct {
	sync.Mutex
	config *RpcConfig

	grpcServer *grpc.Server
	listener   net.Listener
}

func NewRpcService(cfg *RpcConfig) (RpcService, error) {
	if cfg.LoggerName != "" {
		logger = utils.GetLogger(cfg.LoggerName)
	}

	svc := &rpcService{
		config: cfg,
	}

	// Setup gRPC Server
	rpcListener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("Listen on \"%s\" failed: %v", cfg.Addr, err)
	}
	svc.listener = rpcListener
	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(grpc_validator.UnaryServerInterceptor()),
		grpc.RecvBufferPool(SharedBufferPool),
		grpc.MaxRecvMsgSize(MaxRecvSize),
		grpc.MaxSendMsgSize(MaxSendSize),
	}
	svc.grpcServer = grpc.NewServer(append([]grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     time.Hour,
			MaxConnectionAge:      8 * time.Hour,
			MaxConnectionAgeGrace: 5 * time.Minute,
		}),
	}, opts...)...)
	return svc, nil
}

func (r *rpcService) Addr() string {
	r.Lock()
	defer r.Unlock()
	if r.listener != nil {
		return r.listener.Addr().String()
	} else {
		return ""
	}
}

func (r *rpcService) Serve() {
	// Snapshot the server and listener under the lock: Stop() nils both, and a Serve()
	// goroutine still waiting to run would otherwise dereference nil.
	r.Lock()
	server, listener := r.grpcServer, r.listener
	r.Unlock()
	if server == nil || listener == nil {
		return
	}
	go func() {
		// Use the listener's bound address, not the configured one, so a
		// config like "127.0.0.1:0" logs the actual port.
		logger.Infof("Serving RPC service on %s", listener.Addr().String())

		// Serve returns ErrServerStopped when called after Stop(), which is a normal
		// shutdown rather than a fatal error.
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Fatalf("Serve for cachefs RPC: %s", err)
		}
	}()
}

func (r *rpcService) GetServer() *grpc.Server {
	r.Lock()
	defer r.Unlock()
	return r.grpcServer
}

func (r *rpcService) Stop() {
	// Nil the fields under the lock but run the slow shutdown outside it:
	// GracefulStop() blocks until inflight RPCs finish, and readers taking
	// this lock (Addr, GetServer, Serve) must not stall behind that drain.
	r.Lock()
	server, listener := r.grpcServer, r.listener
	r.grpcServer, r.listener = nil, nil
	r.Unlock()

	if server != nil {
		server.GracefulStop()
	}
	if listener != nil {
		listener.Close()
	}
}
