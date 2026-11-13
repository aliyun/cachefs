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

package transfer

import (
	"context"
	"fmt"
	"time"

	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
)

var logger = utils.GetLogger("cachefs")

const (
	// Supported transport
	TCPTransport  = "tcp"
	RDMATransport = "rdma"

	MbtoB = 1e6 / 8
)

// A transfer service has two components, the server part and the client part.
// A server must be started by calling `StartServer` method, and an transfer
// service instance can be used as `TransferClient` without starting the server,
type TransferService interface {
	TransferServer
	TransferClient
	PeerEvictor
}

// A transfer server receives requests from clients and upload data to them.
type TransferServer interface {
	// Start a server instance.
	StartServer() error

	// Stop a server.
	StopServer() error

	// Get the actual transpart endpoint addresses, multiple NICs or RDMA
	// devices require multiple addresses.
	ServerAddrs() []string

	// Selects a server address for serving data, round-robin over all
	// addresses. Safe for concurrent use.
	SelectServerAddr() (string, error)

	// ID of this server instance.
	ServerID() uint32

	// Register data to be uploaded associated with a given key to server,
	// return its corresponding `RequestID`, which should be used by client
	// to identify this data. The `key` and `data` will be unregistered
	// automatically after being uploaded to client.
	// `onRelease`, if not nil, is called exactly once when the registration is
	// dropped, and must return the ownership keeping `data` alive, e.g. a page
	// reference: the upload still reads `data` after the caller's handler returns.
	Register(key string, data []byte, onRelease func()) (RequestID, error)

	// Unregister releases an unclaimed request ID. It is a no-op after the data
	// plane has claimed the registration.
	Unregister(reqID RequestID)
}

// A transfer client requests data from a server.
type TransferClient interface {
	// Download data from server identifies by `serverAddr`, which should
	// be one of `ServerAddrs`. `svcID` is the ID of the server, and `reqID`
	// is to identify the data client requests, which is returned by
	// `Register` of server. Data will be written to `data`.
	Download(ctx context.Context, serverAddr string, svcID uint32, reqID RequestID, data []byte) error
}

// Clear idle cached transport connections.
type PeerEvictor interface {
	// EvictPeer discards all idle cached connections whose server IP is in
	// peerIPs. Returns the total number of connections discarded.
	EvictPeer(peerIPs []string) int
}

type TransferConfig struct {
	// Name in logger, default to 'cachefs'
	LoggerName string

	// Transport method, support 'tcp' or 'rdma'
	Transport string

	// Transport devices, e.g. one or multiple eth NICs, one or multiple RDMA devices
	TransportNics []string

	// For tcp transport, this IP is used as default IP when `TransportNics`
	// is not set, otherwise IPs associated with each `TransportNics` are used.
	//
	// For RDMA based transport, this IP is used as control plane.
	TransportIP string

	// For tcp transport, this port is used as the preferred transfer service port
	// for each transport device, and if this port is not available, a random
	// available port will be used.
	//
	// For rdma transport, `BasePort` + RDMA device index (starting from 0) will
	// be used as the preferred port of RDMA control plane for current device,
	// and a random available port will be used if current port is not available.
	BasePort int

	// Transport bandwidth limit for uploads and downloads, shared by all transport
	// devices.
	// `PeerTimeout` is the timeout of each client `Download` request.
	UploadLimit   int64 // bytes per second
	DownloadLimit int64 // bytes per second
	TotalLimit    int64 // bytes per second
	SingleLimit   int64 // bytes per second, used instead of TotalLimit when only a single transport NIC is in use
	PeerTimeout   time.Duration

	// RDMA related configs
	RdmaBufSize  int    // Bounce buffer size in byte
	RdmaBufCount int    // Total bounce buffer count
	RdmaMaxMem   uint64 // Total memory limit used for bounce buffer
}

func DefaultTransferConfig() *TransferConfig {
	localIP, err := utils.GetDefaultIp()
	if err != nil {
		logger.Warnf("Get default IP in default config failed: %s, use 127.0.0.1 as default transport IP", err)
		localIP = "127.0.0.1"
	}
	return &TransferConfig{
		Transport:     TCPTransport,
		TransportNics: []string{},
		TransportIP:   localIP,
		BasePort:      7712,
		UploadLimit:   0,
		DownloadLimit: 0,
		TotalLimit:    0,
		SingleLimit:   0,
		PeerTimeout:   time.Second * 30,
		RdmaBufSize:   128 << 10, // 128KiB
		RdmaBufCount:  16,
		RdmaMaxMem:    1 << 30, // 1GiB
	}
}

func IsRdmaTransport(transport string) bool {
	return transport == RDMATransport
}

func (cfg *TransferConfig) RdmaEnabled() bool {
	return IsRdmaTransport(cfg.Transport)
}

func (cfg *TransferConfig) IsValidTransport() bool {
	switch cfg.Transport {
	case TCPTransport, RDMATransport:
		return true
	default:
		return false
	}
}

// Validate reports whether the config describes a service that can actually be built.
func (cfg *TransferConfig) Validate() error {
	if !cfg.IsValidTransport() {
		return fmt.Errorf("transport %s not supported", cfg.Transport)
	}
	return nil
}

func (cfg *TransferConfig) DeepCopy() *TransferConfig {
	newCfg := *cfg
	newCfg.TransportNics = make([]string, len(cfg.TransportNics))
	copy(newCfg.TransportNics, cfg.TransportNics)
	return &newCfg
}

type RequestID uint64
type transferBuffer struct {
	key   string
	data  []byte
	atime time.Time

	// onRelease gives back the ownership this registration borrowed, typically a page
	// reference. Called exactly once, outside buffersMu.
	onRelease func()
}

func NewTransferService(cfg *TransferConfig, reg prometheus.Registerer) (TransferService, error) {
	if cfg.LoggerName != "" {
		logger = utils.GetLogger(cfg.LoggerName)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	logger.Infof("Initializing transfer service with %s transport", cfg.Transport)
	return newCoroTransfer(cfg, reg)
}

func SupportedTransport() []string {
	return []string{TCPTransport, RDMATransport}
}
