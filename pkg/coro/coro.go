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

package coro

/*
#cgo CXXFLAGS: -std=c++20
#cgo CFLAGS: -I../../coro_rpc_lib/coro_rpc_lib
#cgo LDFLAGS: -L../../coro_rpc_lib/ -lcoro_rpc -lm -static-libstdc++ -static-libgcc -libverbs
#include "coro_rpc.h"
#include <stdlib.h>
#include <stdint.h>
*/
import "C"
import (
	"fmt"
	"strings"
	"unsafe"
)

// InitLog initializes the RPC logging system.
//
// The `levelStr` can be one of: "TRACE", "DEBUG", "INFO", "WARNING".
// The other parameters are kept at their default values:
// - log_path (nil): logs will be output to stdout, not a file.
// - file_size (0): no file size limit (only used when logging to file).
// - file_count (0): no file rotation count (only used when logging to file).
// - async (true): asynchronous logging.
// In our scenario, we log to stdout to follow the upper-level logging system.
func InitLog(levelStr string) {
	var level int
	switch levelStr {
	case "TRACE":
		level = 1
	case "DEBUG":
		level = 2
	case "INFO":
		level = 3
	case "WARNING":
		level = 4
	default:
		level = 3 // Default to INFO if unknown
	}
	C.init_rpc_log(nil, C.int(level), 0, 0, C.bool(true))
}

// BufferPoolConfig holds the configuration of the RDMA buffer pool. A zero
// field means unset and keeps the library default.
type BufferPoolConfig struct {
	BufferSize  int    // Bounce buffer size in byte
	MaxMemUsage uint64 // Max pool size in byte
}

// InitBufferPool must be called before starting any server or creating any
// client pool: the RDMA device configuration is latched on first use.
func InitBufferPool(cfg *BufferPoolConfig) {
	conf := C.pool_config{
		buffer_size:      C.uint32_t(cfg.BufferSize),
		max_memory_usage: C.uint64_t(cfg.MaxMemUsage),
	}
	C.config_buffer_pool(conf)
}

// ServerConfig holds the configuration for starting a coro_rpc server.
type ServerConfig struct {
	Addr      string // Listen address
	RdmaDev   string // RDMA device name, empty for TCP
	ThreadNum int    // Number of worker threads
}

func StartServer(cfg *ServerConfig) unsafe.Pointer {
	rdma := false
	var cDevName *C.char = nil
	if cfg.RdmaDev != "" {
		rdma = true
		cDevName = C.CString(cfg.RdmaDev)
		defer C.free(unsafe.Pointer(cDevName))
	}

	cstr := C.CString(cfg.Addr)
	defer C.free(unsafe.Pointer(cstr))

	conf := C.server_config{
		parallel:    C.int(cfg.ThreadNum),
		enable_ib:   C.bool(rdma),
		device_name: cDevName,
	}
	if rdma {
		conf.min_recv_buf_count = C.uint32_t(2)
		conf.max_recv_buf_count = C.uint32_t(4)
	}
	return C.start_rpc_server(cstr, conf)
}

func StopServer(server unsafe.Pointer) {
	C.stop_rpc_server(server)
}

type ClientPoolConfig struct {
	Addr string // Target server address
	Src  string // Local device, e.g. local RDMA dev or IP address

	ConnTimeout int // Connection timeout in second
	ReqTimeout  int // Request timeout in second
	IdleTimeout int // Idle connection timeout in second, must be > 0

	// ConnectRetryCount is the number of connect retries on top of the guaranteed
	// initial attempt: 0 = no retry (fail on first connect timeout), negative =
	// keep library default (3).
	ConnectRetryCount int

	EnableRdma    bool // Enable RDMA
	RdmaMinBuffer int  // Min buffer count per RDMA connection
	RdmaMaxBuffer int  // Max buffer count per RDMA connection
}

// Create new client pool to specified server `Addr`, `Src` is used to specify which local device to
// use, it could be IP of local NIC if RDMA is not enabled, or empty to let OS select NIC, and could
// be RDMA device name if RDMA is enabled.
func CreateClientPool(pcfg *ClientPoolConfig) unsafe.Pointer {
	cAddr := C.CString(pcfg.Addr)
	defer C.free(unsafe.Pointer(cAddr))

	cSrc := C.CString(pcfg.Src)
	defer C.free(unsafe.Pointer(cSrc))

	idleTimeout := pcfg.IdleTimeout
	if idleTimeout < 0 {
		idleTimeout = 0
	}
	conf := C.client_config{
		connect_timeout_sec: C.int(pcfg.ConnTimeout),
		req_timeout_sec:     C.int(pcfg.ReqTimeout),
		idle_sec:            C.int(idleTimeout),
		connect_retry_count: C.int(pcfg.ConnectRetryCount),
		enable_ib:           C.bool(pcfg.EnableRdma),
		local_ip:            nil,
	}
	if pcfg.Src != "" {
		conf.local_ip = cSrc
	}

	if pcfg.EnableRdma {
		conf.min_recv_buf_count = C.uint32_t(pcfg.RdmaMinBuffer)
		conf.max_recv_buf_count = C.uint32_t(pcfg.RdmaMaxBuffer)
	}
	return C.create_client_pool(cAddr, conf)
}

func Load(pool unsafe.Pointer, svcID uint32, req uint64, buf []byte) error {
	reqInfo := C.load_request{
		req_id: C.uint64_t(req),
		svc_id: C.uint32_t(svcID),
	}
	result := C.load(pool, reqInfo, (*C.char)(unsafe.Pointer(&buf[0])), C.uint64_t(len(buf)))
	if result.ec > 0 {
		defer C.free(unsafe.Pointer(result.err_msg))
		return fmt.Errorf("load failed: %d, %s", result.ec, C.GoString(result.err_msg))
	}
	if int(result.len) != len(buf) {
		return fmt.Errorf("data size mismatch: expected %d, got %d", len(buf), result.len)
	}
	return nil
}

func ResponseMsg(ctx unsafe.Pointer, buf []byte) unsafe.Pointer {
	return C.response_msg(ctx, (*C.char)(unsafe.Pointer(&buf[0])), C.uint64_t(len(buf)))
}

func WaitResponseFinish(promise unsafe.Pointer) error {
	result := C.wait_response_finish(promise)
	if result.ec > 0 {
		defer C.free(unsafe.Pointer(result.err_msg))
		return fmt.Errorf("wait_response_finish error: %d, %s", result.ec, C.GoString(result.err_msg))
	}
	return nil
}

func ResponseError(ctx unsafe.Pointer, msg string) {
	cstr := C.CString(msg)
	defer C.free(unsafe.Pointer(cstr))
	C.response_error(ctx, 1001, cstr)
}

func FreeClientPool(p unsafe.Pointer) {
	C.free_client_pool(p)
}

// Atomically drains all idle cached connections (stale or not) from client_pool
// for the given server address.
func FlushClientPool(p unsafe.Pointer, addr string) uint64 {
	cAddr := C.CString(addr)
	defer C.free(unsafe.Pointer(cAddr))
	return uint64(C.flush_client_pool(p, cAddr))
}

// RdmaMemUsage returns the current RDMA buffer pool memory usage in bytes.
func RdmaMemUsage() uint64 {
	return uint64(C.global_memory_usage())
}

// RdmaMaxMemUsage returns the historical max RDMA buffer pool memory usage in bytes.
func RdmaMaxMemUsage() uint64 {
	return uint64(C.global_history_max_memory_usage())
}

func GetAllRDMADevs() []string {
	cAllDevs := C.get_all_rdma_devs()
	if cAllDevs == nil {
		return []string{}
	}
	defer C.free(unsafe.Pointer(cAllDevs))
	allDevs := C.GoString(cAllDevs)
	return strings.Fields(allDevs)
}
