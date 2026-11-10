/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package utils

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mattn/go-isatty"
)

// Min returns min of 2 int
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Exists checks if the file/folder in given path exists
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

func GetFileInode(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if sst, ok := fi.Sys().(*syscall.Stat_t); ok {
		return sst.Ino, nil
	}
	return 0, nil
}

// SplitDir splits a path with default path list separator or comma.
func SplitDir(d string) []string {
	dd := strings.Split(d, string(os.PathListSeparator))
	if len(dd) == 1 {
		dd = strings.Split(dd[0], ",")
	}
	return dd
}

// GetLocalIp get the local ip used to access remote address.
func GetLocalIp(address string) (string, error) {
	conn, err := net.Dial("udp", address)
	if err != nil {
		return "", err
	}
	ip, _, err := net.SplitHostPort(conn.LocalAddr().String())
	if err != nil {
		return "", err
	}
	return ip, nil
}

// GetInterfaceIp retrieves the first valid IPv4 address of the given network interface.
func GetInterfaceIp(interfaceName string) (string, error) {
	iface, err := net.InterfaceByName(interfaceName)
	if err != nil {
		return "", fmt.Errorf("failed to get interface %s: %w", interfaceName, err)
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return "", fmt.Errorf("failed to get addresses for interface %s: %w", interfaceName, err)
	}

	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok {
			if ip := ipNet.IP.To4(); ip != nil {
				return ip.String(), nil
			}
		}
	}

	return "", fmt.Errorf("no valid IPv4 address found for interface %s", interfaceName)
}

func GetIPsFromInterfaces(nicNames []string) ([]string, error) {
	ips := []string{}
	if len(nicNames) == 0 {
		return ips, nil
	}
	for _, name := range nicNames {
		ip, err := GetInterfaceIp(name)
		if err != nil {
			return nil, err
		}
		ips = append(ips, ip)
	}
	return ips, nil
}

func WithTimeout(f func() error, timeout time.Duration) error {
	var done = make(chan int, 1)
	var t = time.NewTimer(timeout)
	var err error
	go func() {
		err = f()
		done <- 1
	}()
	select {
	case <-done:
		t.Stop()
	case <-t.C:
		err = fmt.Errorf("timeout after %s", timeout)
	}
	return err
}

func RemovePassword(uri string) string {
	p := strings.LastIndex(uri, "@")
	if p < 0 {
		return uri
	}
	sp := strings.Index(uri, "://") + 3
	if sp == 2 {
		sp = 0
	}
	cp := strings.Index(uri[sp:], ":")
	if cp < 0 || sp+cp > p {
		return uri
	}
	return uri[:sp+cp] + ":****" + uri[p:]
}

func StringContains(s []string, e string) bool {
	for _, item := range s {
		if item == e {
			return true
		}
	}
	return false
}

func FormatBytes(n uint64) string {
	if n < 1024 {
		return fmt.Sprintf("%d Bytes", n)
	}
	units := []string{"K", "M", "G", "T", "P", "E"}
	m := n
	i := 0
	for ; i < len(units)-1 && m >= 1<<20; i++ {
		m = m >> 10
	}
	return fmt.Sprintf("%.2f %siB (%d Bytes)", float64(m)/1024.0, units[i], n)
}

// Format n as a compact human-readable size (e.g. "4.10GiB") without the raw
// byte count, keeping tabular output aligned.
func FormatBytesShort(n uint64) string {
	if n < 1024 {
		return fmt.Sprintf("%dB", n)
	}
	units := []string{"K", "M", "G", "T", "P", "E"}
	m := n
	i := 0
	for ; i < len(units)-1 && m >= 1<<20; i++ {
		m >>= 10
	}
	return fmt.Sprintf("%.2f%siB", float64(m)/1024.0, units[i])
}

func SupportANSIColor(fd uintptr) bool {
	return isatty.IsTerminal(fd)
}

func ListenWithRandomPort(ip string) (net.Listener, int, error) {
	i := 0
	rand.Seed(time.Now().UnixNano())
	for ; i < 10; i++ {
		port := rand.Intn(65535-1024) + 1024
		listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
		if err == nil && listener != nil {
			return listener, port, nil
		}
	}
	return nil, -1, errors.New("no available port after 10 attemps")
}

// Get IP address of device with default route
func GetDefaultIp() (string, error) {
	return GetLocalIp("8.8.8.8:80")
}

// Get size of given block
func GetBlockSize(blockSize uint32, length uint64, blk uint32) uint64 {
	blksize := uint64(blockSize)
	bsize := length - uint64(blk)*blksize
	if bsize > blksize {
		bsize = blksize
	}
	return bsize
}

// GetRangeBlockBytes returns the bytes of complete blocks intersecting [start, start+length).
func GetRangeBlockBytes(fileSize, start, length uint64, blockSize uint32) (int64, error) {
	if blockSize == 0 {
		return 0, fmt.Errorf("cache block size is zero")
	}
	if length == 0 {
		return 0, fmt.Errorf("file range length must be positive")
	}
	end := start + length
	if end < start {
		return 0, fmt.Errorf("file range overflows the file")
	}
	if start >= fileSize || end > fileSize {
		return 0, fmt.Errorf("file range is outside the file")
	}
	blockBytes := uint64(blockSize)
	first := start / blockBytes
	last := (end - 1) / blockBytes
	lastBytes := fileSize - last*blockBytes
	if lastBytes > blockBytes {
		lastBytes = blockBytes
	}
	maxInt64 := uint64(^uint64(0) >> 1)
	fullBlocks := last - first
	if fullBlocks > (maxInt64-lastBytes)/blockBytes {
		return 0, fmt.Errorf("selected block span exceeds int64")
	}
	return int64(fullBlocks*blockBytes + lastBytes), nil
}

// Return block count given total length and block size
func GetBlockCount(length uint64, blockSize uint32) uint32 {
	if length == 0 {
		return 0
	}
	blocksize := uint64(blockSize)
	blkcnt := length / blocksize
	if length%blocksize != 0 {
		blkcnt += 1
	}
	return uint32(blkcnt)
}

func IsPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

// GoGcConfig holds GC settings given on the command line as an alternative to
// the GOGC and GOMEMLIMIT environment variables, which take precedence.
type GoGcConfig struct {
	GCPercent    int
	GCPercentSet bool
	MemLimit     int64 // in bytes, 0 means not set
}

// SetupGoGc applies conf as if it were given via GOGC and GOMEMLIMIT at startup.
// The Go runtime reads these variables only at startup, so a setting from conf
// is applied explicitly, and only if the corresponding variable is not set.
//
// If neither the variables nor conf is set, disable GC and set memory limit to
// defaultLimit. This is to reduce GC frequency, as go heap is low when allocating
// memory via mmap or memfd, which triggers frequent GC and puts more load on CPU.
func SetupGoGc(conf GoGcConfig, defaultLimit int64) {
	gogc, memlimit := os.Getenv("GOGC"), os.Getenv("GOMEMLIMIT")
	if gogc == "" && memlimit == "" && !conf.GCPercentSet && conf.MemLimit == 0 {
		debug.SetGCPercent(-1)
		debug.SetMemoryLimit(defaultLimit)
		return
	}
	if conf.GCPercentSet {
		if gogc != "" {
			logger.Infof("GOGC=%s takes precedence over --go-gc %d", gogc, conf.GCPercent)
		} else {
			debug.SetGCPercent(conf.GCPercent)
			logger.Infof("Set Go GC percent to %d", conf.GCPercent)
		}
	}
	if conf.MemLimit > 0 {
		if memlimit != "" {
			logger.Infof("GOMEMLIMIT=%s takes precedence over --go-memlimit %dMiB", memlimit, conf.MemLimit>>20)
		} else {
			debug.SetMemoryLimit(conf.MemLimit)
			logger.Infof("Set Go memory limit to %dMiB", conf.MemLimit>>20)
		}
	}
}

func Duration(s string) time.Duration {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return time.Microsecond * time.Duration(v*1e6)
	}

	err = nil
	var d time.Duration
	p := strings.Index(s, "d")
	if p >= 0 {
		v, err = strconv.ParseFloat(s[:p], 64)
	}
	if err == nil && s[p+1:] != "" {
		d, err = time.ParseDuration(s[p+1:])
	}

	if err != nil {
		logger.Warnf("Invalid duration value: %s, setting it to 0", s)
		return 0
	}
	return d + time.Hour*time.Duration(v*24)
}

// Check if `sliceB` contains all elements in `sliceA`, and returns the first not found
// element in `sliceA`
func ContainsAll(sliceA, sliceB []string) (bool, string) {
	elementSet := make(map[string]struct{})
	for _, item := range sliceB {
		elementSet[item] = struct{}{}
	}

	for _, item := range sliceA {
		if _, exists := elementSet[item]; !exists {
			return false, item
		}
	}
	return true, ""
}

func TidInPid(tid, pid int) bool {
	path := fmt.Sprintf("/proc/%d/task/%d", pid, tid)
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// Attempts to listen on a TCP port at the specified IP.
// It first tries to use the specified port. If that port is not available (e.g. already
// in use), it will then attempt to listen on any available port by specifying IP with
// an empty port, which lets the system choose a random available port.
func FindAvailablePort(ip string, port int) (net.Listener, error) {
	lis, err := net.Listen("tcp", net.JoinHostPort(ip, fmt.Sprintf("%d", port)))
	if err != nil {
		if opErr, ok := err.(*net.OpError); ok && strings.Contains(opErr.Error(), "address already in use") {
			lis, err = net.Listen("tcp", fmt.Sprintf("%s:", ip))
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return lis, nil
}

// LastNBytes returns the last n bytes of s.
// If n <= 0, it returns an empty string.
// If n >= len(s), it returns the entire string.
func LastNBytes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
