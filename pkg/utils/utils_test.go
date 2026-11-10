/*
 * JuiceFS, Copyright 2021 Juicedata, Inc.
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
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMin(t *testing.T) {
	assertEqual(t, Min(1, 2), 1)
	assertEqual(t, Min(-1, -2), -2)
	assertEqual(t, Min(0, 0), 0)
}

func TestExists(t *testing.T) {
	assertEqual(t, Exists("/"), true)
	assertEqual(t, Exists("/not_exist_path"), false)
}

func TestSplitDir(t *testing.T) {
	assertEqual(t, SplitDir("/a:/b"), []string{"/a", "/b"})
	assertEqual(t, SplitDir("a,/b"), []string{"a", "/b"})
	assertEqual(t, SplitDir("/a;b"), []string{"/a;b"})
	assertEqual(t, SplitDir("a/b"), []string{"a/b"})
}

func TestGetInode(t *testing.T) {
	_, err := GetFileInode("")
	if err == nil {
		t.Errorf("invalid path should fail")
		return
	}
	ino, err := GetFileInode("/")
	if err != nil {
		t.Errorf("get file inode: %s", err)
		return
	} else if ino > 2 {
		if _, err := os.Stat("/.dockerenv"); err != nil {
			t.Errorf("inode of root should be 1/2, but got %d", ino)
			return
		} else {
			t.Skipf("inode of root should be 1/2, but got %d", ino)
		}
	}
}

func TestLocalIp(t *testing.T) {
	_, err := GetLocalIp("127.0.0.1")
	if err == nil {
		t.Errorf("should fail with invalid address")
		return
	}
	ip, err := GetLocalIp("127.0.0.1:22")
	if err != nil {
		t.Errorf("get local ip: %s", err)
		return
	}
	if ip != "127.0.0.1" {
		t.Errorf("local ip should be 127.0.0.1, bug got %s", ip)
		return
	}
}

func TestTimeout(t *testing.T) {
	err := WithTimeout(func() error {
		return nil
	}, time.Millisecond*10)
	if err != nil {
		t.Errorf("fast function should return nil")
		return
	}
	err = WithTimeout(func() error {
		time.Sleep(time.Millisecond * 100)
		return nil
	}, time.Millisecond*10)
	if err == nil || !strings.HasPrefix(err.Error(), "timeout after") {
		t.Errorf("slow function should be timeout: %s", err)
		return
	}
}

func TestRemovePassword(t *testing.T) {
	testCase := []struct {
		uri      string
		expected string
	}{
		{"redis://:password@localhost:6379/0",
			"redis://:****@localhost:6379/0",
		},
		{"redis://:pass@word@localhost:6379/0",
			"redis://:****@localhost:6379/0",
		},
		{":password@localhost:6379/0",
			":****@localhost:6379/0",
		},
		{"oss://ak:sk@zhijian-test2.oss-cn-hangzhou.aliyuncs.com",
			"oss://ak:****@zhijian-test2.oss-cn-hangzhou.aliyuncs.com",
		},
		{"/tmp/file",
			"/tmp/file",
		},
		{"file:///tmp/file",
			"file:///tmp/file",
		},
		{"sftp:///tmp/file",
			"sftp:///tmp/file",
		},
	}
	for _, tc := range testCase {
		assertEqual(t, RemovePassword(tc.uri), tc.expected)
	}
}

func TestListenWithRandomPort(t *testing.T) {
	lis, port, err := ListenWithRandomPort("127.0.0.1")
	if err != nil {
		t.Errorf("ListenWithRandomPort failed: %s", err)
		return
	}
	t.Logf("Listen on port %d", port)
	lis.Close()

	lis, port, err = ListenWithRandomPort("256.0.0.1")
	if err == nil {
		t.Error("ListenWithRandomPort with invalid ip should fail")
		return
	}
}

func TestGetInterfaceIp(t *testing.T) {
	interfaceName := "lo"

	ip, err := GetInterfaceIp(interfaceName)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
		return
	}

	expectedIP := "127.0.0.1"
	if ip != expectedIP {
		t.Errorf("expected IP %s, got %s", expectedIP, ip)
		return
	}
}

func TestGetBlockSize(t *testing.T) {
	tcs := []struct {
		blocksize uint32
		blk       uint32
		length    uint64
		expect    uint64
	}{
		{
			blocksize: 4 * 1024 * 1024,
			length:    0,
			blk:       0,
			expect:    0,
		},
		{
			blocksize: 4 * 1024 * 1024,
			length:    1,
			blk:       0,
			expect:    1,
		},
		{
			blocksize: 4 * 1024 * 1024,
			length:    4*1024*1024 - 1,
			blk:       0,
			expect:    4*1024*1024 - 1,
		},
		{
			blocksize: 4 * 1024 * 1024,
			length:    4 * 1024 * 1024,
			blk:       0,
			expect:    4 * 1024 * 1024,
		},
		{
			blocksize: 4 * 1024 * 1024,
			length:    2 * 1024 * 1024,
			blk:       0,
			expect:    2 * 1024 * 1024,
		},
		{
			blocksize: 4 * 1024 * 1024,
			length:    6 * 1024 * 1024,
			blk:       1,
			expect:    2 * 1024 * 1024,
		},
		{
			blocksize: 4 * 1024 * 1024,
			length:    8*1024*1024 + 1,
			blk:       1,
			expect:    4 * 1024 * 1024,
		},
		{
			blocksize: 4 * 1024 * 1024,
			length:    8*1024*1024 - 1,
			blk:       1,
			expect:    4*1024*1024 - 1,
		},
		{
			blocksize: 4 * 1024 * 1024,
			length:    8*1024*1024 + 1,
			blk:       2,
			expect:    1,
		},
	}

	for _, tc := range tcs {
		got := GetBlockSize(tc.blocksize, tc.length, tc.blk)
		if got != tc.expect {
			t.Errorf("blocksize %d length %d block %d, expect data size %d, got %d", tc.blocksize, tc.length, tc.blk, tc.expect, got)
			return
		}
	}
}

func TestGetRangeBlockBytes(t *testing.T) {
	maxInt64 := uint64(^uint64(0) >> 1)
	tests := []struct {
		name      string
		fileSize  uint64
		start     uint64
		length    uint64
		blockSize uint32
		want      int64
		wantErr   bool
	}{
		{"one block", 10, 1, 2, 4, 4, false},
		{"crosses blocks", 10, 3, 2, 4, 8, false},
		{"aligned block", 10, 4, 4, 4, 4, false},
		{"short tail", 10, 8, 2, 4, 2, false},
		{"whole file", 10, 0, 10, 4, 10, false},
		{"zero block size", 10, 0, 1, 0, 0, true},
		{"zero length", 10, 0, 0, 4, 0, true},
		{"start at eof", 10, 10, 1, 4, 0, true},
		{"past eof", 10, 9, 2, 4, 0, true},
		{"empty file", 0, 0, 1, 4, 0, true},
		{"endpoint overflow", 10, ^uint64(0), 1, 4, 0, true},
		{"max int64", maxInt64, 0, maxInt64, 1, int64(maxInt64), false},
		{"int64 overflow", ^uint64(0), 0, ^uint64(0), 1, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetRangeBlockBytes(tt.fileSize, tt.start, tt.length, tt.blockSize)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetBlockCount(t *testing.T) {
	tests := []struct {
		length uint64
		expect uint32
	}{
		{
			length: 0,
			expect: 0,
		},
		{
			length: 1,
			expect: 1,
		},
		{
			length: 4095,
			expect: 1,
		},
		{
			length: 4096,
			expect: 1,
		},
		{
			length: 4097,
			expect: 2,
		},
		{
			length: 8191,
			expect: 2,
		},
		{
			length: 8192,
			expect: 2,
		},
		{
			length: 8193,
			expect: 3,
		},
	}
	for _, tc := range tests {
		result := GetBlockCount(tc.length, 4096)
		if result != tc.expect {
			t.Errorf("Wrong block count with 4096 block size for length %d, got %d expect %d", tc.length, result, tc.expect)
		}
	}
}

func TestIsPowerOfTwo(t *testing.T) {
	tests := []struct {
		input    int
		expected bool
	}{
		{1, true},
		{2, true},
		{3, false},
		{4, true},
		{5, false},
		{8, true},
		{16, true},
		{20, false},
		{32, true},
		{64, true},
		{100, false},
		{0, false},
		{-8, false},
	}

	for _, test := range tests {
		result := IsPowerOfTwo(test.input)
		if result != test.expected {
			t.Errorf("IsPowerOfTwo(%d) = %v; expected %v", test.input, result, test.expected)
		}
	}
}

func TestGetIPsFromInterfaces(t *testing.T) {
	got, _ := GetIPsFromInterfaces([]string{})
	assert.Equal(t, []string{}, got, "emptydev")

	got, _ = GetIPsFromInterfaces([]string{"lo"})
	assert.Equal(t, []string{"127.0.0.1"}, got, "lo dev")

	_, err := GetIPsFromInterfaces([]string{"nosuchdev"})
	assert.Error(t, err, "nosuchdev")
}

func TestDuration(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want time.Duration
	}{
		{
			name: "DurationWithSeconds",
			s:    "60",
			want: time.Minute,
		},
		{
			name: "DurationWithFloatSeconds",
			s:    "1.5",
			want: 1500 * time.Millisecond,
		},
		{
			name: "DurationWithHours",
			s:    "2h",
			want: 2 * time.Hour,
		},
		{
			name: "DurationWithDays",
			s:    "1d",
			want: 24 * time.Hour,
		},
		{
			name: "DurationWithDaysAndTime",
			s:    "1d2h",
			want: 26 * time.Hour,
		},
		{
			name: "DurationWithInvalidInput",
			s:    "invalid",
			want: 0,
		},
		{
			name: "DurationWithEmptyString",
			s:    "",
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Duration(tt.s), "Duration(%v)", tt.s)
		})
	}
}

func TestContainsAll(t *testing.T) {
	tests := []struct {
		name     string
		a        []string
		b        []string
		expected bool
		notfound string
	}{
		// Basic cases
		{
			name:     "all elements exist",
			a:        []string{"apple", "banana"},
			b:        []string{"apple", "banana", "orange"},
			expected: true,
			notfound: "",
		},
		{
			name:     "some elements missing",
			a:        []string{"apple", "grape"},
			b:        []string{"apple", "banana"},
			expected: false,
			notfound: "grape",
		},

		// Empty slice handling
		{
			name:     "empty a slice",
			a:        []string{},
			b:        []string{"apple"},
			expected: true,
			notfound: "",
		},
		{
			name:     "empty b slice",
			a:        []string{"apple"},
			b:        []string{},
			expected: false,
			notfound: "apple",
		},
		{
			name:     "both slices empty",
			a:        []string{},
			b:        []string{},
			expected: true,
			notfound: "",
		},

		// Duplicate elements
		{
			name:     "duplicates in a",
			a:        []string{"apple", "apple"},
			b:        []string{"apple", "banana"},
			expected: true,
			notfound: "",
		},
		{
			name:     "duplicates in b",
			a:        []string{"apple"},
			b:        []string{"apple", "apple", "banana"},
			expected: true,
			notfound: "",
		},

		// Full/partial matches
		{
			name:     "exact match",
			a:        []string{"a", "b", "c"},
			b:        []string{"a", "b", "c"},
			expected: true,
			notfound: "",
		},
		{
			name:     "subset with extra elements",
			a:        []string{"a", "b"},
			b:        []string{"a", "b", "c", "d"},
			expected: true,
			notfound: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ele := ContainsAll(tt.a, tt.b)
			if got != tt.expected || ele != tt.notfound {
				t.Errorf("ContainsAll(%v, %v) = (%v, %s), want (%v, %s)",
					tt.a, tt.b, got, ele, tt.expected, tt.notfound)
			}
		})
	}
}

func TestTidInPid(t *testing.T) {
	selfPid := os.Getpid()

	// Read all TIDs from /proc/self/task/
	taskDir := fmt.Sprintf("/proc/%d/task", selfPid)
	entries, err := os.ReadDir(taskDir)
	if err != nil {
		t.Skipf("cannot read /proc/%d/task: %v, skipping test", selfPid, err)
	}

	if len(entries) == 0 {
		t.Skipf("no tasks found in /proc/%d/task, skipping test", selfPid)
	}

	// Build set of current TIDs and find maxTID
	var maxTid int
	tidSet := make(map[int]struct{})
	for _, entry := range entries {
		if tid, err := strconv.Atoi(entry.Name()); err == nil {
			tidSet[tid] = struct{}{}
			if tid > maxTid {
				maxTid = tid
			}
		}
	}

	if len(tidSet) == 0 {
		t.Skip("no valid TID parsed from task directory")
	}

	var validTid int
	for tid := range tidSet {
		validTid = tid
		break
	}

	invalidTid := maxTid + 1
	invalidPid := invalidTid

	tests := []struct {
		name     string
		tid      int
		pid      int
		expected bool
	}{
		{
			name:     "valid TID and correct PID: should return true",
			tid:      validTid,
			pid:      selfPid,
			expected: true,
		},
		{
			name:     "valid TID but wrong PID: should return false",
			tid:      validTid,
			pid:      invalidPid,
			expected: false,
		},
		{
			name:     "non-existent TID with correct PID: should return false",
			tid:      invalidTid,
			pid:      selfPid,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TidInPid(tt.tid, tt.pid)
			if got != tt.expected {
				t.Errorf("TidInPid(%d, %d) = %v, want %v", tt.tid, tt.pid, got, tt.expected)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		bytes    uint64
		expected string
	}{
		{
			name:     "zero bytes",
			bytes:    0,
			expected: "0 Bytes",
		},
		{
			name:     "less than 1 KiB",
			bytes:    512,
			expected: "512 Bytes",
		},
		{
			name:     "exactly 1 KiB",
			bytes:    1024,
			expected: "1.00 KiB (1024 Bytes)",
		},
		{
			name:     "1.5 KiB",
			bytes:    1536,
			expected: "1.50 KiB (1536 Bytes)",
		},
		{
			name:     "exactly 1 MiB",
			bytes:    1048576,
			expected: "1.00 MiB (1048576 Bytes)",
		},
		{
			name:     "2.5 MiB",
			bytes:    2621440,
			expected: "2.50 MiB (2621440 Bytes)",
		},
		{
			name:     "exactly 1 GiB",
			bytes:    1073741824,
			expected: "1.00 GiB (1073741824 Bytes)",
		},
		{
			name:     "1.2 GiB",
			bytes:    1288490189,
			expected: "1.20 GiB (1288490189 Bytes)",
		},
		{
			name:     "exactly 1 TiB",
			bytes:    1099511627776,
			expected: "1.00 TiB (1099511627776 Bytes)",
		},
		{
			name:     "5.7 TiB",
			bytes:    6269010895462,
			expected: "5.70 TiB (6269010895462 Bytes)",
		},
		{
			name:     "exactly 1 PiB",
			bytes:    1125899906842624,
			expected: "1.00 PiB (1125899906842624 Bytes)",
		},
		{
			name:     "3.4 PiB",
			bytes:    3830088282152140,
			expected: "3.40 PiB (3830088282152140 Bytes)",
		},
		{
			name:     "exactly 1 EiB",
			bytes:    1152921504606846976,
			expected: "1.00 EiB (1152921504606846976 Bytes)",
		},
		{
			name:     "large value 10 EiB",
			bytes:    11529215046068469760,
			expected: "10.00 EiB (11529215046068469760 Bytes)",
		},
		{
			name:     "boundary - 1023 bytes",
			bytes:    1023,
			expected: "1023 Bytes",
		},
		{
			name:     "single byte",
			bytes:    1,
			expected: "1 Bytes",
		},
		{
			name:     "100 KiB",
			bytes:    102400,
			expected: "100.00 KiB (102400 Bytes)",
		},
		{
			name:     "close to 1 MiB",
			bytes:    1023999,
			expected: "1000.00 KiB (1023999 Bytes)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatBytes(tt.bytes)
			if got != tt.expected {
				t.Errorf("FormatBytes(%d) = %s, want %s", tt.bytes, got, tt.expected)
			}
		})
	}
}

func TestFormatBytesShort(t *testing.T) {
	tests := []struct {
		name     string
		bytes    uint64
		expected string
	}{
		{"zero bytes", 0, "0B"},
		{"single byte", 1, "1B"},
		{"less than 1 KiB", 512, "512B"},
		{"boundary - 1023 bytes", 1023, "1023B"},
		{"exactly 1 KiB", 1024, "1.00KiB"},
		{"1.5 KiB", 1536, "1.50KiB"},
		{"100 KiB", 102400, "100.00KiB"},
		{"close to 1 MiB", 1023999, "1000.00KiB"},
		{"exactly 1 MiB", 1048576, "1.00MiB"},
		{"2.5 MiB", 2621440, "2.50MiB"},
		{"exactly 1 GiB", 1073741824, "1.00GiB"},
		{"1.2 GiB", 1288490189, "1.20GiB"},
		{"exactly 1 TiB", 1099511627776, "1.00TiB"},
		{"5.7 TiB", 6269010895462, "5.70TiB"},
		{"exactly 1 PiB", 1125899906842624, "1.00PiB"},
		{"3.4 PiB", 3830088282152140, "3.40PiB"},
		{"exactly 1 EiB", 1152921504606846976, "1.00EiB"},
		{"large value 10 EiB", 11529215046068469760, "10.00EiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatBytesShort(tt.bytes)
			if got != tt.expected {
				t.Errorf("FormatBytesShort(%d) = %s, want %s", tt.bytes, got, tt.expected)
			}
		})
	}
}

func TestLastNBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		n        int
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			n:        10,
			expected: "",
		},
		{
			name:     "n is zero",
			input:    "hello",
			n:        0,
			expected: "",
		},
		{
			name:     "n is negative",
			input:    "hello",
			n:        -5,
			expected: "",
		},
		{
			name:     "n is large negative",
			input:    "test",
			n:        -1000,
			expected: "",
		},
		{
			name:     "exact length",
			input:    "hello",
			n:        5,
			expected: "hello",
		},
		{
			name:     "shorter than n",
			input:    "hi",
			n:        10,
			expected: "hi",
		},
		{
			name:     "longer than n",
			input:    "hello world",
			n:        5,
			expected: "world",
		},
		{
			name:     "unicode string (byte-based)",
			input:    "你好世界", // 12 bytes in UTF-8
			n:        6,
			expected: "世界", // last 6 bytes = 2 Chinese chars
		},
		{
			name:     "large positive n",
			input:    "abc",
			n:        1000,
			expected: "abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := LastNBytes(tt.input, tt.n)
			if result != tt.expected {
				t.Errorf("LastNBytes(%q, %d) = %q, want %q", tt.input, tt.n, result, tt.expected)
			}
		})
	}
}

func TestSetupGoGc(t *testing.T) {
	const defaultLimit = int64(1 << 30)
	origPercent := debug.SetGCPercent(100)
	origLimit := debug.SetMemoryLimit(math.MaxInt64)
	defer func() {
		debug.SetGCPercent(origPercent)
		debug.SetMemoryLimit(origLimit)
	}()

	tests := []struct {
		name        string
		gogc        string
		gomemlimit  string
		conf        GoGcConfig
		wantPercent int
		wantLimit   int64
	}{
		{
			name:        "nothing set, use default",
			wantPercent: -1,
			wantLimit:   defaultLimit,
		},
		{
			name:        "both flags set",
			conf:        GoGcConfig{GCPercent: 50, GCPercentSet: true, MemLimit: 2 << 30},
			wantPercent: 50,
			wantLimit:   2 << 30,
		},
		{
			name:        "go gc zero is applied",
			conf:        GoGcConfig{GCPercent: 0, GCPercentSet: true},
			wantPercent: 0,
			wantLimit:   math.MaxInt64,
		},
		{
			name:        "only go memlimit set",
			conf:        GoGcConfig{MemLimit: 2 << 30},
			wantPercent: 100,
			wantLimit:   2 << 30,
		},
		{
			name:        "env takes precedence over flags",
			gogc:        "200",
			gomemlimit:  "4GiB",
			conf:        GoGcConfig{GCPercent: 50, GCPercentSet: true, MemLimit: 2 << 30},
			wantPercent: 100,
			wantLimit:   math.MaxInt64,
		},
		{
			name:        "GOGC set, go memlimit applied",
			gogc:        "200",
			conf:        GoGcConfig{GCPercent: 50, GCPercentSet: true, MemLimit: 2 << 30},
			wantPercent: 100,
			wantLimit:   2 << 30,
		},
		{
			name:        "GOMEMLIMIT set, go gc applied",
			gomemlimit:  "4GiB",
			conf:        GoGcConfig{GCPercent: 50, GCPercentSet: true, MemLimit: 2 << 30},
			wantPercent: 50,
			wantLimit:   math.MaxInt64,
		},
		{
			name:        "only env set, default not applied",
			gogc:        "200",
			wantPercent: 100,
			wantLimit:   math.MaxInt64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The runtime reads these env vars only at startup, they only affect SetupGoGc here.
			t.Setenv("GOGC", tt.gogc)
			t.Setenv("GOMEMLIMIT", tt.gomemlimit)
			debug.SetGCPercent(100)
			debug.SetMemoryLimit(math.MaxInt64)

			SetupGoGc(tt.conf, defaultLimit)

			if got := debug.SetGCPercent(100); got != tt.wantPercent {
				t.Errorf("GC percent = %d, want %d", got, tt.wantPercent)
			}
			if got := debug.SetMemoryLimit(math.MaxInt64); got != tt.wantLimit {
				t.Errorf("memory limit = %d, want %d", got, tt.wantLimit)
			}
		})
	}
}
