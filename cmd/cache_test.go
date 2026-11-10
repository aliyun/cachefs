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

package cmd

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/fuse"
	meta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
)

func TestProcessPath(t *testing.T) {
	// Setup test CLI context
	createCLI := func(config, value string) *cli.Context {
		set := flag.NewFlagSet("test", flag.ContinueOnError)
		set.String("cache-dir", "", "")
		set.String("other-path", "", "")
		c := cli.NewContext(nil, set, nil)
		c.Set(config, value)
		return c
	}

	// Create temp dir for test cases
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "testfile")
	os.WriteFile(testFile, []byte{}, 0644)

	tests := []struct {
		name           string
		cliValue       string
		configName     string
		required       bool
		wantPath       string
		wantCreated    bool
		wantErr        bool
		errMsgContains string
	}{
		// Required path missing
		{
			name:           "required missing",
			cliValue:       "",
			configName:     "cache-dir",
			required:       true,
			wantErr:        true,
			errMsgContains: "Missing --cache-dir",
		},
		// Memory cache case
		{
			name:        "memory cache",
			cliValue:    "memory",
			configName:  "cache-dir",
			required:    true,
			wantPath:    "memory",
			wantCreated: false,
		},
		// Successful directory creation
		{
			name:        "create new cache dir",
			cliValue:    filepath.Join(tempDir, "newdir"),
			configName:  "cache-dir",
			required:    true,
			wantPath:    filepath.Join(tempDir, "newdir"),
			wantCreated: true,
		},
		// Existing directory
		{
			name:        "existing dir",
			cliValue:    tempDir,
			configName:  "cache-dir",
			required:    true,
			wantPath:    tempDir,
			wantCreated: false,
		},
		// Path is a file
		{
			name:           "path is file",
			cliValue:       testFile,
			configName:     "cache-dir",
			required:       true,
			wantErr:        true,
			errMsgContains: "is not a directory",
		},
		// Non-cache-dir path missing
		{
			name:           "non-cache missing",
			cliValue:       filepath.Join(tempDir, "invalid.path"),
			configName:     "other-path",
			required:       true,
			wantErr:        true,
			errMsgContains: "no such file or directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := createCLI(tt.configName, tt.cliValue)
			gotPath, gotCreated, err := processPath(c, tt.configName, tt.required)

			// Validate error cases
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsgContains) {
					t.Errorf("Error %q should contain %q", err.Error(), tt.errMsgContains)
				}
				return
			}

			// Validate success cases
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Verify path processing
			if tt.wantPath != "memory" { // Skip path checks for memory case
				// Check absolute path conversion
				if !filepath.IsAbs(gotPath) {
					t.Errorf("Path should be absolute: %s", gotPath)
				}
				// Check symlink resolution (compare eval symlinks)
				evaluated, _ := filepath.EvalSymlinks(gotPath)
				if evaluated != gotPath {
					t.Errorf("Path should resolve symlinks: %s vs %s", evaluated, gotPath)
				}
			}

			if gotPath != tt.wantPath {
				t.Errorf("Path mismatch: got %q, want %q", gotPath, tt.wantPath)
			}

			if gotCreated != tt.wantCreated {
				t.Errorf("Created flag: got %v, want %v", gotCreated, tt.wantCreated)
			}
		})
	}
}

func TestCheckPathDependency(t *testing.T) {
	tmpDir := os.TempDir()
	tests := []struct {
		path1     string
		path2     string
		shouldErr bool
	}{
		{"./testdata/sourcedir", "./testdata/cachedir", true},
		{"./testdata/sourcedir", "./testdata/sourcedir/subdir", false},
		{tmpDir, "/not/existent/path", true},
		{tmpDir, tmpDir, false},
		{filepath.Join(tmpDir, "subdir"), tmpDir, false},
		{tmpDir, filepath.Join(tmpDir, "subdir"), false},
		{tmpDir, os.TempDir(), false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("path1=%s path2=%s", tt.path1, tt.path2), func(t *testing.T) {
			if got := checkPathDependency(tt.path1, tt.path2); got != tt.shouldErr {
				t.Errorf("checkPathDependency() = %v, want %v", got, tt.shouldErr)
			}
		})
	}
}

func TestIsSubdirectory(t *testing.T) {
	tests := []struct {
		name     string
		child    string
		parent   string
		expected bool
	}{
		{"ChildIsSubdir", "parent/child", "parent", true},
		{"RootIsNotSubdir", "/", "parent", false},
		{"SameDirIsSubdir", "parent", "parent", true},
		{"NestedSubdir", "parent/child/subchild", "parent", true},
		{"SiblingDirIsNotSubdir", "parent/sibling", "parent/child", false},
		{"ChildIsParent", "parent", "parent/child", false},
		{"ChildAboveParent", "../parent", "otherparent", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			childPath := filepath.Clean(filepath.FromSlash(tt.child))
			parentPath := filepath.Clean(filepath.FromSlash(tt.parent))

			if got := isSubdirectory(childPath, parentPath); got != tt.expected {
				t.Errorf("isSubdirectory(%v, %v) = %v, want %v", tt.child, tt.parent, got, tt.expected)
			}
		})
	}
}

// Wrapper inputValue to implement flag.Value
type inputValue struct {
	input string
}

func (i *inputValue) String() string {
	return i.input
}

func (i *inputValue) Set(s string) error {
	return nil
}

func TestGenStoreConf(t *testing.T) {
	tests := []struct {
		name            string
		option          string
		input           *inputValue
		overrides       map[string]string
		expect          error
		checkBurstBytes bool // if true, verify conf.BurstCacheSize == wantBurstBytes
		wantBurstBytes  int64
		wantInitialB    int64 // conf.InitialBurstSize, checked along with wantBurstBytes
		checkDeferBurst bool
		wantDeferBurst  bool
		checkRootAddrs  bool // if true, verify conf.CacheRootAddrs == wantRootAddrs
		wantRootAddrs   []string
		checkDirectIO   bool // if true, verify conf.Cache{Read,Write}DirectIO
		wantReadDIO     bool
		wantWriteDIO    bool
		checkGoGc       bool // if true, verify conf.GoGc == wantGoGc
		wantGoGc        utils.GoGcConfig
		checkBlockLRU   bool
		wantBlockLRU    int
	}{
		{
			name:   "invalid block size: 0",
			option: "block-size",
			input:  &inputValue{"0"},
			expect: errors.New("Only support block size in 4/16/32/64/128 MiB"),
		},
		{
			name:   "invalid block size: <4",
			option: "block-size",
			input:  &inputValue{"2"},
			expect: errors.New("Only support block size in 4/16/32/64/128 MiB"),
		},
		{
			name:   "invalid block size: >128",
			option: "block-size",
			input:  &inputValue{"256"},
			expect: errors.New("Only support block size in 4/16/32/64/128 MiB"),
		},
		{
			name:   "invalid block size: not power of 2",
			option: "block-size",
			input:  &inputValue{"255"},
			expect: errors.New("Only support block size in 4/16/32/64/128 MiB"),
		},
		{
			name:   "invalid block size: negative value",
			option: "block-size",
			input:  &inputValue{"-1"},
			expect: errors.New("Only support block size in 4/16/32/64/128 MiB"),
		},
		{
			name:   "valid block size",
			option: "block-size",
			input:  &inputValue{"4"},
			expect: nil,
		},
		{
			name:   "invalid cache size: <0",
			option: "cache-size",
			input:  &inputValue{"-1"},
			expect: errors.New("Invalid cache size: -1MiB"),
		},
		{
			name:   "valid cache size: =0",
			option: "cache-size",
			input:  &inputValue{"0"},
			expect: nil,
		},
		{
			name:   "valid cache size",
			option: "cache-size",
			input:  &inputValue{"10"},
			expect: nil,
		},
		{
			name:          "prefetch sets block LRU size",
			option:        "prefetch",
			input:         &inputValue{"48"},
			checkBlockLRU: true,
			wantBlockLRU:  48,
		},
		{
			name:          "zero prefetch disables block LRU",
			option:        "prefetch",
			input:         &inputValue{"0"},
			checkBlockLRU: true,
			wantBlockLRU:  0,
		},
		{
			name:          "negative prefetch disables block LRU",
			option:        "prefetch",
			input:         &inputValue{"-1"},
			checkBlockLRU: true,
			wantBlockLRU:  0,
		},
		{
			name:   "lo cache nic",
			option: "cache-nic",
			input:  &inputValue{"lo"},
			expect: nil,
		},
		{
			name:   "invalid cache nic",
			option: "cache-nic",
			input:  &inputValue{"loo"},
			expect: errors.New("Get IP of cache server: failed to get interface loo: route ip+net: no such network interface"),
		},
		{
			name:   "rdma no transport nic",
			option: "transport-nic",
			input:  &inputValue{""},
			expect: nil,
		},
		{
			name:   "invalid rdma buffer size: 0",
			option: "rdma-buffer-size",
			input:  &inputValue{"0"},
			expect: errors.New("RDMA buffer size must be power of 2"),
		},
		{
			name:   "invalid rdma buffer size: <0",
			option: "rdma-buffer-size",
			input:  &inputValue{"-1"},
			expect: errors.New("RDMA buffer size must be power of 2"),
		},
		{
			name:   "invalid rdma buffer size: 14",
			option: "rdma-buffer-size",
			input:  &inputValue{"14"},
			expect: errors.New("RDMA buffer size must be power of 2"),
		},
		{
			name:   "valid rdma buffer size: <128",
			option: "rdma-buffer-size",
			input:  &inputValue{"16"},
			expect: nil,
		},
		{
			name:   "invalid rdma max usage: 0",
			option: "rdma-max-mem",
			input:  &inputValue{"0"},
			expect: errors.New("Max RDMA buffer memory usage must be > 0"),
		},
		{
			name:   "invalid rdma max usage: <0",
			option: "rdma-max-mem",
			input:  &inputValue{"-1"},
			expect: errors.New("Max RDMA buffer memory usage must be > 0"),
		},
		{
			name:   "valid rdma max usage: 14",
			option: "rdma-max-mem",
			input:  &inputValue{"14"},
			expect: nil,
		},
		{
			name:   "invalid rdma buffer count: 0",
			option: "rdma-buffer-count",
			input:  &inputValue{"0"},
			expect: errors.New("Invalid RDMA buffer count: 0"),
		},
		{
			name:   "invalid rdma buffer count: <0",
			option: "rdma-buffer-count",
			input:  &inputValue{"-1"},
			expect: errors.New("Invalid RDMA buffer count: -1"),
		},
		{
			name:   "rdma buffer count: =120",
			option: "rdma-buffer-count",
			input:  &inputValue{"120"},
			expect: nil,
		},
		{
			name:   "rdma buffer count: >120, reset to 120",
			option: "rdma-buffer-count",
			input:  &inputValue{"121"},
			expect: nil,
		},
		{
			name:   "rdma max memory too small",
			option: "rdma-max-mem",
			input:  &inputValue{"2"},
			expect: errors.New("RDMA max memory usage is too small to support one connection"),
		},
		// burst-cache-size cases (cache-size defaults to 100 in newContext)
		{
			name:   "invalid burst cache size: <0",
			option: "burst-cache-size",
			input:  &inputValue{"-1"},
			expect: errors.New("Invalid burst cache size: -1MiB"),
		},
		{
			name:            "burst cache size: =0, disabled",
			option:          "burst-cache-size",
			input:           &inputValue{"0"},
			checkBurstBytes: true,
			wantBurstBytes:  int64(100) << 20, // 0 < cache-size, raised to cache-size
			wantInitialB:    int64(100) << 20,
		},
		{
			name:            "burst cache size: smaller than cache-size",
			option:          "burst-cache-size",
			input:           &inputValue{"50"},
			checkBurstBytes: true,
			wantBurstBytes:  int64(100) << 20, // 50 < cache-size(100), raised to cache-size
			wantInitialB:    int64(100) << 20,
		},
		{
			name:            "burst cache size: equal to cache-size",
			option:          "burst-cache-size",
			input:           &inputValue{"100"},
			checkBurstBytes: true,
			wantBurstBytes:  int64(100) << 20,
			wantInitialB:    int64(100) << 20,
		},
		{
			name:            "burst cache size: larger than cache-size",
			option:          "burst-cache-size",
			input:           &inputValue{"200"},
			checkBurstBytes: true,
			wantBurstBytes:  int64(200) << 20,
			wantInitialB:    int64(200) << 20,
		},
		{
			name:            "defer burst cache size",
			overrides:       map[string]string{"burst-cache-size": "200", "defer-burst": "true"},
			checkBurstBytes: true,
			wantBurstBytes:  int64(100) << 20, // held back to cache-size until restored
			wantInitialB:    int64(200) << 20,
			checkDeferBurst: true,
			wantDeferBurst:  true,
		},
		{
			name:   "invalid source id",
			option: "source-id",
			input:  &inputValue{"invalid/id"},
			expect: fmt.Errorf("Source ID \"%s\" must not contain \"/\"", "invalid/id"),
		},
		{
			name:           "cacheroot-tracker: addr without port auto-appends default port",
			option:         "cacheroot-tracker",
			input:          &inputValue{"host1"},
			checkRootAddrs: true,
			wantRootAddrs:  []string{"host1:80"},
		},
		{
			name:           "cacheroot-tracker: multiple addrs all without port",
			option:         "cacheroot-tracker",
			input:          &inputValue{"host1 host2"},
			checkRootAddrs: true,
			wantRootAddrs:  []string{"host1:80", "host2:80"},
		},
		{
			name:           "cacheroot-tracker: multiple addrs mixed with and without port",
			option:         "cacheroot-tracker",
			input:          &inputValue{"host1 host2:8811"},
			checkRootAddrs: true,
			wantRootAddrs:  []string{"host1:80", "host2:8811"},
		},
		{
			name:   "cacheroot-tracker: invalid addr returns error",
			option: "cacheroot-tracker",
			input:  &inputValue{"host1:host2:8811"},
			expect: fmt.Errorf("Cache root tracker address: %s", "address host1:host2:8811: too many colons in address"),
		},
		{
			name:          "cache direct io: disabled by default",
			option:        "block-size",
			input:         &inputValue{"4"},
			expect:        nil,
			checkDirectIO: true,
			wantReadDIO:   false,
			wantWriteDIO:  false,
		},
		{
			name:          "cache read direct io enabled",
			option:        "cache-read-direct-io",
			input:         &inputValue{"true"},
			expect:        nil,
			checkDirectIO: true,
			wantReadDIO:   true,
			wantWriteDIO:  false,
		},
		{
			name:          "cache write direct io enabled",
			option:        "cache-write-direct-io",
			input:         &inputValue{"true"},
			expect:        nil,
			checkDirectIO: true,
			wantReadDIO:   false,
			wantWriteDIO:  true,
		},
		{
			name:      "go gc: not set by default",
			option:    "block-size",
			input:     &inputValue{"4"},
			checkGoGc: true,
			wantGoGc:  utils.GoGcConfig{},
		},
		{
			name:      "go gc: zero is set",
			option:    "block-size",
			input:     &inputValue{"4"},
			overrides: map[string]string{"go-gc": "0"},
			checkGoGc: true,
			wantGoGc:  utils.GoGcConfig{GCPercent: 0, GCPercentSet: true},
		},
		{
			name:      "go gc: negative disables gc",
			option:    "block-size",
			input:     &inputValue{"4"},
			overrides: map[string]string{"go-gc": "-1"},
			checkGoGc: true,
			wantGoGc:  utils.GoGcConfig{GCPercent: -1, GCPercentSet: true},
		},
		{
			name:      "go memlimit: valid MiB",
			option:    "block-size",
			input:     &inputValue{"4"},
			overrides: map[string]string{"go-gc": "50", "go-memlimit": "2048"},
			checkGoGc: true,
			wantGoGc:  utils.GoGcConfig{GCPercent: 50, GCPercentSet: true, MemLimit: 2048 << 20},
		},
		{
			name:      "go memlimit: zero ignored",
			option:    "block-size",
			input:     &inputValue{"4"},
			overrides: map[string]string{"go-memlimit": "0"},
			checkGoGc: true,
			wantGoGc:  utils.GoGcConfig{},
		},
		{
			name:      "go memlimit: negative ignored",
			option:    "block-size",
			input:     &inputValue{"4"},
			overrides: map[string]string{"go-memlimit": "-1"},
			checkGoGc: true,
			wantGoGc:  utils.GoGcConfig{},
		},
		{
			name:      "go memlimit: overflow ignored",
			option:    "block-size",
			input:     &inputValue{"4"},
			overrides: map[string]string{"go-memlimit": "9223372036854775807"},
			checkGoGc: true,
			wantGoGc:  utils.GoGcConfig{},
		},
	}

	assert := assert.New(t)

	newContext := func(tc string, input *inputValue, overrides map[string]string) *cli.Context {
		// Prepare a valid cli.Context, set all required options
		type defValue struct {
			option string
			defVal string
			isBool bool
		}
		flagSet := &flag.FlagSet{}
		opts := []defValue{
			{"block-size", "4", false},
			{"cache-size", "100", false},
			{"burst-cache-size", "0", false},
			{"prefetch", "24", false},
			{"defer-burst", "false", true},
			{"cache-nic", "lo", false},
			{"transport", "rdma", false},
			{"transport-nic", "erdma_0", false}, // genStoreConf won't check if rdma dev exists
			{"rdma-buffer-size", "256", false},
			{"rdma-buffer-count", "16", false},
			{"rdma-max-mem", "1024", false},
			{"cacheroot-tracker", "", false},
			{"source-id", "", false},
			{"cache-read-direct-io", "false", false},
			{"cache-write-direct-io", "false", false},
		}

		for _, opt := range opts {
			value := opt.defVal
			if opt.option == tc {
				value = input.input
			}
			if v, ok := overrides[opt.option]; ok {
				value = v
			}
			if opt.isBool {
				flagSet.Bool(opt.option, value == "true", "")
			} else {
				flagSet.Var(&inputValue{value}, opt.option, "")
			}
		}
		// Set explicitly so that c.IsSet reports them as given on the command line.
		flagSet.Int("go-gc", 0, "")
		flagSet.Int64("go-memlimit", 0, "")
		for _, name := range []string{"go-gc", "go-memlimit"} {
			if v, ok := overrides[name]; ok {
				if err := flagSet.Set(name, v); err != nil {
					t.Errorf("%s: set --%s %s: %s", tc, name, v, err)
				}
			}
		}
		return cli.NewContext(nil, flagSet, nil)
	}

	for _, tc := range tests {
		conf, err := genStoreConf(newContext(tc.option, tc.input, tc.overrides), "/mnt/source")
		assert.Equal(tc.expect, err, tc.name)
		if conf != nil && tc.checkBurstBytes {
			assert.Equal(tc.wantBurstBytes, conf.BurstCacheSize, tc.name)
			assert.Equal(tc.wantInitialB, conf.InitialBurstSize, tc.name)
		}
		if conf != nil && tc.checkDeferBurst {
			assert.Equal(tc.wantDeferBurst, conf.DeferBurst, tc.name)
		}
		if conf != nil && tc.checkRootAddrs {
			assert.Equal(tc.wantRootAddrs, conf.CacheRootAddrs, tc.name)
		}
		if conf != nil && tc.checkDirectIO {
			assert.Equal(tc.wantReadDIO, conf.CacheReadDirectIO, tc.name)
			assert.Equal(tc.wantWriteDIO, conf.CacheWriteDirectIO, tc.name)
		}
		if conf != nil && tc.checkGoGc {
			assert.Equal(tc.wantGoGc, conf.GoGc, tc.name)
		}
		if conf != nil && tc.checkBlockLRU {
			assert.Equal(tc.wantBlockLRU, conf.BlockLRUSize, tc.name)
		}
		if tc.option == "transport" || tc.option == "cacheroot-transport" {
			input := tc.input.input
			var testConf string
			if tc.option == "cacheroot-transport" {
				testConf = conf.CacheRootXprt
			} else if tc.option == "transport" {
				testConf = conf.Transport
			}
			assert.Equal(strings.ToLower(input), testConf)
		}
	}
}

func TestConfigureCacheSize(t *testing.T) {
	newContext := func(cacheSizeMiB, burstSizeMiB int, deferBurst bool) *cli.Context {
		flagSet := &flag.FlagSet{}
		flagSet.Int("cache-size", cacheSizeMiB, "")
		flagSet.Int("burst-cache-size", burstSizeMiB, "")
		flagSet.Bool("defer-burst", deferBurst, "")
		return cli.NewContext(nil, flagSet, nil)
	}

	tests := []struct {
		name            string
		cacheSizeMiB    int
		burstSizeMiB    int
		deferBurst      bool
		wantErr         error
		wantCacheSize   int64
		wantBurstSize   int64
		wantInitialSize int64
		wantDefer       bool
	}{
		{
			name:         "invalid cache size",
			cacheSizeMiB: -1,
			wantErr:      errors.New("Invalid cache size: -1MiB"),
		},
		{
			name:         "invalid burst cache size",
			cacheSizeMiB: 100,
			burstSizeMiB: -1,
			wantErr:      errors.New("Invalid burst cache size: -1MiB"),
		},
		{
			name:            "burst cache disabled",
			cacheSizeMiB:    100,
			burstSizeMiB:    0,
			wantCacheSize:   int64(100) << 20,
			wantBurstSize:   int64(100) << 20,
			wantInitialSize: int64(100) << 20,
		},
		{
			name:            "burst cache smaller than cache size",
			cacheSizeMiB:    100,
			burstSizeMiB:    50,
			wantCacheSize:   int64(100) << 20,
			wantBurstSize:   int64(100) << 20,
			wantInitialSize: int64(100) << 20,
		},
		{
			name:            "burst cache larger than cache size",
			cacheSizeMiB:    100,
			burstSizeMiB:    200,
			wantCacheSize:   int64(100) << 20,
			wantBurstSize:   int64(200) << 20,
			wantInitialSize: int64(200) << 20,
		},
		{
			name:            "defer burst cache size",
			cacheSizeMiB:    100,
			burstSizeMiB:    200,
			deferBurst:      true,
			wantCacheSize:   int64(100) << 20,
			wantBurstSize:   int64(100) << 20,
			wantInitialSize: int64(200) << 20,
			wantDefer:       true,
		},
	}

	for _, tt := range tests {
		cfg := &chunk.StoreConfig{}
		err := configureCacheSize(newContext(tt.cacheSizeMiB, tt.burstSizeMiB, tt.deferBurst), cfg)
		assert.Equal(t, tt.wantErr, err, tt.name)
		if tt.wantErr == nil {
			assert.Equal(t, tt.wantCacheSize, cfg.CacheSize, tt.name)
			assert.Equal(t, tt.wantBurstSize, cfg.BurstCacheSize, tt.name)
			assert.Equal(t, tt.wantInitialSize, cfg.InitialBurstSize, tt.name)
			assert.Equal(t, tt.wantDefer, cfg.DeferBurst, tt.name)
		}
	}
}

func TestTransportNicParsing(t *testing.T) {
	assert := assert.New(t)

	newContext := func(transportNic string) *cli.Context {
		flagSet := &flag.FlagSet{}
		opts := []struct {
			option string
			defVal string
		}{
			{"block-size", "4"},
			{"cache-size", "100"},
			{"cache-nic", "lo"},
			{"transport", "tcp"},
			{"transport-nic", transportNic},
		}
		for _, opt := range opts {
			flagSet.Var(&inputValue{opt.defVal}, opt.option, "")
		}
		return cli.NewContext(nil, flagSet, nil)
	}

	tests := []struct {
		name   string
		input  string
		expect []string
	}{
		{"comma-separated", "eth0,eth1,eth2", []string{"eth0", "eth1", "eth2"}},
		{"space-separated", "eth0 eth1 eth2", []string{"eth0", "eth1", "eth2"}},
		{"mixed comma and space", "eth0,eth1 eth2", []string{"eth0", "eth1", "eth2"}},
		{"single value", "eth0", []string{"eth0"}},
		{"empty", "", []string{}},
		{"trailing comma", "eth0,eth1,", []string{"eth0", "eth1"}},
		{"comma with spaces", "eth0, eth1, eth2", []string{"eth0", "eth1", "eth2"}},
		{"tab-separated", "eth0\teth1\teth2", []string{"eth0", "eth1", "eth2"}},
		{"newline-separated", "eth0\neth1\neth2", []string{"eth0", "eth1", "eth2"}},
		{"vertical-tab-separated", "eth0\veth1\veth2", []string{"eth0", "eth1", "eth2"}},
		{"form-feed-separated", "eth0\feth1\feth2", []string{"eth0", "eth1", "eth2"}},
		{"carriage-return-separated", "eth0\r\neth1\r\neth2", []string{"eth0", "eth1", "eth2"}},
		{"NEL-separated", "eth0\u0085eth1\u0085eth2", []string{"eth0", "eth1", "eth2"}},
		{"NBSP-separated", "eth0\u00A0eth1\u00A0eth2", []string{"eth0", "eth1", "eth2"}},
		{"mixed tab and comma", "eth0,\teth1,\teth2", []string{"eth0", "eth1", "eth2"}},
		{"leading separators", ", eth0,eth1", []string{"eth0", "eth1"}},
		{"trailing separators", "eth0,eth1, ", []string{"eth0", "eth1"}},
		{"leading and trailing separators", " ,eth0,eth1, ", []string{"eth0", "eth1"}},
		{"multiple commas between nics", "eth0,,eth1", []string{"eth0", "eth1"}},
		{"multiple spaces between nics", "eth0  eth1", []string{"eth0", "eth1"}},
		{"multiple mixed separators between nics", "eth0, , \teth1", []string{"eth0", "eth1"}},
		{"only separators", ", , ", []string{}},
	}
	for _, tc := range tests {
		conf, err := genStoreConf(newContext(tc.input), "/mnt/source")
		assert.NoError(err, tc.name)
		assert.Equal(tc.expect, conf.TransportNics, tc.name)
	}
}

func TestGenSerfConf(t *testing.T) {
	storeConf := &chunk.StoreConfig{
		CacheAddr:     "127.0.0.1:8888",
		CachePort:     8888,
		CacheIP:       "127.0.0.1",
		CacheEviction: chunk.HashCleanup,
	}
	tests := []struct {
		name      string
		option    string
		input     *inputValue
		storeConf *chunk.StoreConfig
		expect    error
	}{
		{
			name:      "invalid serf reconnect timeout: -1",
			option:    "serf-reconnect-timeout",
			input:     &inputValue{"-1"},
			storeConf: storeConf,
			expect:    errors.New("--serf-reconnect-timeout must be >= 0"),
		},
		{
			name:      "invalid serf leave delay: -1",
			option:    "serf-leave-delay",
			input:     &inputValue{"-1"},
			storeConf: storeConf,
			expect:    errors.New("--serf-leave-delay must be >= 0"),
		},
		{
			name:      "valid config",
			option:    "serf-port",
			input:     &inputValue{"11111"},
			storeConf: storeConf,
			expect:    nil,
		},
		{
			name:      "valid virtual-node-count",
			option:    "virtual-node-count",
			input:     &inputValue{"100"},
			storeConf: storeConf,
			expect:    nil,
		},
		{
			name:      "0 virtual-node-count",
			option:    "virtual-node-count",
			input:     &inputValue{"0"},
			storeConf: storeConf,
			expect:    errors.New("--virtual-node-count must be > 0"),
		},
		{
			name:      "-1 virtual-node-count",
			option:    "virtual-node-count",
			input:     &inputValue{"-1"},
			storeConf: storeConf,
			expect:    errors.New("--virtual-node-count must be > 0"),
		},
	}

	assert := assert.New(t)

	newContext := func(tc string, input *inputValue) *cli.Context {
		// Prepare a valid cli.Context, set all required options
		type defValue struct {
			option string
			defVal string
		}
		flagSet := &flag.FlagSet{}
		opts := []defValue{
			{"serf-port", "7946"},
			{"serf-join", ""},
			{"serf-memberlist-cmd", ""},
			{"serf-maxretry", "10"},
			{"serf-group", ""},
			{"serf-node-name", ""},
			{"serf-reconnect-timeout", "10"},
			{"serf-leave-delay", "20"},
			{"virtual-node-count", "100"},
		}

		for _, opt := range opts {
			if opt.option == tc {
				flagSet.Var(input, opt.option, "")
			} else {
				flagSet.Var(&inputValue{opt.defVal}, opt.option, "")
			}
		}
		c := cli.NewContext(nil, flagSet, nil)
		return c
	}

	for _, tc := range tests {
		conf, err := genSerfConf(newContext(tc.option, tc.input), tc.storeConf)
		assert.Equal(tc.expect, err, tc.name)
		if tc.expect == nil {
			assert.Equal(tc.storeConf.CacheAddr, conf.CacheAddr, "cache addr")
			assert.Equal(tc.storeConf.CachePort, conf.CachePort, "cache port")
			assert.Equal(tc.storeConf.CacheIP, conf.BindAddr, "bind addr")
			assert.Equal(tc.storeConf.HashringEnabled(), conf.HashRing, "hash ring")
		}
	}
}

func TestGenMetaConf(t *testing.T) {
	// Mock CLI context helper
	mockCli := func(params map[string]interface{}) *cli.Context {
		set := flag.NewFlagSet("test", flag.ContinueOnError)
		// Register all flags used in genMetaConf
		set.Bool("sourceless", false, "")
		set.String("source-subpath", "", "")
		set.String("source-subpath-file", "", "")
		set.Int("source-subpath-reload", 0, "")
		set.Int("source-peermeta", 0, "")
		set.String("peermeta-mode", "auto", "")
		set.Int("readdir-cache", 0, "")
		set.Bool("attr-cleanup", false, "")
		set.Int("metaop-timeout", 0, "")
		set.Bool("no-pagecache", false, "")

		for k, v := range params {
			set.Set(k, fmt.Sprintf("%v", v))
		}

		return cli.NewContext(nil, set, nil)
	}

	tests := []struct {
		name        string
		cliParams   map[string]interface{}
		fsname      string
		mp          string
		sourceDir   string
		cacheSize   int64
		wantCfg     *meta.MetaConfig
		wantErr     bool
		errContains string
	}{
		{
			name:      "basic config",
			cliParams: map[string]interface{}{},
			fsname:    "testfs",
			mp:        "/mnt",
			sourceDir: "/source",
			cacheSize: 1024,
			wantCfg: &meta.MetaConfig{
				Name:           "testfs",
				MountPoint:     "/mnt",
				SourceDirs:     []string{"/source"},
				Sourceless:     false,
				CacheSize:      0, // Should be 0 in non-sourceless
				FuseCache:      true,
				ReaddirTimeout: 0,
				MetaOpTimeout:  0,
				SubPathConfig:  meta.SourceSubPathConfig{},
			},
		},
		{
			name: "attr cleanup enabled",
			cliParams: map[string]interface{}{
				"attr-cleanup":  true,
				"readdir-cache": 60,
			},
			wantCfg: &meta.MetaConfig{AttrCleanup: true, ReaddirTimeout: 60},
		},
		{
			name: "attr cleanup with metaop timeout",
			cliParams: map[string]interface{}{
				"attr-cleanup":   true,
				"readdir-cache":  60,
				"metaop-timeout": 5,
			},
			wantCfg: &meta.MetaConfig{AttrCleanup: true, ReaddirTimeout: 60},
		},
		{
			name: "attr cleanup requires readdir timeout",
			cliParams: map[string]interface{}{
				"attr-cleanup":  true,
				"readdir-cache": -1,
			},
			wantCfg: &meta.MetaConfig{ReaddirTimeout: -1},
		},
		{
			name: "attr cleanup disabled for sourceless",
			cliParams: map[string]interface{}{
				"attr-cleanup":  true,
				"sourceless":    true,
				"readdir-cache": 60,
			},
			cacheSize: 2048,
			wantCfg:   &meta.MetaConfig{Sourceless: true, CacheSize: 2048, ReaddirTimeout: -1},
		},
		{
			name: "attr cleanup disabled for peer meta",
			cliParams: map[string]interface{}{
				"attr-cleanup":    true,
				"source-peermeta": 3,
				"readdir-cache":   60,
			},
			wantCfg: &meta.MetaConfig{ReaddirTimeout: -1},
		},
		{
			name: "sourceless mode",
			cliParams: map[string]interface{}{
				"sourceless": true,
			},
			fsname:    "nocache",
			mp:        "/mnt/nc",
			sourceDir: "",
			cacheSize: 2048,
			wantCfg: &meta.MetaConfig{
				Sourceless:     true,
				CacheSize:      2048,
				ReaddirTimeout: -1,
				SubPathConfig:  meta.SourceSubPathConfig{},
			},
		},
		{
			name: "subpath reload adjustment",
			cliParams: map[string]interface{}{
				"attr-cleanup":          true,
				"source-subpath-file":   "/paths.txt",
				"source-subpath-reload": 10,
				"readdir-cache":         -1,
			},
			wantCfg: &meta.MetaConfig{
				AttrCleanup:    true,
				ReaddirTimeout: 30,
				SubPathConfig: meta.SourceSubPathConfig{
					File:   "/paths.txt",
					Reload: 10,
				},
			},
		},
		{
			name: "metaop timeout clamp",
			cliParams: map[string]interface{}{
				"metaop-timeout": -5,
			},
			wantCfg: &meta.MetaConfig{
				MetaOpTimeout: 0,
			},
		},
		{
			name: "peermeta and subpath reload are incompatible",
			cliParams: map[string]interface{}{
				"source-peermeta":       3,
				"source-subpath-file":   "/paths.txt",
				"source-subpath-reload": 10,
			},
			wantErr:     true,
			errContains: "incompatible",
		},
		{
			name: "valid peermeta-mode is accepted",
			cliParams: map[string]interface{}{
				"peermeta-mode": "manual",
			},
			wantCfg: &meta.MetaConfig{},
		},
		{
			name: "invalid peermeta-mode is rejected",
			cliParams: map[string]interface{}{
				"peermeta-mode": "bad",
			},
			wantErr:     true,
			errContains: "auto' or 'manual'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := mockCli(tt.cliParams)
			var sourceDirs []string
			if tt.sourceDir != "" {
				sourceDirs = []string{tt.sourceDir}
			}
			cfg, err := genMetaConf(c, tt.fsname, tt.mp, sourceDirs, tt.cacheSize)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("Error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.wantCfg.Name != "" && cfg.Name != tt.wantCfg.Name {
				t.Errorf("Name: got %q want %q", cfg.Name, tt.wantCfg.Name)
			}
			if tt.wantCfg.MountPoint != "" && cfg.MountPoint != tt.wantCfg.MountPoint {
				t.Errorf("MountPoint: got %q want %q", cfg.MountPoint, tt.wantCfg.MountPoint)
			}
			if cfg.Sourceless != tt.wantCfg.Sourceless {
				t.Errorf("Sourceless: got %v want %v", cfg.Sourceless, tt.wantCfg.Sourceless)
			}
			if cfg.CacheSize != tt.wantCfg.CacheSize {
				t.Errorf("CacheSize: got %d want %d", cfg.CacheSize, tt.wantCfg.CacheSize)
			}
			if cfg.ReaddirTimeout != tt.wantCfg.ReaddirTimeout {
				t.Errorf("ReaddirTimeout: got %d want %d", cfg.ReaddirTimeout, tt.wantCfg.ReaddirTimeout)
			}
			if cfg.AttrCleanup != tt.wantCfg.AttrCleanup {
				t.Errorf("AttrCleanup: got %v want %v", cfg.AttrCleanup, tt.wantCfg.AttrCleanup)
			}
			if cfg.MetaOpTimeout != tt.wantCfg.MetaOpTimeout {
				t.Errorf("MetaOpTimeout: got %d want %d", cfg.MetaOpTimeout, tt.wantCfg.MetaOpTimeout)
			}
			if cfg.SubPathConfig != tt.wantCfg.SubPathConfig {
				t.Errorf("SubPathConfig: got %+v want %+v", cfg.SubPathConfig, tt.wantCfg.SubPathConfig)
			}

		})
	}
}

func TestGenConf(t *testing.T) {
	// Mock CLI context helper
	mockCli := func(params map[string]interface{}) *cli.Context {
		set := flag.NewFlagSet("test", flag.ContinueOnError)
		// Register all flags used in genConf
		set.Int("share-reserve", 0, "")
		// Same defaults as cachefsFlags(), so the cases that leave these unset
		// exercise the real out-of-the-box behaviour.
		set.Int("buffer-size", 0, "")
		set.Bool("buffer-reuse", true, "")
		set.Bool("buffer-prefault", true, "")
		set.Int("buffer-timeout", 180, "")
		set.Int("prefetch", 3, "")
		set.Int("prefetch-workers", 0, "")
		set.String("preload", "", "")
		set.Int("preload-workers", 0, "")
		set.Int("preload-interval", 0, "")
		set.Bool("force-preload", false, "")
		set.String("api-server-sock", "", "")
		set.Bool("enable-api-server", false, "")
		set.String("socket", "", "")
		set.Bool("unsafe-cache-key", false, "")
		set.Int("negative-timeout", 0, "")
		set.Bool("sourceless", false, "")
		set.String("pin-cache", "", "")
		set.String("pin-cache-file", "", "")
		set.Int("pin-cache-reload", 0, "")

		for k, v := range params {
			set.Set(k, fmt.Sprintf("%v", v))
		}

		return cli.NewContext(nil, set, nil)
	}

	// Create mock configs
	mockMetaConf := &meta.MetaConfig{
		Name:           "testfs",
		MountPoint:     "/mnt/test",
		SourceDirs:     []string{"/source"},
		ReaddirTimeout: 30,
	}

	mockStoreConf := &chunk.StoreConfig{
		BlockSize: 4 << 20, // 4MiB
		CacheSize: 1 << 30,
		// genStoreConf always leaves the burst cache size settled, and it is the
		// cap the share-reserve clamp checks. 1GiB, so the clamp passes with the
		// API server enabled.
		BurstCacheSize: 1 << 30,
	}

	for _, tt := range []struct {
		name     string
		params   map[string]interface{}
		reuse    bool
		prefault bool
	}{
		{"defaults", nil, true, true},
		{"reuse disabled", map[string]interface{}{"buffer-reuse": false}, false, true},
		{"prefault disabled", map[string]interface{}{"buffer-prefault": false}, true, false},
		{"both disabled", map[string]interface{}{"buffer-reuse": false, "buffer-prefault": false}, false, false},
	} {
		t.Run("block pool "+tt.name, func(t *testing.T) {
			cfg := genConf(mockCli(tt.params), mockMetaConf, mockStoreConf)
			assert.Equal(t, tt.reuse, cfg.BufferReuse)
			assert.Equal(t, tt.prefault, cfg.BufferPrefault)
		})
	}

	tests := []struct {
		name      string
		cliParams map[string]interface{}
		metaConf  *meta.MetaConfig
		storeConf *chunk.StoreConfig
		wantCfg   *vfs.Config
	}{
		{
			name:      "default config",
			cliParams: map[string]interface{}{},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				Prefetch:        3,
				PrefetchWorkers: 3,
				ShareReserve:    0, // API server disabled, share-reserve not computed
				AttrTimeout:     30 * time.Second,
				EntryTimeout:    30 * time.Second,
				DirEntryTimeout: 30 * time.Second,
				NegativeTimeout: 30 * time.Second,
			},
		},
		{
			name: "default share reserve with api server enabled",
			cliParams: map[string]interface{}{
				"enable-api-server": true,
				"api-server-sock":   "/tmp/api.sock",
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				ApiSocket:    "/tmp/api.sock",
				ShareReserve: 3 * (4 << 20), // prefetch x block size
			},
		},
		{
			name: "custom share reserve",
			cliParams: map[string]interface{}{
				"enable-api-server": true,
				"api-server-sock":   "/tmp/api.sock",
				"share-reserve":     500,
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				ApiSocket:    "/tmp/api.sock",
				ShareReserve: 500 << 20,
			},
		},
		{
			name: "share reserve explicitly zero means no reserve",
			cliParams: map[string]interface{}{
				"enable-api-server": true,
				"api-server-sock":   "/tmp/api.sock",
				"share-reserve":     0,
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				ApiSocket:    "/tmp/api.sock",
				ShareReserve: 0, // explicitly 0, not the prefetch x block size default
			},
		},
		{
			name: "default share reserve exceeds small burst cache, reset to 0",
			cliParams: map[string]interface{}{
				"enable-api-server": true,
				"api-server-sock":   "/tmp/api.sock",
			},
			metaConf: mockMetaConf,
			storeConf: &chunk.StoreConfig{
				BlockSize:      4 << 20, // 4MiB
				CacheSize:      8 << 20, // 8MiB — smaller than default reserve (3×4=12MiB)
				BurstCacheSize: 8 << 20, // no burst cache, so the cap in effect is the cache size
			},
			wantCfg: &vfs.Config{
				ApiSocket:    "/tmp/api.sock",
				ShareReserve: 0, // default (12MiB) >= burstCap (8MiB) → reset to 0
			},
		},
		{
			name:      "default buffer size leaves the block pool disabled",
			cliParams: map[string]interface{}{},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				BlockPoolSize: 0,
				BufferTimeout: 180,
			},
		},
		{
			name: "custom buffer size",
			cliParams: map[string]interface{}{
				"buffer-size": 500,
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				BlockPoolSize: 500 << 20,
			},
		},
		{
			name: "buffer size -1 clamped to zero",
			cliParams: map[string]interface{}{
				"buffer-size": -1,
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				BlockPoolSize: 0,
			},
		},
		{
			name: "custom buffer timeout",
			cliParams: map[string]interface{}{
				"buffer-timeout": 60,
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				BufferTimeout: 60,
			},
		},
		{
			name:      "no timeout",
			cliParams: map[string]interface{}{},
			metaConf: &meta.MetaConfig{
				ReaddirTimeout: -1,
			},
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				AttrTimeout:     fuse.DefTimeout * time.Second,
				EntryTimeout:    fuse.DefTimeout * time.Second,
				DirEntryTimeout: fuse.DefTimeout * time.Second,
				NegativeTimeout: fuse.DefTimeout * time.Second,
			},
		},
		{
			name: "custom timeouts",
			cliParams: map[string]interface{}{
				"negative-timeout": 10,
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				NegativeTimeout: 10 * time.Second,
			},
		},
		{
			name: "negative timeout clamped to default",
			cliParams: map[string]interface{}{
				"negative-timeout": -1,
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				NegativeTimeout: fuse.DefTimeout * time.Second,
			},
		},
		{
			name: "explicit negative timeout overrides readdir timeout",
			cliParams: map[string]interface{}{
				"negative-timeout": 5,
			},
			metaConf: &meta.MetaConfig{
				ReaddirTimeout: 30,
			},
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				AttrTimeout:     30 * time.Second,
				EntryTimeout:    30 * time.Second,
				DirEntryTimeout: 30 * time.Second,
				NegativeTimeout: 5 * time.Second,
			},
		},
		{
			name:      "readdir timeout sets all timeouts",
			cliParams: map[string]interface{}{},
			metaConf: &meta.MetaConfig{
				ReaddirTimeout: 15,
			},
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				AttrTimeout:     15 * time.Second,
				EntryTimeout:    15 * time.Second,
				DirEntryTimeout: 15 * time.Second,
				NegativeTimeout: 15 * time.Second,
			},
		},
		{
			name: "prefetch workers set",
			cliParams: map[string]interface{}{
				"prefetch-workers": 5,
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				PrefetchWorkers: 5,
			},
		},
		{
			name: "enable api server",
			cliParams: map[string]interface{}{
				"enable-api-server": true,
				"api-server-sock":   "/tmp/api.sock",
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				ApiSocket: "/tmp/api.sock",
			},
		},
		{
			name: "disable api server",
			cliParams: map[string]interface{}{
				"enable-api-server": false,
				"api-server-sock":   "/tmp/api.sock",
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				ApiSocket: "",
			},
		},
		{
			name: "sourceless meta provider mode with preload",
			cliParams: map[string]interface{}{
				"preload":    "/path1 /path2",
				"sourceless": true,
			},
			metaConf: &meta.MetaConfig{
				Sourceless: true,
				SourceDirs: []string{"/source"},
			},
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				Preload: nil, // Should be disabled in sourceless mode
			},
		},
		{
			name: "force-preload enabled",
			cliParams: map[string]interface{}{
				"force-preload": true,
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				ForcePreload: true,
			},
		},
		{
			name:      "force-preload default false",
			cliParams: map[string]interface{}{},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				ForcePreload: false,
			},
		},
		{
			name: "pin cache static paths",
			cliParams: map[string]interface{}{
				"pin-cache": "/models/llama/ /data/embeddings/",
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				PinPath: vfs.PinPathConfig{
					Paths: "/models/llama/ /data/embeddings/",
				},
			},
		},
		{
			name: "pin cache file with reload",
			cliParams: map[string]interface{}{
				"pin-cache-file":   "/etc/pin-paths.txt",
				"pin-cache-reload": 60,
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				PinPath: vfs.PinPathConfig{
					File:   "/etc/pin-paths.txt",
					Reload: 60,
				},
			},
		},
		{
			name: "pin cache file overrides static paths",
			cliParams: map[string]interface{}{
				"pin-cache":      "/static/path/",
				"pin-cache-file": "/etc/pin-paths.txt",
			},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				PinPath: vfs.PinPathConfig{
					Paths: "/static/path/",
					File:  "/etc/pin-paths.txt",
				},
			},
		},
		{
			name:      "pin cache default empty",
			cliParams: map[string]interface{}{},
			metaConf:  mockMetaConf,
			storeConf: mockStoreConf,
			wantCfg: &vfs.Config{
				PinPath: vfs.PinPathConfig{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := mockCli(tt.cliParams)
			cfg := genConf(c, tt.metaConf, tt.storeConf)

			if tt.wantCfg.ShareReserve != 0 && cfg.ShareReserve != tt.wantCfg.ShareReserve {
				t.Errorf("ShareReserve: got %d want %d", cfg.ShareReserve, tt.wantCfg.ShareReserve)
			}

			// Checked unconditionally: 0 (pool disabled) is the default every other
			// case expects, and is exactly the value worth pinning down.
			if cfg.BlockPoolSize != tt.wantCfg.BlockPoolSize {
				t.Errorf("BlockPoolSize: got %d want %d", cfg.BlockPoolSize, tt.wantCfg.BlockPoolSize)
			}

			if tt.wantCfg.BufferTimeout != 0 && cfg.BufferTimeout != tt.wantCfg.BufferTimeout {
				t.Errorf("BufferTimeout: got %d want %d", cfg.BufferTimeout, tt.wantCfg.BufferTimeout)
			}

			if tt.wantCfg.Prefetch != 0 && cfg.Prefetch != tt.wantCfg.Prefetch {
				t.Errorf("Prefetch: got %d want %d", cfg.Prefetch, tt.wantCfg.Prefetch)
			}

			if tt.wantCfg.PrefetchWorkers != 0 && cfg.PrefetchWorkers != tt.wantCfg.PrefetchWorkers {
				t.Errorf("PrefetchWorkers: got %d want %d", cfg.PrefetchWorkers, tt.wantCfg.PrefetchWorkers)
			}

			if tt.wantCfg.AttrTimeout != 0 && cfg.AttrTimeout != tt.wantCfg.AttrTimeout {
				t.Errorf("AttrTimeout: got %v want %v", cfg.AttrTimeout, tt.wantCfg.AttrTimeout)
			}

			if tt.wantCfg.EntryTimeout != 0 && cfg.EntryTimeout != tt.wantCfg.EntryTimeout {
				t.Errorf("EntryTimeout: got %v want %v", cfg.EntryTimeout, tt.wantCfg.EntryTimeout)
			}

			if tt.wantCfg.DirEntryTimeout != 0 && cfg.DirEntryTimeout != tt.wantCfg.DirEntryTimeout {
				t.Errorf("DirEntryTimeout: got %v want %v", cfg.DirEntryTimeout, tt.wantCfg.DirEntryTimeout)
			}

			if tt.wantCfg.NegativeTimeout != 0 && cfg.NegativeTimeout != tt.wantCfg.NegativeTimeout {
				t.Errorf("NegativeTimeout: got %v want %v", cfg.NegativeTimeout, tt.wantCfg.NegativeTimeout)
			}

			if tt.wantCfg.ApiSocket != "" && cfg.ApiSocket != tt.wantCfg.ApiSocket {
				t.Errorf("ApiSocket: got %s want %s", cfg.ApiSocket, tt.wantCfg.ApiSocket)
			}

			if tt.wantCfg.ApiSocket == "" && !c.Bool("enable-api-server") {
				if cfg.ApiSocket != "" {
					t.Errorf("ApiSocket should be empty when api server is disabled, got %s", cfg.ApiSocket)
				}
			}

			if tt.wantCfg.ForcePreload != cfg.ForcePreload {
				t.Errorf("ForcePreload: got %v want %v", cfg.ForcePreload, tt.wantCfg.ForcePreload)
			}

			if tt.wantCfg.PinPath.Paths != "" && cfg.PinPath.Paths != tt.wantCfg.PinPath.Paths {
				t.Errorf("PinPath.Paths: got %q want %q", cfg.PinPath.Paths, tt.wantCfg.PinPath.Paths)
			}
			if tt.wantCfg.PinPath.File != "" && cfg.PinPath.File != tt.wantCfg.PinPath.File {
				t.Errorf("PinPath.File: got %q want %q", cfg.PinPath.File, tt.wantCfg.PinPath.File)
			}
			if tt.wantCfg.PinPath.Reload != 0 && cfg.PinPath.Reload != tt.wantCfg.PinPath.Reload {
				t.Errorf("PinPath.Reload: got %d want %d", cfg.PinPath.Reload, tt.wantCfg.PinPath.Reload)
			}

			// Check special cases
			if tt.name == "sourceless meta provider mode with preload" {
				if len(cfg.Preload) != 0 {
					t.Errorf("Preload should be empty in sourceless mode, got %v", cfg.Preload)
				}
			}
			if tt.name == "pin cache default empty" {
				if cfg.PinPath.Paths != "" || cfg.PinPath.File != "" || cfg.PinPath.Reload != 0 {
					t.Errorf("PinPath should be empty by default, got %+v", cfg.PinPath)
				}
			}
			if tt.name == "share reserve explicitly zero means no reserve" {
				if cfg.ShareReserve != 0 {
					t.Errorf("ShareReserve: got %d want 0 (no reserve)", cfg.ShareReserve)
				}
			}
			if tt.name == "default share reserve exceeds small burst cache, reset to 0" {
				if cfg.ShareReserve != 0 {
					t.Errorf("ShareReserve: got %d want 0 (default exceeded burst cache, reset)", cfg.ShareReserve)
				}
			}
		})
	}
}

// TestGenConfNegativeBufferTimeout checks a negative --buffer-timeout is
// clamped, so it cannot reach the pool as a negative duration and silently mean
// "never reap". It stands apart from the table above, whose BufferTimeout check
// skips a zero expectation - and zero is the whole point here.
func TestGenConfNegativeBufferTimeout(t *testing.T) {
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	set.Int("buffer-timeout", 180, "")
	if err := set.Set("buffer-timeout", "-1"); err != nil {
		t.Errorf("set buffer-timeout: %s", err)
		return
	}
	cfg := genConf(cli.NewContext(nil, set, nil),
		&meta.MetaConfig{Name: "testfs", MountPoint: "/mnt/test", SourceDirs: []string{"/source"}, ReaddirTimeout: 30},
		&chunk.StoreConfig{BlockSize: 4 << 20})
	assert.Equal(t, 0, cfg.BufferTimeout)
}

func TestValidateCacheConfig(t *testing.T) {
	tests := []struct {
		name        string
		metaConf    *meta.MetaConfig
		cachefsConf *vfs.Config
		wantErr     bool
		errContains string
	}{
		{
			name:        "valid standalone with peermeta",
			metaConf:    &meta.MetaConfig{PeerMetaTimeout: 30},
			cachefsConf: &vfs.Config{},
			wantErr:     false,
		},
		{
			name:        "valid sourceless without daemon socket",
			metaConf:    &meta.MetaConfig{Sourceless: true},
			cachefsConf: &vfs.Config{},
			wantErr:     false,
		},
		{
			name:        "multiple source dirs with sourceless",
			metaConf:    &meta.MetaConfig{Sourceless: true, SourceDirs: []string{"/a", "/b"}},
			cachefsConf: &vfs.Config{},
			wantErr:     true,
			errContains: "multiple source dirs",
		},
		{
			name:        "multiple source dirs standalone",
			metaConf:    &meta.MetaConfig{SourceDirs: []string{"/a", "/b"}},
			cachefsConf: &vfs.Config{},
			wantErr:     false,
		},
		{
			name:        "sourceless with daemon socket",
			metaConf:    &meta.MetaConfig{Sourceless: true},
			cachefsConf: &vfs.Config{DaemonSocket: "/var/run/cachefsd.sock"},
			wantErr:     true,
			errContains: "Fuse/Store separation",
		},
		{
			name:        "peermeta with daemon socket",
			metaConf:    &meta.MetaConfig{PeerMetaTimeout: 30},
			cachefsConf: &vfs.Config{DaemonSocket: "/var/run/cachefsd.sock"},
			wantErr:     true,
			errContains: "Fuse/Store separation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCacheConfig(tt.metaConf, tt.cachefsConf)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got nil")
					return
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestProcessSourceDirs(t *testing.T) {
	newCli := func(sourceDir, extraDirs string, sourceless bool) *cli.Context {
		set := flag.NewFlagSet("test", flag.ContinueOnError)
		set.String("source-dir", "", "")
		set.String("extra-source-dirs", "", "")
		set.Bool("sourceless", false, "")
		c := cli.NewContext(nil, set, nil)
		if err := c.Set("source-dir", sourceDir); err != nil {
			t.Error(err)
			return c
		}
		if err := c.Set("extra-source-dirs", extraDirs); err != nil {
			t.Error(err)
			return c
		}
		if err := c.Set("sourceless", fmt.Sprintf("%v", sourceless)); err != nil {
			t.Error(err)
			return c
		}
		return c
	}
	resolved := func(dir string) string {
		ad, err := filepath.EvalSymlinks(dir)
		if err != nil {
			t.Error(err)
			return dir
		}
		return ad
	}

	dirA := t.TempDir()
	dirB := t.TempDir()
	dirC := t.TempDir()
	spaceDir := filepath.Join(dirA, "with space")
	commaDir := filepath.Join(dirA, "with,comma")
	nestedDir := filepath.Join(dirA, "nested")
	assert.NoError(t, os.Mkdir(spaceDir, 0755))
	assert.NoError(t, os.Mkdir(commaDir, 0755))
	assert.NoError(t, os.Mkdir(nestedDir, 0755))
	fileA := filepath.Join(dirA, "file")
	assert.NoError(t, os.WriteFile(fileA, []byte("x"), 0644))

	tests := []struct {
		name        string
		sourceDir   string
		extraDirs   string
		sourceless  bool
		want        []string
		errContains string
	}{
		{"single source dir", dirA, "", false, []string{resolved(dirA)}, ""},
		{"source dir with space taken literally", spaceDir, "", false, []string{resolved(spaceDir)}, ""},
		{"source dir with comma taken literally", commaDir, "", false, []string{resolved(commaDir)}, ""},
		{"extra space separated keeps order", dirA, dirB + " " + dirC, false, []string{resolved(dirA), resolved(dirB), resolved(dirC)}, ""},
		{"extra comma separated keeps order", dirA, dirC + "," + dirB, false, []string{resolved(dirA), resolved(dirC), resolved(dirB)}, ""},
		{"extra mixed separators", dirA, dirB + ", " + dirC, false, []string{resolved(dirA), resolved(dirB), resolved(dirC)}, ""},
		{"extra duplicating source dir deduped", dirA, dirB + " " + dirA + "/", false, []string{resolved(dirA), resolved(dirB)}, ""},
		{"duplicate within extra deduped", dirA, dirB + " " + dirB, false, []string{resolved(dirA), resolved(dirB)}, ""},
		{"extra without source dir", "", dirB, false, nil, "--extra-source-dirs requires --source-dir"},
		{"extra without source dir in sourceless", "", dirB, true, nil, "--extra-source-dirs requires --source-dir"},
		{"empty without sourceless", "", "", false, nil, "Missing --source-dir"},
		{"empty with sourceless", "", "", true, nil, ""},
		{"missing source dir", filepath.Join(dirA, "nope"), "", false, nil, "stat source dir"},
		{"not a directory source dir", fileA, "", false, nil, "not a directory"},
		{"missing dir in extra tolerated", dirA, filepath.Join(dirA, "nope"), false, []string{resolved(dirA)}, ""},
		{"not a directory in extra tolerated", dirA, fileA, false, []string{resolved(dirA)}, ""},
		{"all extras missing keeps primary", dirA, filepath.Join(dirA, "nope") + " " + filepath.Join(dirB, "nope"), false, []string{resolved(dirA)}, ""},
		{"nested source dir in extra rejected", dirA, nestedDir, false, nil, "parent/child relationship"},
		{"nested source dir in reverse order rejected", nestedDir, dirA, false, nil, "parent/child relationship"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := processSourceDirs(newCli(tt.sourceDir, tt.extraDirs, tt.sourceless))
			if tt.errContains != "" {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errContains)
					return
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// The fallback replica list must be injectable via environment, like
// CFS_DEFER_BURST and friends.
func TestExtraSourceDirsEnvVar(t *testing.T) {
	for _, f := range cacheFlags() {
		sf, ok := f.(*cli.StringFlag)
		if !ok || sf.Name != "extra-source-dirs" {
			continue
		}
		assert.Equal(t, []string{"CFS_EXTRA_SOURCE_DIRS"}, sf.EnvVars)
		return
	}
	t.Error("extra-source-dirs flag not found in cacheFlags")
}
