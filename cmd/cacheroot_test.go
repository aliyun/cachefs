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

package cmd

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRatioPct(t *testing.T) {
	tests := []struct {
		name   string
		cached uint64
		total  uint64
		want   float64
	}{
		{"zero total", 0, 0, 0},
		{"nothing cached", 0, 100, 0},
		{"half cached", 50, 100, 50},
		{"fully cached", 100, 100, 100},
		{"three quarters", 3, 4, 75},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ratioPct(tt.cached, tt.total))
		})
	}
}

func TestCachePct(t *testing.T) {
	tests := []struct {
		name   string
		cached uint64
		total  uint64
		want   float64
	}{
		// Empty target (total == 0) is treated as fully cached.
		{"empty target", 0, 0, 100},
		{"nothing cached", 0, 100, 0},
		{"half cached", 50, 100, 50},
		{"fully cached", 100, 100, 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, cachePct(tt.cached, tt.total))
		})
	}
}

func TestRatioRowJSON(t *testing.T) {
	r := ratioRow{
		Path:         "/data/dir",
		Ok:           true,
		TotalBlocks:  10,
		TotalBytes:   1024,
		CachedBlocks: 5,
		CachedBytes:  512,
	}
	data, err := json.Marshal(r)
	assert.NoError(t, err)

	var got map[string]any
	assert.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, "/data/dir", got["path"])
	assert.Equal(t, true, got["ok"])
	assert.Equal(t, float64(10), got["total_blocks"])
	assert.Equal(t, float64(1024), got["total_bytes"])
	assert.Equal(t, float64(5), got["cached_blocks"])
	assert.Equal(t, float64(512), got["cached_bytes"])
	// error is empty and marked omitempty, so it must be absent.
	_, hasErr := got["error"]
	assert.False(t, hasErr)
}

func TestRatioRowJSONWithError(t *testing.T) {
	r := ratioRow{Path: "/missing", Ok: false, Error: "no such file"}
	data, err := json.Marshal(r)
	assert.NoError(t, err)

	var got map[string]any
	assert.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, false, got["ok"])
	assert.Equal(t, "no such file", got["error"])
}

func TestRatioRowJSONNodesOmittedWhenEmpty(t *testing.T) {
	r := ratioRow{Path: "/data/dir", Ok: true, TotalBlocks: 10, CachedBlocks: 5}
	data, err := json.Marshal(r)
	assert.NoError(t, err)

	var got map[string]any
	assert.NoError(t, json.Unmarshal(data, &got))
	// nodes is omitempty, so a non-verbose row must not carry it.
	_, hasNodes := got["nodes"]
	assert.False(t, hasNodes)
}

func TestRatioRowJSONWithNodes(t *testing.T) {
	r := ratioRow{
		Path:         "/data/dir",
		Ok:           true,
		TotalBlocks:  10,
		TotalBytes:   1024,
		CachedBlocks: 8,
		CachedBytes:  800,
		Nodes: []nodeRow{
			{Node: "10.0.0.1:80", CachedBlocks: 5, CachedBytes: 512},
			{Node: "10.0.0.2:80", CachedBlocks: 3, CachedBytes: 288},
		},
	}
	data, err := json.Marshal(r)
	assert.NoError(t, err)

	var got ratioRow
	assert.NoError(t, json.Unmarshal(data, &got))
	require.Len(t, got.Nodes, 2)
	assert.Equal(t, "10.0.0.1:80", got.Nodes[0].Node)
	assert.Equal(t, uint64(5), got.Nodes[0].CachedBlocks)
	assert.Equal(t, uint64(512), got.Nodes[0].CachedBytes)
	assert.Equal(t, "10.0.0.2:80", got.Nodes[1].Node)
	assert.Equal(t, uint64(3), got.Nodes[1].CachedBlocks)
	assert.Equal(t, uint64(288), got.Nodes[1].CachedBytes)
}

func TestStatsRowJSON(t *testing.T) {
	r := statsRow{
		Node:          "10.0.0.1:80",
		Ok:            true,
		Capacity:      2048,
		Used:          1024,
		HashhitBlocks: 3,
		HashhitBytes:  300,
		CachedBlocks:  6,
		CachedBytes:   600,
		PinnedBlocks:  4,
		PinnedBytes:   400,
	}
	data, err := json.Marshal(r)
	assert.NoError(t, err)

	var got map[string]any
	assert.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, "10.0.0.1:80", got["node"])
	assert.Equal(t, true, got["ok"])
	assert.Equal(t, float64(2048), got["capacity"])
	assert.Equal(t, float64(1024), got["used"])
	assert.Equal(t, float64(3), got["hashhit_blocks"])
	assert.Equal(t, float64(300), got["hashhit_bytes"])
	assert.Equal(t, float64(6), got["cached_blocks"])
	assert.Equal(t, float64(600), got["cached_bytes"])
	assert.Equal(t, float64(4), got["pinned_blocks"])
	assert.Equal(t, float64(400), got["pinned_bytes"])
	_, hasErr := got["error"]
	assert.False(t, hasErr)
}
