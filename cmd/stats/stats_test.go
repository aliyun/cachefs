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

package stats

import (
	"strings"
	"testing"
)

func TestFormatU64(t *testing.T) {
	w := &StatsWatcher{Colorful: false}

	tests := []struct {
		name     string
		input    float64
		isByte   bool
		dark     bool
		expected string
	}{
		{"ZeroValue", 0.0, false, false, "   0 "},
		{"LessThan10K", 9999.0, false, false, "9999 "},
		{"Exact10K", 10240.0, false, false, "  10K"},
		{"KilobyteMax", (1024.0*10000 - 1), false, false, "9999K"},
		{"MegabyteStart", 1024.0 * 1024.0 * 1, false, false, "1024K"},
		{"MegabyteMax", 1024.0 * 1024.0 * 9999.0, false, false, "9999M"},
		{"TerabyteStart", 1024.0 * 1024.0 * 1024.0 * 1024.0, false, false, "1024G"},
		{"Petabyte", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1.5, false, false, "1536T"},
		{"ByteUnitFlag", 9999.0, true, false, "9999B"},
		{"KilobyteByte", 1024.0 * 5, true, false, "5120B"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := w.formatU64(tt.input, tt.dark, tt.isByte)
			if got != tt.expected {
				t.Errorf("formatU64(%f, %t) got = %q, want %q", tt.input, tt.isByte, got, tt.expected)
			}
		})
	}
}

func TestFormatTime(t *testing.T) {
	w := &StatsWatcher{Colorful: false}

	tests := []struct {
		name     string
		input    float64
		dark     bool
		expected string
	}{
		{"ZeroValue", 0.0, false, "   0 "},
		{"Less_than_10_OneDigit", 5.12, false, "5.12 "},
		{"Less_than_10_Max", 9.99, false, "9.99 "},
		{"Start_at_10", 10.0, false, "10.0 "},
		{"Less_than_100_Max", 99.9, false, "99.9 "},
		{"Start_at_100", 100.0, false, " 100 "},
		{"Less_than_10k_Max", 9999.0, false, "9999 "},
		{"Start_at_10k", 10000.0, false, "1e+04"},
		{"LargeValue", 512345.67, false, "5e+05"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := w.formatTime(tt.input, tt.dark)
			if tt.name == "Start_at_10k" || tt.name == "LargeValue" {
				if !strings.Contains(got, "e+") {
					t.Errorf("formatTime(%f) expected scientific notation, got %q", tt.input, got)
				}
				return
			}
			if got != tt.expected {
				t.Errorf("formatTime(%f) got = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestFormatCPU(t *testing.T) {
	w := &StatsWatcher{Colorful: false}

	tests := []struct {
		name     string
		input    float64
		dark     bool
		expected string
	}{
		{"ZeroValue", 0.0, false, " 0.0%"},
		{"Below30", 0.299, false, "29.9%"},
		{"At30Boundary", 0.30, false, "30.0%"},
		{"Below100", 0.999, false, "99.9%"},
		{"At100Boundary", 1.00, false, " 100%"},
		{"Over100", 1.55, false, " 155%"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := w.formatCPU(tt.input, tt.dark)
			if got != tt.expected {
				t.Errorf("formatCPU(%f) got = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestBuildCacheSchemaPrefault(t *testing.T) {
	tests := []struct {
		name      string
		prefault  bool
		verbosity uint
		expected  []string
	}{
		{
			name:     "disabled",
			prefault: false,
			expected: []string{
				"cachefs_block_pool_bytes",
				"cachefs_block_pool_reuse_bytes",
				"cachefs_block_pool_realloc_bytes",
			},
		},
		{
			name:     "enabled",
			prefault: true,
			expected: []string{
				"cachefs_block_pool_bytes",
				"cachefs_block_pool_reuse_bytes",
				"cachefs_block_pool_realloc_bytes",
				"cachefs_block_pool_prefault_bytes",
			},
		},
		{
			name:      "enabled verbosity 2",
			prefault:  true,
			verbosity: 2,
			expected: []string{
				"cachefs_block_pool_bytes",
				"cachefs_block_pool_reuse_bytes",
				"cachefs_block_pool_reuse",
				"cachefs_block_pool_realloc_bytes",
				"cachefs_block_pool_realloc",
				"cachefs_block_pool_prefault_bytes",
				"cachefs_block_pool_prefault_discard_bytes",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &StatsWatcher{Prefault: tt.prefault}
			w.BuildCacheSchema("p", tt.verbosity)

			var names []string
			for _, item := range w.sections[0].items {
				names = append(names, item.name)
			}
			if got := strings.Join(names, ","); got != strings.Join(tt.expected, ",") {
				t.Errorf("block-pool metrics = %s, expected %s", got, strings.Join(tt.expected, ","))
			}
		})
	}
}
