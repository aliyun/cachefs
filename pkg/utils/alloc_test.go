/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package utils

import (
	"os"
	"testing"
)

func TestPowerOf2(t *testing.T) {
	tests := []struct {
		input    int
		expected int
		desc     string
	}{
		// Edge cases
		{-1, 0, "negative value returns 0"},
		{0, 0, "zero returns 0"},
		// Power of 2 values
		{1, 0, "1 -> 2^0"},
		{2, 1, "2 -> 2^1"},
		{4, 2, "4 -> 2^2"},
		{8, 3, "8 -> 2^3"},
		{16, 4, "16 -> 2^4"},
		{1024, 10, "1024 -> 2^10"},
		{1 << 20, 20, "1MB -> 2^20"},
		// Non-power of 2 values
		{3, 2, "3 -> 2^2 = 4"},
		{5, 3, "5 -> 2^3 = 8"},
		{7, 3, "7 -> 2^3 = 8"},
		{9, 4, "9 -> 2^4 = 16"},
		{15, 4, "15 -> 2^4 = 16"},
		{1023, 10, "1023 -> 2^10 = 1024"},
		{1025, 11, "1025 -> 2^11 = 2048"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			result := powerOf2(tt.input)
			if result != tt.expected {
				t.Errorf("powerOf2(%d) = %d, expected %d", tt.input, result, tt.expected)
				return
			}
		})
	}
}

func TestPowerOf2Property(t *testing.T) {
	// Property-based test: verify that 2^result >= input for all inputs
	// and 2^(result-1) < input for inputs > 1
	for input := 1; input <= 10000; input++ {
		result := powerOf2(input)
		powerValue := 1 << result
		if powerValue < input {
			t.Errorf("powerOf2(%d) = %d, but 2^%d = %d < %d", input, result, result, powerValue, input)
			return
		}
		if input > 1 && result > 0 {
			prevPower := 1 << (result - 1)
			if prevPower >= input {
				t.Errorf("powerOf2(%d) = %d, but 2^%d = %d >= %d (should be smaller)", input, result, result-1, prevPower, input)
				return
			}
		}
	}
}

func TestAlloc(t *testing.T) {
	old := AllocMemory()
	b := Alloc(10)
	if AllocMemory()-old != 16 {
		t.Errorf("alloc 16 bytes, but got %d", AllocMemory()-old)
		return
	}
	Free(b)
	if AllocMemory()-old != 0 {
		t.Errorf("free all allocated memory, but got %d", AllocMemory()-old)
		return
	}
}

func TestSetupMadvise(t *testing.T) {
	tests := []struct {
		name              string
		thpEnabledContent string
		shmemContent      string
		expectThpMadvise  bool
		expectShmemAdvise bool
	}{
		{
			name:              "both madvise",
			thpEnabledContent: "always [madvise] never",
			shmemContent:      "always [advise] never deny force",
			expectThpMadvise:  true,
			expectShmemAdvise: true,
		},
		{
			name:              "thp always shmem advise",
			thpEnabledContent: "[always] madvise never",
			shmemContent:      "always [advise] never deny force",
			expectThpMadvise:  false,
			expectShmemAdvise: true,
		},
		{
			name:              "thp madvise shmem always",
			thpEnabledContent: "always [madvise] never",
			shmemContent:      "[always] advise never deny force",
			expectThpMadvise:  true,
			expectShmemAdvise: false,
		},
		{
			name:              "both always",
			thpEnabledContent: "[always] madvise never",
			shmemContent:      "[always] advise never deny force",
			expectThpMadvise:  false,
			expectShmemAdvise: false,
		},
		{
			name:              "both never",
			thpEnabledContent: "always madvise [never]",
			shmemContent:      "always advise [never] deny force",
			expectThpMadvise:  false,
			expectShmemAdvise: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create temp dir and files for test
			dir := t.TempDir()
			thpEnabledFile := dir + "/enabled"
			thpShmemFile := dir + "/shmem_enabled"

			// Save original paths and restore after test
			origThpEnabled := thpEnabled
			origThpShmemEnabled := thpShmemEnabled
			defer func() {
				thpEnabled = origThpEnabled
				thpShmemEnabled = origThpShmemEnabled
			}()

			// Set test paths
			thpEnabled = thpEnabledFile
			thpShmemEnabled = thpShmemFile

			// Write test files
			if err := os.WriteFile(thpEnabledFile, []byte(tc.thpEnabledContent), 0644); err != nil {
				t.Errorf("failed to write thp enabled file: %v", err)
				return
			}
			if err := os.WriteFile(thpShmemFile, []byte(tc.shmemContent), 0644); err != nil {
				t.Errorf("failed to write shmem enabled file: %v", err)
				return
			}

			// Reset global vars
			thpMadvise = false
			thpShmemAdvise = false

			// Call SetupMadvise
			SetupMadvise()

			// Verify results
			if thpMadvise != tc.expectThpMadvise {
				t.Errorf("thpMadvise = %v, expect %v", thpMadvise, tc.expectThpMadvise)
				return
			}
			if thpShmemAdvise != tc.expectShmemAdvise {
				t.Errorf("thpShmemAdvise = %v, expect %v", thpShmemAdvise, tc.expectShmemAdvise)
				return
			}
		})
	}

	// Test case when files don't exist
	t.Run("files not exist", func(t *testing.T) {
		// Save original paths
		origThpEnabled := thpEnabled
		origThpShmemEnabled := thpShmemEnabled
		defer func() {
			thpEnabled = origThpEnabled
			thpShmemEnabled = origThpShmemEnabled
		}()

		// Set non-existent paths
		thpEnabled = "/no/such/path/enabled"
		thpShmemEnabled = "/no/such/path/shmem_enabled"

		// Reset global vars
		thpMadvise = false
		thpShmemAdvise = false

		// Call SetupMadvise, should not panic
		SetupMadvise()

		// Both should remain false when files don't exist
		if thpMadvise != false {
			t.Errorf("thpMadvise should be false when file doesn't exist, got %v", thpMadvise)
			return
		}
		if thpShmemAdvise != false {
			t.Errorf("thpShmemAdvise should be false when file doesn't exist, got %v", thpShmemAdvise)
			return
		}
	})
}
