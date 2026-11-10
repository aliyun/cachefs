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
	"os"
	"path/filepath"
	"testing"
)

func TestGetAbsolutePaths(t *testing.T) {
	tests := []struct {
		name      string
		paths     []string
		wantErr   bool
		checkFunc func(t *testing.T, result []string)
	}{
		{
			name:    "convert relative paths to absolute",
			paths:   []string{".", ".."},
			wantErr: false,
			checkFunc: func(t *testing.T, result []string) {
				if len(result) != 2 {
					t.Errorf("expected 2 paths, got %d", len(result))
				}
				for _, p := range result {
					if !filepath.IsAbs(p) {
						t.Errorf("expected absolute path, got %s", p)
					}
				}
			},
		},
		{
			name:    "absolute paths remain unchanged",
			paths:   []string{"/tmp", "/var"},
			wantErr: false,
			checkFunc: func(t *testing.T, result []string) {
				if len(result) != 2 {
					t.Errorf("expected 2 paths, got %d", len(result))
				}
				for i, p := range result {
					if p != []string{"/tmp", "/var"}[i] {
						t.Errorf("expected %s, got %s", []string{"/tmp", "/var"}[i], p)
					}
				}
			},
		},
		{
			name:    "empty input",
			paths:   []string{},
			wantErr: false,
			checkFunc: func(t *testing.T, result []string) {
				if len(result) != 0 {
					t.Errorf("expected empty result, got %d paths", len(result))
				}
			},
		},
		{
			name:    "mixed relative and absolute paths",
			paths:   []string{"/tmp", ".", "/var"},
			wantErr: false,
			checkFunc: func(t *testing.T, result []string) {
				if len(result) != 3 {
					t.Errorf("expected 3 paths, got %d", len(result))
				}
				for _, p := range result {
					if !filepath.IsAbs(p) {
						t.Errorf("expected absolute path, got %s", p)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getAbsolutePaths(tt.paths)
			if (err != nil) != tt.wantErr {
				t.Errorf("getAbsolutePaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.checkFunc != nil {
				tt.checkFunc(t, result)
			}
		})
	}
}

func TestConvertToRelativePaths(t *testing.T) {
	// Note: This function requires a real cachefs mount point with RootInode
	// These tests are designed to verify the logic but will fail without cachefs
	// Skip these tests in CI/CD or run them manually with a real cachefs mount

	if os.Getenv("CFS_MOUNT_POINT") == "" {
		t.Skip("Skipping test that requires CFS_MOUNT_POINT environment variable")
	}

	cachefsMountPoint := os.Getenv("CFS_MOUNT_POINT")
	subDir := filepath.Join(cachefsMountPoint, "testdir")

	// Create test directory if it doesn't exist
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Logf("Warning: failed to create test directory: %v", err)
	}

	tests := []struct {
		name            string
		absPaths        []string
		wantErr         bool
		expectedRelPath []string
	}{
		{
			name:            "mount point itself",
			absPaths:        []string{cachefsMountPoint},
			wantErr:         false,
			expectedRelPath: []string{"/"},
		},
		{
			name:            "subdirectory under mount point",
			absPaths:        []string{subDir},
			wantErr:         false,
			expectedRelPath: []string{"/testdir"},
		},
		{
			name:            "multiple paths",
			absPaths:        []string{cachefsMountPoint, subDir},
			wantErr:         false,
			expectedRelPath: []string{"/", "/testdir"},
		},
		{
			name:     "empty input",
			absPaths: []string{},
			wantErr:  true,
		},
		{
			name:     "path not under mount point",
			absPaths: []string{cachefsMountPoint, "/tmp"},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relPaths, err := convertToRelativePaths(tt.absPaths)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToRelativePaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if len(relPaths) == 0 {
					t.Error("expected non-empty relative paths")
				}
				if len(relPaths) != len(tt.expectedRelPath) {
					t.Errorf("expected %d relative paths, got %d", len(tt.expectedRelPath), len(relPaths))
					return
				}
				for i, expected := range tt.expectedRelPath {
					if relPaths[i] != expected {
						t.Errorf("expected relative path[%d] = %s, got %s", i, expected, relPaths[i])
					}
				}
			}
		})
	}
}

func TestConvertToRelativePathsEdgeCases(t *testing.T) {
	// These tests verify error handling logic
	tests := []struct {
		name     string
		absPaths []string
		wantErr  bool
	}{
		{
			name:     "empty input",
			absPaths: []string{},
			wantErr:  true,
		},
		{
			name:     "non-existent path",
			absPaths: []string{"/nonexistent/path/that/does/not/exist/foo/bar"},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := convertToRelativePaths(tt.absPaths)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToRelativePaths() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Integration test for the full workflow
// This test uses regular directories and focuses on path conversion logic
// without requiring cachefs mount point
func TestPathConversionWorkflow(t *testing.T) {
	tmpDir := t.TempDir()
	subDir1 := filepath.Join(tmpDir, "dir1")
	subDir2 := filepath.Join(tmpDir, "dir2")

	if err := os.Mkdir(subDir1, 0755); err != nil {
		t.Errorf("failed to create test directory: %v", err)
		return
	}
	if err := os.Mkdir(subDir2, 0755); err != nil {
		t.Errorf("failed to create test directory: %v", err)
		return
	}

	// Test the absolute path conversion part
	relativePaths := []string{
		filepath.Join(".", filepath.Base(tmpDir), "dir1"),
		filepath.Join(".", filepath.Base(tmpDir), "dir2"),
	}

	// Change to parent directory of tmpDir
	originalWd, err := os.Getwd()
	if err != nil {
		t.Errorf("failed to get working directory: %v", err)
		return
	}
	defer os.Chdir(originalWd)

	parentDir := filepath.Dir(tmpDir)
	if err := os.Chdir(parentDir); err != nil {
		t.Errorf("failed to change directory: %v", err)
		return
	}

	// Step 1: Convert to absolute paths
	absPaths, err := getAbsolutePaths(relativePaths)
	if err != nil {
		t.Errorf("getAbsolutePaths() failed: %v", err)
		return
	}

	if len(absPaths) != 2 {
		t.Errorf("expected 2 absolute paths, got %d", len(absPaths))
	}

	for _, p := range absPaths {
		if !filepath.IsAbs(p) {
			t.Errorf("expected absolute path, got %s", p)
		}
	}

	// Verify the paths are correct
	expectedPaths := []string{subDir1, subDir2}
	for i, expected := range expectedPaths {
		if absPaths[i] != expected {
			t.Errorf("expected path[%d] = %s, got %s", i, expected, absPaths[i])
		}
	}
}
