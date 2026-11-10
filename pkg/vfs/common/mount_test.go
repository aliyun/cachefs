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

package common

import (
	"strings"
	"testing"
)

func TestFindMountPoint(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name:    "non-existent path",
			path:    "/nonexistent/path/that/does/not/exist",
			wantErr: true,
		},
		{
			name:    "root path - no CacheFS inode",
			path:    "/",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := FindMountPoint(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("FindMountPoint() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateMountPoint(t *testing.T) {
	t.Run("non-existent path returns error", func(t *testing.T) {
		err := ValidateMountPoint("/non/existent/path")
		if err == nil {
			t.Error("expected error for non-existent path")
		}
	})

	t.Run("non-CacheFS mount returns error", func(t *testing.T) {
		tmpDir := t.TempDir()
		err := ValidateMountPoint(tmpDir)
		if err == nil {
			t.Error("expected error for non-CacheFS mount")
		}
		if !strings.Contains(err.Error(), "mount point inode is") &&
			!strings.Contains(err.Error(), "no such file or directory") {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}
