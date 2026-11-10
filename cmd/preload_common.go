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
	"fmt"
	"path/filepath"
	"strings"

	"github.com/aliyun/cachefs/pkg/vfs/common"
)

// convertToRelativePaths converts absolute paths to paths relative to mount point
// Returns relative paths
func convertToRelativePaths(absPaths []string) (relativePaths []string, err error) {
	if len(absPaths) == 0 {
		return nil, fmt.Errorf("no paths provided")
	}

	// Find mount point from first path
	first := absPaths[0]
	mp, err := common.FindMountPoint(first)
	if err != nil {
		return nil, err
	}

	// Convert absolute paths to relative paths (relative to mount point)
	start := len(mp)
	for _, path := range absPaths {
		if strings.HasPrefix(path, mp) {
			// if path is cachefs mountpoint, use / instead
			p := path[start:]
			if p == "" {
				p = "/"
			}
			relativePaths = append(relativePaths, p)
		} else {
			return nil, fmt.Errorf("path %s is not under mount point %s", path, mp)
		}
	}

	if len(relativePaths) == 0 {
		return nil, fmt.Errorf("no valid paths after conversion")
	}

	return relativePaths, nil
}

// getAbsolutePaths converts input paths to absolute paths
func getAbsolutePaths(paths []string) ([]string, error) {
	var absPaths []string
	for _, p := range paths {
		if abs, err := filepath.Abs(p); err == nil {
			absPaths = append(absPaths, abs)
		} else {
			return nil, fmt.Errorf("failed to get absolute path of %s: %s", p, err)
		}
	}
	return absPaths, nil
}
