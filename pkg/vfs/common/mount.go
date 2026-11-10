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

package common

import (
	"fmt"
	"path/filepath"

	"github.com/aliyun/cachefs/pkg/meta"
	"github.com/aliyun/cachefs/pkg/utils"
)

// FindMountPoint finds the CacheFS mount point for the given path by traversing
// up the directory tree until finding a directory with meta.RootInode.
func FindMountPoint(path string) (string, error) {
	var inode uint64
	var err error
	mp := path
	for ; mp != "/"; mp = filepath.Dir(mp) {
		inode, err = utils.GetFileInode(mp)
		if err != nil {
			return "", fmt.Errorf("lookup inode for %s: %s", mp, err)
		}
		if inode == uint64(meta.RootInode) {
			return mp, nil
		}
	}
	return "", fmt.Errorf("mount point inode is %d, expected %d for CacheFS",
		inode, meta.RootInode)
}

// ValidateMountPoint checks whether the given file path resides in a CacheFS
// mount point by verifying that the .cachefs special file has the expected inode.
func ValidateMountPoint(filePath string) error {
	mountPoint, err := FindMountPoint(filePath)
	if err != nil {
		return fmt.Errorf("failed to find cachefs mount point: %v", err)
	}

	cfsPath := filepath.Join(mountPoint, ".cachefs")
	cfsInode, err := utils.GetFileInode(cfsPath)
	if err != nil {
		return err
	}
	if cfsInode != uint64(CachefsInode) {
		return fmt.Errorf("expect .cachefs inode %d for CacheFS, got %d",
			uint64(CachefsInode), cfsInode)
	}
	return nil
}
