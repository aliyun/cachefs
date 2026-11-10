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

// Package common provides shared constants and utilities used by both the vfs
// package and its subpackages (e.g. apiclient). It has no CGO dependencies.
package common

const (
	// MinInternalNode is the base inode number for VFS-internal virtual files.
	MinInternalNode = 0x7FFFFFFF00000000

	// LogInode is the inode of the .accesslog virtual file.
	LogInode = MinInternalNode + 1
	// ControlInode is the inode of the .control virtual file.
	ControlInode = MinInternalNode + 2
	// StatsInode is the inode of the .stats virtual file.
	StatsInode = MinInternalNode + 3
	// ConfigInode is the inode of the .config virtual file.
	ConfigInode = MinInternalNode + 4

	// CachefsInode is the inode of the .cachefs virtual file. This value is
	// part of the CacheFS VFS protocol and is stable across versions.
	CachefsInode = MinInternalNode + 100
)
