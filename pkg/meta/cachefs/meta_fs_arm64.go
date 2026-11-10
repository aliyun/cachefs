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

package cachefsmeta

import "syscall"

// makeSysStat builds a *syscall.Stat_t from a MetaFileInfo.
// On arm64, syscall.Stat_t.Nlink is uint32.
func makeSysStat(fi *MetaFileInfo) *syscall.Stat_t {
	return &syscall.Stat_t{
		Ino:   uint64(fi.Ino),
		Nlink: uint32(fi.Attr.Nlink),
		Size:  int64(fi.Attr.Length),
		Mode:  fi.Attr.SMode(),
		Mtim:  syscall.Timespec{Sec: fi.Attr.Mtime, Nsec: int64(fi.Attr.Mtimensec)},
	}
}
