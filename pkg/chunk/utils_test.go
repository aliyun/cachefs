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

package chunk

import (
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetNlink(t *testing.T) {
	file, err := os.CreateTemp("", "testfile")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	fi, err := file.Stat()
	require.NoError(t, err)

	nlink := getNlink(fi)
	assert.Equal(t, 1, nlink, "The number of links for a new file should be 1")
}

func TestGetDiskUsage(t *testing.T) {
	total, avail, files, free := getDiskUsage("/")

	assert.Greater(t, total, uint64(0), "Total blocks should be greater than 0")
	assert.Greater(t, avail, uint64(0), "Available blocks should be greater than 0")
	assert.Greater(t, files, uint64(0), "Total files should be greater than 0")
	assert.Greater(t, free, uint64(0), "Free files should be greater than 0")
}

func TestChangeMode(t *testing.T) {
	dir := t.TempDir()

	file, err := os.CreateTemp(dir, "testfile")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	fi, err := file.Stat()
	require.NoError(t, err)

	initialMode := fi.Mode()
	newMode := initialMode ^ 0777

	changeMode(file.Name(), fi, newMode)

	updatedFi, err := os.Stat(file.Name())
	require.NoError(t, err)
	assert.Equal(t, newMode.Perm(), updatedFi.Mode().Perm(), "File mode should be updated")
}

func changeMode(dir string, st os.FileInfo, mode os.FileMode) {
	sst := st.Sys().(*syscall.Stat_t)
	if os.Getuid() == int(sst.Uid) {
		_ = os.Chmod(dir, mode)
	}
}
