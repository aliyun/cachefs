/*
 * JuiceFS, Copyright 2021 Juicedata, Inc.
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
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aliyun/cachefs/pkg/meta"
)

func openController(mp string) *os.File {
	st, err := os.Stat(mp)
	if err != nil {
		logger.Fatal(err)
	}
	if !st.IsDir() {
		mp = filepath.Dir(mp)
	}
	for ; mp != "/"; mp = filepath.Dir(mp) {
		f, err := os.OpenFile(filepath.Join(mp, ".control"), os.O_RDWR, 0)
		if err == nil {
			return f
		}
		if !os.IsNotExist(err) {
			logger.Fatal(err)
		}
	}
	logger.Fatalf("Path %s is not inside CacheFS", mp)
	panic("unreachable")
}

func readControl(cf *os.File, resp []byte) int {
	for {
		if n, err := cf.Read(resp); err == nil {
			return n
		} else if err == io.EOF {
			time.Sleep(time.Millisecond * 300)
		} else {
			logger.Fatalf("Read message: %d %s", n, err)
		}
	}
}

func readProgress(cf *os.File, showProgress func(uint64, uint64)) (errno syscall.Errno) {
	var resp = make([]byte, 1024)
END:
	for {
		n := readControl(cf, resp)
		for off := 0; off < n; {
			if off+1 == n {
				errno = syscall.Errno(resp[off])
				break END
			} else if off+17 <= n && resp[off] == meta.CPROGRESS {
				showProgress(binary.BigEndian.Uint64(resp[off+1:off+9]), binary.BigEndian.Uint64(resp[off+9:off+17]))
				off += 17
			} else {
				logger.Errorf("Bad response off %d n %d: %v", off, n, resp)
				break
			}
		}
	}
	return
}
