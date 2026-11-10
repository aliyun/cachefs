/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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
	"io"
	"testing"

	"github.com/aliyun/cachefs/pkg/utils"
	"golang.org/x/sys/unix"
)

func TestPage(t *testing.T) {
	p1, err := AllocPage(1)
	if err != nil {
		t.Errorf("AllocPage: %s", err)
		return
	}
	if len(p1.Data) != 1 {
		t.Fail()
	}
	if cap(p1.Data) != 1 {
		t.Fail()
	}
	p1.Acquire()
	p1.Release()
	if p1.Data == nil {
		t.Fail()
	}

	p2 := p1.Slice(0, 1)
	p1.Release()
	if p1.Data == nil {
		t.Fail()
	}

	p2.Release()
	if p2.Data != nil {
		t.Fail()
	}
	if p1.Data != nil {
		t.Fail()
	}
}

func TestPageReader(t *testing.T) {
	data := []byte("hello")
	p := NewPage(data)
	r := NewPageReader(p)

	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Errorf("read should return 0")
		return
	}
	buf := make([]byte, 3)
	if n, err := r.Read(buf); n != 3 || err != nil {
		t.Errorf("read should return 3 but got %d", n)
		return
	}
	if n, err := r.Read(buf); n != 2 || (err != nil && err != io.EOF) {
		t.Errorf("read should return 2 but got %d", n)
		return
	}
	if n, err := r.Read(buf); n != 0 || err != io.EOF {
		t.Errorf("read should return 0")
		return
	}
	if n, err := r.ReadAt(buf, 4); n != 1 || (err != nil && err != io.EOF) {
		t.Errorf("read should return 1")
		return
	}
	if n, err := r.ReadAt(buf, 5); n != 0 || err != io.EOF {
		t.Errorf("read should return 0")
		return
	}
	_ = r.Close()
	if n, err := r.ReadAt(buf, 5); n != 0 || err == nil {
		t.Errorf("read should fail after close")
		return
	}
}

func TestPageGenDigest(t *testing.T) {
	// Create a page with some test data
	testData := []byte("hello world")
	p := NewPage(testData)

	// Initially Digest and Hasher should be empty
	if p.Digest != "" {
		t.Errorf("Expected empty digest initially, got %s", p.Digest)
	}
	if p.Hasher != "" {
		t.Errorf("Expected empty hasher initially, got %s", p.Hasher)
	}

	// Calculate CRC32 digest
	digest, err := p.GenDigest("crc32")
	if err != nil {
		t.Errorf("GenDigest calculation failed: %v", err)
		return
	}

	// Check that digest and hasher are set correctly
	if digest == "" {
		t.Error("Returned digest should not be empty")
	}
	if p.Digest == "" {
		t.Error("Page Digest should not be empty after calculation")
	}
	if p.Hasher != "crc32" {
		t.Errorf("Expected hasher to be 'crc32', got %s", p.Hasher)
	}

	// Store the calculated values
	firstDigest := p.Digest
	firstHasher := p.Hasher

	// Calling GenDigest again with the same hasher should not recalculate and return previous
	// calculation
	digest, err = p.GenDigest("crc32")
	if err != nil {
		t.Errorf("Second GenDigest calculation failed: %v", err)
		return
	}

	// Values should remain the same as the first calculation
	if digest != firstDigest {
		t.Error("GenDigest should return the same digest on subsequent calls")
	}
	if p.Digest != firstDigest {
		t.Error("Digest should not change on subsequent calls")
	}
	if p.Hasher != firstHasher {
		t.Error("Hasher should not change on subsequent calls")
	}

	// GenDigest again with different hasher should fail, but page hasher and digest should
	// remain the same as the first calculation
	digest, err = p.GenDigest("md5")
	if err == nil {
		t.Errorf("Second GenDigest calculation with different hasher succeeded")
	}
	if digest != "" {
		t.Errorf("GenDigest with different hasher should return empty string on subsequent calls, but got %s", digest)
	}
	if p.Digest != firstDigest {
		t.Error("Digest should not change on subsequent calls")
	}
	if p.Hasher != firstHasher {
		t.Error("Hasher should not change on subsequent calls")
	}

	// Test with unsupported hasher
	p = NewPage(testData)
	_, err = p.GenDigest("unsupported")
	if err == nil {
		t.Error("Expected error for unsupported hasher")
	}

	// Test with MD5 on a new page
	p2 := NewPage(testData)
	digest, err = p2.GenDigest("md5")
	if err != nil {
		t.Errorf("MD5 GenDigest calculation failed: %v", err)
		return
	}
	if digest == "" {
		t.Error("MD5 digest should not be empty")
	}
	if p2.Digest == "" {
		t.Error("Page Digest should not be empty after MD5 calculation")
	}
	if p2.Hasher != "md5" {
		t.Errorf("Expected hasher to be 'md5', got %s", p2.Hasher)
	}

	// Test with empty page data
	p3 := NewPage([]byte{})
	digest, err = p3.GenDigest("crc32")
	if err != nil {
		t.Errorf("GenDigest calculation for empty data failed: %v", err)
		return
	}
	if digest == "" {
		t.Error("Digest for empty data should not be empty")
	}
	if p3.Digest == "" {
		t.Error("Page Digest should not be empty after calculation with empty data")
	}
	if p3.Hasher != "crc32" {
		t.Errorf("Expected hasher to be 'crc32', got %s", p3.Hasher)
	}
}

func TestPageReaderExportFd(t *testing.T) {
	// Test case 1: Page with nil Memfd, ExportFd() should fail
	t.Run("NilMemfd", func(t *testing.T) {
		data := []byte("test data")
		p := NewPage(data) // NewPage does not set Memfd
		r := NewPageReader(p)

		if _, _, err := r.ExportFd(); err == nil {
			t.Errorf("Expected ExportFd() to fail when Memfd is nil")
		}

		_ = r.Close()
	})

	// Test case 2: Page with non-nil Memfd, ExportFd() should export a dup and
	// report the Memfd that now counts it
	t.Run("ValidMemfd", func(t *testing.T) {
		memfd, err := utils.MemfdAlloc("test", 4096)
		if err != nil {
			t.Errorf("MemfdAlloc: %s", err)
			return
		}
		p := NewPage(memfd.Data())
		p.Memfd = memfd

		r := NewPageReader(p)
		defer func() {
			_ = r.Close()
			if err := memfd.Free(); err != nil {
				t.Errorf("Free: %s", err)
			}
		}()
		fd, m, err := r.ExportFd()
		if err != nil {
			t.Errorf("ExportFd: %s", err)
			return
		}
		if fd < 0 || fd == memfd.Fd {
			t.Errorf("Expected ExportFd() to return a new fd, got %d (original %d)", fd, memfd.Fd)
		}
		if m != memfd {
			t.Errorf("Expected ExportFd() to report the page's Memfd")
		}
		_ = unix.Close(fd)
		m.UnexportFd()
	})

	// Test case 3: page reader with nil page
	t.Run("NilPage", func(t *testing.T) {
		memfd, err := utils.MemfdAlloc("test", 4096)
		if err != nil {
			t.Errorf("MemfdAlloc: %s", err)
			return
		}
		defer func() { _ = memfd.Free() }()
		p := NewPage(memfd.Data())
		p.Memfd = memfd

		r := NewPageReader(p)
		r.p = nil

		if _, _, err := r.ExportFd(); err == nil {
			t.Errorf("Expected ExportFd() to fail when the page is nil")
		}

		_ = r.Close()
	})
}
