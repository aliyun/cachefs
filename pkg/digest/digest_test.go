/*
 * Copyright 2024 Alibaba Cloud, Inc. or its affiliates.
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

package digest

import "testing"

func TestNewHasher(t *testing.T) {
	tests := []struct {
		hasher string
		fail   bool
	}{
		{"crc32", false},
		{"md5", false},
		{"test", true},
	}
	for _, tc := range tests {
		_, err := NewHasher(tc.hasher)
		if (err == nil && tc.fail) || (err != nil && !tc.fail) {
			t.Errorf("NewHasher(%s) should fail %v, but got result %v", tc.hasher, tc.fail, err)
		}
	}
}

func TestGetDigest(t *testing.T) {
	tests := []struct {
		hasher string
		data   []byte
		digest string
	}{
		{"crc32", []byte("hello world"), "0d4a1185"},
		{"md5", []byte("hello world"), "5eb63bbbe01eeed093cb22bb8f5acdc3"},
		{"test", []byte("hello world"), ""},
	}
	for _, tc := range tests {
		result, _ := GetDigest(tc.hasher, tc.data)
		if result != tc.digest {
			t.Errorf("GetDigest(\"%s\", \"%s\") expect \"%s\" got \"%s\"", tc.hasher, tc.data, tc.digest, result)
		}
	}
}

func TestCheckDigest(t *testing.T) {
	tests := []struct {
		hasher string
		want   string
		data   []byte
		pass   bool
	}{
		{"crc32", "0d4a1185", []byte("hello world"), true},
		{"crc32", "0d4a1186", []byte("hello world"), false},
		{"md5", "5eb63bbbe01eeed093cb22bb8f5acdc3", []byte("hello world"), true},
		{"md5", "5eb63bbbe01eeed093cb22bb8f5acdc4", []byte("hello world"), false},
		{"test", "", []byte("hello world"), false},
	}
	for _, tc := range tests {
		res := CheckDigest(tc.hasher, tc.want, tc.data)
		if (tc.pass && res != nil) || (!tc.pass && res == nil) {
			t.Errorf("CheckDigest(\"%s\", \"%s\", \"%s\") should pass %v, but got %v", tc.hasher, tc.want, tc.data, tc.pass, res)
		}
	}
}
