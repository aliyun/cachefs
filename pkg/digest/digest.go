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

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"strings"
)

func NewHasher(name string) (hash.Hash, error) {
	name = strings.ToLower(name)
	switch name {
	case "crc32":
		return crc32.NewIEEE(), nil
	case "md5":
		return md5.New(), nil
	default:
		return nil, fmt.Errorf("unsupported hash algorithm: %s", name)
	}
}

func GetDigest(name string, data []byte) (string, error) {
	hasher, err := NewHasher(name)
	if err != nil {
		return "", err
	}
	_, _ = hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func CheckDigest(name, want string, data []byte) error {
	digest, err := GetDigest(name, data)
	if err != nil {
		return err
	}
	if digest != want {
		return fmt.Errorf("digest mismatch, want %s got %s", want, digest)
	}
	return nil
}
