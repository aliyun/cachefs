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

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompactRanges(t *testing.T) {
	tests := []struct {
		name   string
		blocks []uint32
		want   string
	}{
		{"empty", nil, ""},
		{"single", []uint32{5}, "5"},
		{"one run", []uint32{0, 1, 2, 3}, "0-3"},
		{"gaps and runs", []uint32{0, 1, 2, 3, 5, 7, 8}, "0-3,5,7-8"},
		{"all isolated", []uint32{1, 3, 5}, "1,3,5"},
		{"trailing run", []uint32{10, 12, 13, 14}, "10,12-14"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, compactRanges(tt.blocks))
		})
	}
}
