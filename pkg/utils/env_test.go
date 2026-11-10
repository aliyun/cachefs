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

package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEnvInt(t *testing.T) {
	const name = "CFS_TEST_GET_ENV_INT"

	tests := []struct {
		desc   string
		set    bool
		val    string
		def    int
		min    int
		expect int
	}{
		{"unset returns default", false, "", 10, 1, 10},
		{"empty returns default", true, "", 10, 1, 10},
		{"valid positive value", true, "42", 10, 1, 42},
		{"value equal to min accepted", true, "1", 9, 1, 1},
		{"value below min returns default", true, "0", 10, 1, 10},
		{"min=0 allows zero", true, "0", 5, 0, 0},
		{"negative below min=0 returns default", true, "-1", 5, 0, 5},
		{"non-numeric returns default", true, "abc", 7, 1, 7},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.set {
				t.Setenv(name, tc.val)
			} else {
				assert.NoError(t, os.Unsetenv(name))
			}
			assert.Equal(t, tc.expect, GetEnvInt(name, tc.def, tc.min))
		})
	}
}

func TestGetEnvBool(t *testing.T) {
	const name = "CFS_TEST_GET_ENV_BOOL"

	tests := []struct {
		desc   string
		set    bool
		val    string
		def    bool
		expect bool
	}{
		{"unset returns default", false, "", true, true},
		{"empty returns default", true, "", true, true},
		{"blank returns default", true, "   ", true, true},
		{"1 is true", true, "1", false, true},
		{"0 is false", true, "0", true, false},
		{"true", true, "true", false, true},
		{"TRUE", true, "TRUE", false, true},
		{"false", true, "false", true, false},
		{"t and f", true, "t", false, true},
		{"yes", true, "yes", false, true},
		{"On is case insensitive", true, "On", false, true},
		{"off", true, "off", true, false},
		{"surrounding space is trimmed", true, " true ", false, true},
		{"unrecognized returns default", true, "maybe", true, true},
		{"unrecognized returns default false", true, "maybe", false, false},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.set {
				t.Setenv(name, tc.val)
			} else {
				assert.NoError(t, os.Unsetenv(name))
			}
			assert.Equal(t, tc.expect, GetEnvBool(name, tc.def))
		})
	}
}
