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

package utils

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testFiles []string

func initThpTestEnv(dir string, out io.Writer) {
	thpEnabled = filepath.Join(dir, "enabled")
	thpShmemEnabled = filepath.Join(dir, "shmem_enabled")
	testFiles = []string{thpEnabled, thpShmemEnabled}
	if out != nil {
		logger = GetLogger("thp_test")
		logger.Logger.SetOutput(out)
	}
}

func restoreThpTestEnv() {
	logger = GetLogger("cachefs")
}

func writeTestFiles(contents []string) error {
	for n, path := range testFiles {
		content := contents[n]
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = file.WriteString(content)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestGetThpCurrentPolicy(t *testing.T) {
	tests := []struct {
		name    string
		content string
		expect  string
	}{
		{"valid format", "one [two] three", "two"},
		{"two current values", "[one] [two] three", "one"},
		{"values with space", "one [two three]", "two three"},
		{"values with trailing space", "[one two three ]", "one two three"},
		{"no current value", "one two three", ""},
		{"reverse bracket", "one] [two three", ""},
		{"missing ]", "[one two three", ""},
		{"missing [", "one] two three", ""},
		{"empty", "", ""},
	}

	dir := t.TempDir()
	testfile := filepath.Join(dir, "testfile")
	for _, tc := range tests {
		err := os.WriteFile(testfile, []byte(tc.content), 0644)
		if err != nil {
			t.Errorf("Write test file: %s", err)
			return
		}
		output, err := getThpCurrentPolicy(testfile)
		if tc.expect == "" {
			assert.Error(t, err, tc.name)
		} else {
			assert.Equal(t, tc.expect, output, tc.name)
		}
	}

	_, err := getThpCurrentPolicy("/nosuchtestfile")
	assert.Error(t, err, "no such file")
}

func TestGetThpPolicy(t *testing.T) {
	initThpTestEnv(t.TempDir(), nil)
	defer restoreThpTestEnv()
	contents := []string{
		"[always] madvise never",
		"always within_size [advise] never deny force",
	}
	if err := writeTestFiles(contents); err != nil {
		t.Errorf("Write test files failed: %s", err)
		return
	}

	output, err := GetThpPolicy()
	assert.Equal(t, "always", output, "GetThpPolicy")
	assert.NoError(t, err, "GetThpPolicy")
	output, err = GetThpShmemPolicy()
	assert.Equal(t, "advise", output, "GetThpShmemPolicy")
	assert.NoError(t, err, "GetThpShmemPolicy")
}

func TestSetThpPolicy(t *testing.T) {
	initThpTestEnv(t.TempDir(), nil)
	defer restoreThpTestEnv()
	contents := []string{"", ""}
	if err := writeTestFiles(contents); err != nil {
		t.Errorf("Write test files failed: %s", err)
		return
	}

	err := SetThpPolicy("never")
	if err != nil {
		t.Errorf("SetThpPolicy never failed: %s", err)
	} else {
		content, err := os.ReadFile(thpEnabled)
		if err != nil {
			t.Errorf("ReadFile failed: %s", err)
		} else {
			assert.Equal(t, "never", string(content), "SetThpPolicy")
		}
	}

	err = SetThpShmemPolicy("always")
	if err != nil {
		t.Errorf("SetThpShmemPolicy always failed: %s", err)
	} else {
		content, err := os.ReadFile(thpShmemEnabled)
		if err != nil {
			t.Errorf("ReadFile failed: %s", err)
		} else {
			assert.Equal(t, "always", string(content), "SetThpShmemPolicy")
		}
	}

	err = setThpPolicy("nosuchpolicyfile", "testpolicy")
	assert.Error(t, err, "setThpPolicy ENOENT")
}

func TestSetupThp(t *testing.T) {
	tests := []struct {
		name      string
		contents  []string
		policy    string
		expectLog string
	}{
		{
			name:      "always+default",
			contents:  []string{"[always] madvise never", ""},
			policy:    "default",
			expectLog: "THP policy \"default\", current THP \"always\"",
		},
		{
			name:      "always+always",
			contents:  []string{"[always] madvise never", ""},
			policy:    "always",
			expectLog: "THP policy is already \"always\"",
		},
		{
			name:      "always+madvise",
			contents:  []string{"[always] madvise never", ""},
			policy:    "madvise",
			expectLog: "THP policy is already \"always\"",
		},
		{
			name:      "always+never",
			contents:  []string{"[always] madvise never", ""},
			policy:    "never",
			expectLog: "Unknown THP policy: never",
		},
		{
			name:      "madvise+default",
			contents:  []string{"always [madvise] never", ""},
			policy:    "default",
			expectLog: "THP policy \"default\", current THP \"madvise\"",
		},
		{
			name:      "madvise+always",
			contents:  []string{"always [madvise] never", ""},
			policy:    "always",
			expectLog: "Set THP policy from \"madvise\" to \"always\"",
		},
		{
			name:      "madvise+madvise",
			contents:  []string{"always [madvise] never", ""},
			policy:    "madvise",
			expectLog: "THP policy is already \"madvise\"",
		},
		{
			name:      "never+default",
			contents:  []string{"always madvise [never]", ""},
			policy:    "default",
			expectLog: "THP policy \"default\", current THP \"never\"",
		},
		{
			name:      "never+always",
			contents:  []string{"always madvise [never]", ""},
			policy:    "always",
			expectLog: "Set THP policy from \"never\" to \"always\"",
		},
		{
			name:      "never+madvise",
			contents:  []string{"always madvise [never]", ""},
			policy:    "madvise",
			expectLog: "Set THP policy from \"never\" to \"madvise\"",
		},
	}

	for _, tc := range tests {
		logs := bytes.Buffer{}
		initThpTestEnv(t.TempDir(), &logs)
		if err := writeTestFiles(tc.contents); err != nil {
			t.Errorf("%s: Write test files failed: %s", tc.name, err)
			restoreThpTestEnv()
			continue
		}
		SetupThp(tc.policy)
		assert.Contains(t, logs.String(), tc.expectLog, tc.name)
		restoreThpTestEnv()
	}
}

func TestSetupShmThp(t *testing.T) {
	tests := []struct {
		name      string
		contents  []string
		policy    string
		expectLog string
	}{
		{
			name:      "always+default",
			contents:  []string{"", "[always] advise never deny force"},
			policy:    "default",
			expectLog: "THP policy \"default\", current shmem THP \"always\"",
		},
		{
			name:      "always+always",
			contents:  []string{"", "[always] advise never deny force"},
			policy:    "always",
			expectLog: "Shmem THP policy is already \"always\"",
		},
		{
			name:      "always+advise",
			contents:  []string{"", "[always] advise never deny force"},
			policy:    "advise",
			expectLog: "Shmem THP policy is already \"always\", skip setting to \"advise\"",
		},
		{
			name:      "always+never",
			contents:  []string{"", "[always] advise never deny force"},
			policy:    "never",
			expectLog: "Unknown THP policy: never",
		},
		{
			name:      "advise+default",
			contents:  []string{"", "always [advise] never deny force"},
			policy:    "default",
			expectLog: "THP policy \"default\", current shmem THP \"advise\"",
		},
		{
			name:      "advise+always",
			contents:  []string{"", "always [advise] never deny force"},
			policy:    "always",
			expectLog: "Set shmem THP policy from \"advise\" to \"always\"",
		},
		{
			name:      "advise+advise",
			contents:  []string{"", "always [advise] never deny force"},
			policy:    "advise",
			expectLog: "Shmem THP policy is already \"advise\"",
		},
		{
			name:      "never+default",
			contents:  []string{"", "always advise [never] deny force"},
			policy:    "default",
			expectLog: "THP policy \"default\", current shmem THP \"never\"",
		},
		{
			name:      "never+always",
			contents:  []string{"", "always advise [never] deny force"},
			policy:    "always",
			expectLog: "Set shmem THP policy from \"never\" to \"always\"",
		},
		{
			name:      "never+advise",
			contents:  []string{"", "always advise [never] deny force"},
			policy:    "advise",
			expectLog: "Set shmem THP policy from \"never\" to \"advise\"",
		},
	}

	for _, tc := range tests {
		logs := bytes.Buffer{}
		initThpTestEnv(t.TempDir(), &logs)
		if err := writeTestFiles(tc.contents); err != nil {
			t.Errorf("%s: Write test files failed: %s", tc.name, err)
			restoreThpTestEnv()
			continue
		}
		SetupShmThp(tc.policy)
		assert.Contains(t, logs.String(), tc.expectLog, tc.name)
		restoreThpTestEnv()
	}
}
