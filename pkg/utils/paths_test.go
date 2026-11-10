/*
 * JuiceFS, Copyright 2025 Juicedata, Inc.
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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReadPathsFromFile(t *testing.T) {
	dir := t.TempDir()
	assert := assert.New(t)

	t.Run("normal", func(t *testing.T) {
		f := filepath.Join(dir, "paths.txt")
		assert.NoError(os.WriteFile(f, []byte("/a/\n/b/c/\n/d/\n"), 0644))
		paths, err := ReadPathsFromFile(f)
		assert.NoError(err)
		assert.Equal([]string{"/a/", "/b/c/", "/d/"}, paths)
	})

	t.Run("skip comments and empty lines", func(t *testing.T) {
		f := filepath.Join(dir, "comments.txt")
		assert.NoError(os.WriteFile(f, []byte("# comment\n/a/\n\n# another comment\n/b/\n"), 0644))
		paths, err := ReadPathsFromFile(f)
		assert.NoError(err)
		assert.Equal([]string{"/a/", "/b/"}, paths)
	})

	t.Run("file not found", func(t *testing.T) {
		_, err := ReadPathsFromFile(filepath.Join(dir, "nonexistent"))
		assert.Error(err)
	})

	t.Run("empty file", func(t *testing.T) {
		f := filepath.Join(dir, "empty.txt")
		assert.NoError(os.WriteFile(f, []byte(""), 0644))
		paths, err := ReadPathsFromFile(f)
		assert.NoError(err)
		assert.Empty(paths)
	})
}

func TestStringSetEqual(t *testing.T) {
	assert := assert.New(t)

	t.Run("equal sets", func(t *testing.T) {
		a := map[string]struct{}{"/a/": {}, "/b/": {}}
		b := map[string]struct{}{"/b/": {}, "/a/": {}}
		assert.True(StringSetEqual(a, b))
	})

	t.Run("different length", func(t *testing.T) {
		a := map[string]struct{}{"/a/": {}}
		b := map[string]struct{}{"/a/": {}, "/b/": {}}
		assert.False(StringSetEqual(a, b))
	})

	t.Run("same length different content", func(t *testing.T) {
		a := map[string]struct{}{"/a/": {}, "/b/": {}}
		b := map[string]struct{}{"/a/": {}, "/c/": {}}
		assert.False(StringSetEqual(a, b))
	})

	t.Run("both empty", func(t *testing.T) {
		assert.True(StringSetEqual(map[string]struct{}{}, map[string]struct{}{}))
	})
}

func TestToStringSet(t *testing.T) {
	assert := assert.New(t)

	t.Run("normal", func(t *testing.T) {
		s := ToStringSet([]string{"/a/", "/b/"})
		assert.Equal(map[string]struct{}{"/a/": {}, "/b/": {}}, s)
	})

	t.Run("dedup", func(t *testing.T) {
		s := ToStringSet([]string{"/a/", "/a/", "/b/"})
		assert.Len(s, 2)
		assert.Contains(s, "/a/")
		assert.Contains(s, "/b/")
	})

	t.Run("empty", func(t *testing.T) {
		s := ToStringSet([]string{})
		assert.Empty(s)
	})

	t.Run("nil", func(t *testing.T) {
		s := ToStringSet(nil)
		assert.Empty(s)
	})
}

func TestParsePathsFromReader(t *testing.T) {
	assert := assert.New(t)

	t.Run("normal", func(t *testing.T) {
		paths, err := ParsePathsFromReader(strings.NewReader("/a/\n/b/c/\n/d/\n"))
		assert.NoError(err)
		assert.Equal([]string{"/a/", "/b/c/", "/d/"}, paths)
	})

	t.Run("skip comments and empty lines", func(t *testing.T) {
		paths, err := ParsePathsFromReader(strings.NewReader("# comment\n/a/\n\n# another\n/b/\n"))
		assert.NoError(err)
		assert.Equal([]string{"/a/", "/b/"}, paths)
	})

	t.Run("empty input", func(t *testing.T) {
		paths, err := ParsePathsFromReader(strings.NewReader(""))
		assert.NoError(err)
		assert.Empty(paths)
	})
}

func TestFmtOutput(t *testing.T) {
	assert := assert.New(t)

	t.Run("nil", func(t *testing.T) {
		assert.Equal("<empty>", fmtOutput(nil))
	})

	t.Run("empty", func(t *testing.T) {
		assert.Equal("<empty>", fmtOutput([]byte{}))
	})

	t.Run("only newlines", func(t *testing.T) {
		assert.Equal("<empty>", fmtOutput([]byte("\n\n")))
	})

	t.Run("trims trailing newlines", func(t *testing.T) {
		assert.Equal("/a/\n/b/", fmtOutput([]byte("/a/\n/b/\n\n")))
	})

	t.Run("keeps leading and inner whitespace", func(t *testing.T) {
		assert.Equal("  /a/\n/b/", fmtOutput([]byte("  /a/\n/b/")))
	})

	t.Run("no trailing newline", func(t *testing.T) {
		assert.Equal("/a/", fmtOutput([]byte("/a/")))
	})
}

func TestReadPathsFromCmd(t *testing.T) {
	assert := assert.New(t)

	t.Run("normal", func(t *testing.T) {
		paths, out, dur, err := ReadPathsFromCmd("test", "printf /a/\\n/b/\\n", 1*time.Second)
		assert.NoError(err)
		assert.Equal([]string{"/a/", "/b/"}, paths)
		assert.Equal("/a/\n/b/", out)
		assert.Positive(dur)
	})

	t.Run("skip comments and empty lines", func(t *testing.T) {
		script := filepath.Join(t.TempDir(), "list.sh")
		assert.NoError(os.WriteFile(script, []byte("#!/bin/sh\nprintf '# comment\\n/a/\\n\\n/b/\\n'\n"), 0755))
		paths, out, dur, err := ReadPathsFromCmd("test", script, 1*time.Second)
		assert.NoError(err)
		assert.Equal([]string{"/a/", "/b/"}, paths)
		assert.Equal("# comment\n/a/\n\n/b/", out)
		assert.Positive(dur)
	})

	t.Run("empty output", func(t *testing.T) {
		paths, out, dur, err := ReadPathsFromCmd("test", "true", 1*time.Second)
		assert.NoError(err)
		assert.Empty(paths)
		assert.Equal("<empty>", out)
		assert.Positive(dur)
	})

	t.Run("command not found", func(t *testing.T) {
		_, _, _, err := ReadPathsFromCmd("test", "/nonexistent/command", 1*time.Second)
		assert.Error(err)
	})

	t.Run("nonzero exit", func(t *testing.T) {
		_, _, _, err := ReadPathsFromCmd("test", "false", 1*time.Second)
		assert.Error(err)
	})

	t.Run("timeout", func(t *testing.T) {
		_, _, _, err := ReadPathsFromCmd("test", "sleep 2", 1*time.Second)
		assert.ErrorIs(err, context.DeadlineExceeded)
	})

	t.Run("zero timeout means no timeout", func(t *testing.T) {
		paths, out, dur, err := ReadPathsFromCmd("test", "true", 0)
		assert.NoError(err)
		assert.Empty(paths)
		assert.Equal("<empty>", out)
		assert.Positive(dur)
	})

	t.Run("empty command", func(t *testing.T) {
		_, _, _, err := ReadPathsFromCmd("test", "", 1*time.Second)
		assert.Error(err)
	})

	t.Run("whitespace only command", func(t *testing.T) {
		_, _, _, err := ReadPathsFromCmd("test", "   ", 1*time.Second)
		assert.Error(err)
	})
}
