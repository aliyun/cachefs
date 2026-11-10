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

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oddName exercises the manifest escaping: it holds a space, a backslash, a
// backtick and shell metacharacters, like the file the smoke test fixture builds.
const oddName = `test_# name-with?!strangetchar%20(){}[]` + "`" + `~\=+|\`

func TestEscape(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "plain", in: "/dir/file.txt", want: "/dir/file.txt"},
		{name: "space", in: "a b", want: `a\x20b`},
		{name: "backslash", in: `a\b`, want: `a\x5cb`},
		// A name that already looks like an escape must not decode as one.
		{name: "looks like an escape", in: `\x41`, want: `\x5cx41`},
		{name: "tab and newline", in: "a\tb\nc", want: `a\x09b\x0ac`},
		{name: "utf8", in: "目录/文件", want: `\xe7\x9b\xae\xe5\xbd\x95/\xe6\x96\x87\xe4\xbb\xb6`},
		{name: "del and high byte", in: "a\x7f\xff", want: `a\x7f\xff`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, escape(tt.in))
		})
	}
}

func TestEscapeRoundTrip(t *testing.T) {
	for _, in := range []string{
		"", "/", "/dir/file.txt", "a b", `a\b`, `\x41`, `\x5c`, "a\tb\nc",
		"目录/文件", "a\x7f\xff", "\x00\x01", oddName, "/" + oddName,
	} {
		t.Run(escape(in), func(t *testing.T) {
			got := escape(in)
			// A token holding whitespace would break the manifest line format.
			assert.NotContains(t, got, " ")
			assert.NotContains(t, got, "\t")
			assert.NotContains(t, got, "\n")
			back, err := unescape(got)
			require.NoError(t, err)
			assert.Equal(t, in, back)
		})
	}
}

func TestUnescapeErrors(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   string
	}{
		{name: "trailing backslash", in: `abc\`},
		{name: "backslash not followed by x", in: `ab\z41`},
		{name: "truncated hex", in: `ab\x4`},
		{name: "invalid hex", in: `ab\xzz`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := unescape(tt.in)
			assert.Error(t, err)
		})
	}
}

func TestParseFields(t *testing.T) {
	for _, tt := range []struct {
		in    string
		canon string
		want  fieldSet
	}{
		{in: "", canon: ""},
		{in: "d", canon: "d", want: fieldSet{data: true}},
		{in: "ugomd", canon: "ugomd", want: fieldSet{uid: true, gid: true, mode: true, mtime: true, data: true}},
		{in: "ugomcd", canon: "ugomcd", want: fieldSet{uid: true, gid: true, mode: true, mtime: true, ctime: true, data: true}},
		// The canonical order is fixed so that two equivalent -fields values
		// produce byte-identical manifests.
		{in: "dmoug", canon: "ugomd", want: fieldSet{uid: true, gid: true, mode: true, mtime: true, data: true}},
		{in: "uudd", canon: "ud", want: fieldSet{uid: true, data: true}},
	} {
		t.Run("fields="+tt.in, func(t *testing.T) {
			got, canon, err := parseFields(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.canon, canon)
			assert.Equal(t, tt.want, got)
		})
	}

	for _, in := range []string{"x", "ugomdX", "ugo m d", "U"} {
		t.Run("invalid="+in, func(t *testing.T) {
			_, _, err := parseFields(in)
			assert.Error(t, err)
		})
	}
}

func TestTypeLetter(t *testing.T) {
	for _, tt := range []struct {
		mode uint32
		want byte
	}{
		{syscall.S_IFDIR | 0755, 'd'},
		{syscall.S_IFREG | 0644, 'f'},
		{syscall.S_IFLNK | 0777, 'l'},
		{syscall.S_IFCHR | 0666, 'c'},
		{syscall.S_IFBLK | 0660, 'b'},
		{syscall.S_IFIFO | 0600, 'p'},
		{syscall.S_IFSOCK | 0777, 's'},
		{0, 'u'},
	} {
		assert.Equal(t, tt.want, typeLetter(tt.mode), "mode %o", tt.mode)
	}
}

func TestFormatRdev(t *testing.T) {
	for _, tt := range []struct {
		name string
		rdev uint64
		want string
	}{
		{name: "zero", rdev: 0, want: "0:0"},
		// makedev(1, 3), the encoding of /dev/null.
		{name: "char 1:3", rdev: 0x103, want: "1:3"},
		// A minor above 255 lives in the high bits of the encoding.
		{name: "minor above 255", rdev: 0x3008e8, want: "8:1000"},
		{name: "maximum", rdev: 0xffffffff, want: "4095:1048575"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, formatRdev(tt.rdev))
		})
	}
}

func TestEntryAttrs(t *testing.T) {
	all := fieldSet{uid: true, gid: true, mode: true, mtime: true, ctime: true, data: true}
	none := fieldSet{}

	file := &entry{path: "/f", typ: 'f', mode: 0100644, uid: 7, gid: 8, nlink: 2, size: 9, mtime: 111, ctime: 222, data: "deadbeef"}
	dir := &entry{path: "/d", typ: 'd', mode: 040755, uid: 7, gid: 8, nlink: 3, mtime: 111, ctime: 222}
	link := &entry{path: "/l", typ: 'l', mode: 0120777, uid: 7, gid: 8, nlink: 1, mtime: 111, ctime: 222, target: `a b\c`}
	dev := &entry{path: "/c", typ: 'c', mode: 020666, uid: 7, gid: 8, nlink: 1, mtime: 111, ctime: 222, rdev: "1:3"}
	fifo := &entry{path: "/p", typ: 'p', mode: 010600, uid: 7, gid: 8, nlink: 1, mtime: 111, ctime: 222}

	assert.Equal(t,
		"type=f mode=100644 uid=7 gid=8 nlink=2 size=9 data=deadbeef mtime=111 ctime=222",
		file.attrs(all))
	// Every optional field is gated, but type, nlink and size always remain.
	assert.Equal(t, "type=f nlink=2 size=9", file.attrs(none))
	// A directory's nlink counts subdirectories, so it is left out.
	assert.NotContains(t, dir.attrs(all), "nlink=")
	assert.Contains(t, dir.attrs(all), "type=d mode=040755")
	// Only a regular file carries a size or a digest.
	assert.NotContains(t, link.attrs(all), "size=")
	assert.NotContains(t, link.attrs(all), "data=")
	assert.NotContains(t, fifo.attrs(all), "size=")
	assert.Contains(t, dev.attrs(all), "rdev=1:3")
	assert.NotContains(t, fifo.attrs(all), "rdev=")

	// The point of the escaping: a target holding a space and a backslash must
	// still leave the line splittable on whitespace.
	attrs := link.attrs(all)
	assert.Contains(t, attrs, `target=a\x20b\x5cc`)
	tokens := strings.Fields(attrs)
	assert.Len(t, tokens, strings.Count(attrs, " ")+1)
	for _, tok := range tokens {
		assert.Contains(t, tok, "=", "token %q is not a key=value pair", tok)
	}
}

func TestParseAttrs(t *testing.T) {
	assert.Equal(t,
		map[string]string{"type": "f", "size": "9", "target": `a\x20b`},
		parseAttrs(`type=f size=9 target=a\x20b`))
	assert.Empty(t, parseAttrs(""))
}

func TestDiffAttrs(t *testing.T) {
	assert.Equal(t, "mode: want 100644 got 100600",
		diffAttrs("type=f mode=100644", "type=f mode=100600"))
	assert.Equal(t, "data: want aa got bb; size: want 1 got 2",
		diffAttrs("type=f size=1 data=aa", "type=f size=2 data=bb"))
	assert.Equal(t, "ctime: want 5, absent in tree",
		diffAttrs("type=f ctime=5", "type=f"))
	assert.Equal(t, "ctime: absent in manifest, got 5",
		diffAttrs("type=f", "type=f ctime=5"))
	assert.Empty(t, diffAttrs("type=f", "type=f"))
}

func TestSortedKeys(t *testing.T) {
	assert.Equal(t, []string{"a", "b", "c"}, sortedKeys(map[string]int{"c": 3, "a": 1, "b": 2}))
	assert.Empty(t, sortedKeys(map[string]struct{}{}))
}

func TestHeaderFields(t *testing.T) {
	got, err := headerFields(manifestHeader + " fields=ugomcd")
	require.NoError(t, err)
	assert.Equal(t, "ugomcd", got)

	_, err = headerFields(manifestHeader)
	assert.Error(t, err)
}

func TestResolveRoot(t *testing.T) {
	dir := t.TempDir()
	resolvedDir, err := resolveRoot(dir)
	require.NoError(t, err)

	// A symlinked root must resolve to its target, or WalkDir would stop at the
	// link and silently summarize an empty tree.
	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(dir, link))
	resolvedLink, err := resolveRoot(link)
	require.NoError(t, err)
	assert.Equal(t, resolvedDir, resolvedLink)

	_, err = resolveRoot(filepath.Join(dir, "absent"))
	assert.Error(t, err)

	file := filepath.Join(dir, "file")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0644))
	_, err = resolveRoot(file)
	assert.Error(t, err, "a regular file is not a tree")
}

// buildFixture lays out a tree holding every node kind the smoke test fixture
// contains, except character and block devices, which need privileges to create.
// Two calls produce trees with identical attributes apart from their timestamps.
func buildFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	require.NoError(t, os.Mkdir(filepath.Join(root, "dir"), 0755))
	require.NoError(t, os.Mkdir(filepath.Join(root, "empty"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "dir", "nested.txt"), []byte("nested"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, oddName), []byte("odd"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "hard1"), []byte("hard"), 0644))
	require.NoError(t, os.Link(filepath.Join(root, "hard1"), filepath.Join(root, "hard2")))
	require.NoError(t, os.Symlink("file.txt", filepath.Join(root, "link")))
	// A dangling link whose target holds a space.
	require.NoError(t, os.Symlink("target with space", filepath.Join(root, "odd link")))
	require.NoError(t, syscall.Mkfifo(filepath.Join(root, "fifo"), 0600))
	return root
}

// flipMode toggles a permission bit. It cannot hard-code the target mode: the
// fixture is created under whatever umask the test runs with, so 0644 may
// already be 0600 on disk and a chmod to it would change nothing.
func flipMode(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, fi.Mode().Perm()^0004))
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}

func entriesByPath(t *testing.T, root string, f fieldSet) map[string]entry {
	t.Helper()
	list, err := walk(root, f)
	require.NoError(t, err)
	out := make(map[string]entry, len(list))
	for _, e := range list {
		out[e.path] = e
	}
	return out
}

func writeFixtureManifest(t *testing.T, root, file, fields string) fieldSet {
	t.Helper()
	f, canon, err := parseFields(fields)
	require.NoError(t, err)
	list, err := walk(root, f)
	require.NoError(t, err)
	require.NoError(t, writeManifest(file, canon, f, list))
	return f
}

func TestWalk(t *testing.T) {
	root := buildFixture(t)
	f := fieldSet{uid: true, gid: true, mode: true, mtime: true, data: true}
	got := entriesByPath(t, root, f)

	// The root itself is never recorded, only the tree below it.
	assert.NotContains(t, got, "/")
	assert.Len(t, got, 10)
	for _, want := range []string{
		"/dir", "/dir/nested.txt", "/empty", "/file.txt", "/fifo",
		"/hard1", "/hard2", "/link", "/odd link", "/" + oddName,
	} {
		assert.Contains(t, got, want)
	}

	assert.Equal(t, byte('d'), got["/dir"].typ)
	assert.Equal(t, byte('f'), got["/file.txt"].typ)
	assert.Equal(t, byte('l'), got["/link"].typ)
	assert.Equal(t, byte('p'), got["/fifo"].typ)

	// A symlink records its target and is never followed, dangling or not.
	assert.Equal(t, "file.txt", got["/link"].target)
	assert.Equal(t, "target with space", got["/odd link"].target)
	assert.Zero(t, got["/link"].size)

	// Hardlinks share a digest and both record nlink 2.
	assert.Equal(t, uint64(2), got["/hard1"].nlink)
	assert.Equal(t, uint64(2), got["/hard2"].nlink)
	assert.NotEmpty(t, got["/hard1"].data)
	assert.Equal(t, got["/hard1"].data, got["/hard2"].data)

	// Only a regular file carries a size or a digest, so a fifo is recorded by
	// metadata alone and walking a tree that holds one can never block.
	assert.Equal(t, int64(4), got["/file.txt"].size)
	assert.NotEmpty(t, got["/file.txt"].data)
	assert.Zero(t, got["/fifo"].size)
	assert.Empty(t, got["/fifo"].data)

	// Without the data field nothing is hashed, but size is still recorded.
	noData := entriesByPath(t, root, fieldSet{mode: true})
	assert.Empty(t, noData["/file.txt"].data)
	assert.Equal(t, got["/file.txt"].size, noData["/file.txt"].size)
}

func TestWalkOrder(t *testing.T) {
	list, err := walk(buildFixture(t), fieldSet{})
	require.NoError(t, err)
	for i := 1; i < len(list); i++ {
		assert.Less(t, list[i-1].path, list[i].path,
			"the manifest must be sorted so two runs over one tree are byte-identical")
	}
}

// A tree that cannot be read in full must fail rather than quietly summarize
// part of itself: a short manifest would let a later verify pass while checking
// nothing at all.
func TestWalkFailsLoudly(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permissions")
	}
	for _, tt := range []struct {
		name   string
		target string
		fields fieldSet
	}{
		{name: "unreadable directory", target: "dir"},
		{name: "unreadable file", target: "file.txt", fields: fieldSet{data: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := buildFixture(t)
			path := filepath.Join(root, tt.target)
			require.NoError(t, os.Chmod(path, 0))
			t.Cleanup(func() { os.Chmod(path, 0755) })

			_, err := walk(root, tt.fields)
			assert.Error(t, err)
		})
	}
}

func TestManifestRoundTrip(t *testing.T) {
	root := buildFixture(t)
	manifest := filepath.Join(t.TempDir(), "manifest")
	f := writeFixtureManifest(t, root, manifest, "ugomd")

	m, err := readManifest(manifest)
	require.NoError(t, err)
	assert.Equal(t, "ugomd", m.fields)
	assert.Equal(t, f, m.f)

	list, err := walk(root, f)
	require.NoError(t, err)
	require.Len(t, m.entries, len(list))
	for i := range list {
		assert.Equal(t, list[i].attrs(f), m.entries[list[i].path], "entry %s", list[i].path)
	}
}

func TestReadManifestErrors(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte(body), 0644))
		return p
	}

	for _, tt := range []struct {
		name string
		path string
	}{
		{name: "missing file", path: filepath.Join(dir, "absent")},
		{name: "empty file", path: write("empty", "")},
		{name: "no header", path: write("noheader", "/a type=f\n")},
		{name: "header without fields", path: write("nofields", manifestHeader+"\n")},
		{name: "unknown field letter", path: write("badfields", manifestHeader+" fields=ux\n")},
		{name: "entry without attributes", path: write("malformed", manifestHeader+" fields=ugomd\nnoseparator\n")},
		{name: "duplicate entry", path: write("duplicate", manifestHeader+" fields=ugomd\n/a type=f\n/a type=f\n")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readManifest(tt.path)
			assert.Error(t, err)
		})
	}

	t.Run("escaped path survives", func(t *testing.T) {
		p := write("escaped", manifestHeader+" fields=ugomd\n"+escape("/"+oddName)+" type=f\n")
		m, err := readManifest(p)
		require.NoError(t, err)
		assert.Contains(t, m.entries, "/"+oddName)
	})
}

func TestVerify(t *testing.T) {
	// Each case builds and snapshots its own tree: a shared manifest would carry
	// the first tree's timestamps, and mtime is part of most field sets.
	for _, tt := range []struct {
		name    string
		fields  string
		mutate  func(t *testing.T, root string)
		wantErr string // substring of the error, empty when verify must succeed
	}{
		{name: "unmodified", fields: "ugomcd"},
		{
			name: "content", fields: "ugomd", wantErr: "1 difference",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.WriteFile(filepath.Join(root, "file.txt"), []byte("tampered"), 0644))
			},
		},
		{
			name: "missing", fields: "ugomd", wantErr: "1 difference",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.Remove(filepath.Join(root, "file.txt")))
			},
		},
		{
			name: "unexpected", fields: "ugomd", wantErr: "1 difference",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.WriteFile(filepath.Join(root, "extra"), []byte("x"), 0644))
			},
		},
		{
			name: "mode", fields: "ugomd", wantErr: "1 difference",
			mutate: func(t *testing.T, root string) {
				flipMode(t, filepath.Join(root, "file.txt"))
			},
		},
		{
			// mode is not in the field set, so this chmod is invisible.
			name: "mode not selected", fields: "ugd",
			mutate: func(t *testing.T, root string) {
				flipMode(t, filepath.Join(root, "file.txt"))
			},
		},
		{
			name: "mtime", fields: "ugomd", wantErr: "1 difference",
			mutate: func(t *testing.T, root string) {
				old := time.Now().Add(-time.Hour)
				require.NoError(t, os.Chtimes(filepath.Join(root, "file.txt"), old, old))
			},
		},
		{
			// Same chtimes, but no timestamp is in the field set.
			name: "mtime not selected", fields: "ugod",
			mutate: func(t *testing.T, root string) {
				old := time.Now().Add(-time.Hour)
				require.NoError(t, os.Chtimes(filepath.Join(root, "file.txt"), old, old))
			},
		},
		{
			name: "symlink target", fields: "ugomd", wantErr: "1 difference",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.Remove(filepath.Join(root, "link")))
				require.NoError(t, os.Symlink("elsewhere", filepath.Join(root, "link")))
			},
		},
		{
			// Same bytes, but hard2 is no longer a link to hard1, so nlink drops.
			name: "hardlink broken", fields: "ugod", wantErr: "2 differences",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.Remove(filepath.Join(root, "hard2")))
				require.NoError(t, os.WriteFile(filepath.Join(root, "hard2"), []byte("hard"), 0644))
			},
		},
		{
			name: "file became a symlink", fields: "ugomd", wantErr: "1 difference",
			mutate: func(t *testing.T, root string) {
				require.NoError(t, os.Remove(filepath.Join(root, "file.txt")))
				require.NoError(t, os.Symlink("elsewhere", filepath.Join(root, "file.txt")))
			},
		},
		{
			name: "more differences than the report cap", fields: "ugomd",
			wantErr: fmt.Sprintf("%d differences", maxReportedDiffs+8),
			mutate: func(t *testing.T, root string) {
				for i := 0; i < maxReportedDiffs+8; i++ {
					name := fmt.Sprintf("extra%02d", i)
					require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte("x"), 0644))
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := buildFixture(t)
			manifest := filepath.Join(t.TempDir(), "manifest")
			writeFixtureManifest(t, root, manifest, tt.fields)
			if tt.mutate != nil {
				tt.mutate(t, root)
			}

			err := verify(manifest, root)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}

	// The smoke test snapshots the source dir and verifies the mount, so the
	// manifest must not depend on the path of the root it was written for.
	// Timestamps are excluded because the two trees are built at two times.
	t.Run("independent of the root path", func(t *testing.T) {
		manifest := filepath.Join(t.TempDir(), "manifest")
		writeFixtureManifest(t, buildFixture(t), manifest, "ugod")
		assert.NoError(t, verify(manifest, buildFixture(t)))
	})

	t.Run("absent manifest", func(t *testing.T) {
		assert.Error(t, verify(filepath.Join(t.TempDir(), "absent"), buildFixture(t)))
	})
}

func TestRun(t *testing.T) {
	// run reports argument errors through flag.Usage, which is only noise here.
	saved := flag.CommandLine.Output()
	flag.CommandLine.SetOutput(io.Discard)
	t.Cleanup(func() { flag.CommandLine.SetOutput(saved) })

	dir := buildFixture(t)
	manifest := filepath.Join(t.TempDir(), "manifest")

	t.Run("write then verify", func(t *testing.T) {
		code, err := run(manifest, "", "ugomd", []string{dir})
		require.NoError(t, err)
		assert.Equal(t, exitOK, code)

		code, err = run("", manifest, "ugomd", []string{dir})
		require.NoError(t, err)
		assert.Equal(t, exitOK, code)
	})

	t.Run("mismatch", func(t *testing.T) {
		other := buildFixture(t)
		otherManifest := filepath.Join(t.TempDir(), "manifest")
		code, err := run(otherManifest, "", "ugomd", []string{other})
		require.NoError(t, err)
		require.Equal(t, exitOK, code)

		require.NoError(t, os.WriteFile(filepath.Join(other, "file.txt"), []byte("tampered"), 0644))
		code, err = run("", otherManifest, "ugomd", []string{other})
		require.Error(t, err)
		assert.Equal(t, exitMismatch, code)
	})

	for _, tt := range []struct {
		name             string
		writeTo, readFrm string
		fields           string
		args             []string
	}{
		{name: "neither mode", fields: "ugomd", args: []string{dir}},
		{name: "both modes", writeTo: manifest, readFrm: manifest, fields: "ugomd", args: []string{dir}},
		{name: "no dir", writeTo: manifest, fields: "ugomd"},
		{name: "two dirs", writeTo: manifest, fields: "ugomd", args: []string{dir, dir}},
		{name: "bad fields", writeTo: filepath.Join(t.TempDir(), "m"), fields: "nope", args: []string{dir}},
		// The flag package stops at the first positional argument, so this form
		// would otherwise make "-fields" the manifest name.
		{name: "flag swallowed as manifest", writeTo: "-fields", fields: "ugomd", args: []string{"ugomd", manifest, dir}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, err := run(tt.writeTo, tt.readFrm, tt.fields, tt.args)
			assert.Error(t, err)
			assert.Equal(t, exitFailed, code)
		})
	}

	t.Run("fields are normalized in the header", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "m")
		code, err := run(out, "", "dmoug", []string{dir})
		require.NoError(t, err)
		require.Equal(t, exitOK, code)
		body, err := os.ReadFile(out)
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(string(body), manifestHeader+" fields=ugomd\n"),
			"header was %q", firstLine(string(body)))
	})
}
