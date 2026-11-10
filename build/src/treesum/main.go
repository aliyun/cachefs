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

// Command treesum snapshots a directory tree into a manifest and verifies
// another tree against it. The smoke tests snapshot the source dir once and
// then verify every mount against that snapshot, so any byte or attribute
// CacheFS fails to reproduce shows up as a diff.
//
// Usage:
//
//	treesum -w MANIFEST [-fields ugomcd] DIR    write a manifest for DIR
//	treesum -r MANIFEST DIR			    verify DIR against MANIFEST
//
// -fields picks the optional attributes: u=uid, g=gid, o=mode, m=mtime,
// c=ctime, d=file data. Type, nlink, size, symlink target and device numbers
// are always recorded. Timestamps are whole seconds. The root directory itself
// is not recorded, only the tree below it, and the walk stays inside the root's
// filesystem.
//
// Regular file content is the only thing ever read: a fifo or a socket in the
// tree is recorded by its metadata alone, so verifying a tree that contains one
// never blocks.
//
// The manifest header records the field set and -r takes it from there, so a
// manifest is always verified against the attributes it was built from.
package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

// Exit codes: 0 verified, 1 the trees differ, 2 treesum itself failed.
const (
	exitOK       = 0
	exitMismatch = 1
	exitFailed   = 2
)

const (
	manifestHeader   = "# treesum manifest v1"
	maxReportedDiffs = 32
	// fieldOrder is the canonical order of the field letters, used both to
	// normalize -fields and to document it.
	fieldOrder = "ugomcd"
	// manifestPathLimit bounds a single manifest line. Paths are escaped, so
	// an entry is longer than the path alone, and the data digest adds 32 bytes.
	manifestPathLimit = 16 << 20
)

// fieldSet is the set of optional attributes to record.
type fieldSet struct {
	uid, gid, mode, mtime, ctime, data bool
}

func parseFields(s string) (fieldSet, string, error) {
	var f fieldSet
	var canon strings.Builder
	seen := make(map[rune]bool)
	for _, c := range s {
		if seen[c] {
			continue
		}
		seen[c] = true
		switch c {
		case 'u':
			f.uid = true
		case 'g':
			f.gid = true
		case 'o':
			f.mode = true
		case 'm':
			f.mtime = true
		case 'c':
			f.ctime = true
		case 'd':
			f.data = true
		default:
			return f, "", fmt.Errorf("unknown field %q, want a combination of %s", c, fieldOrder)
		}
	}
	for _, c := range fieldOrder {
		if seen[c] {
			canon.WriteRune(c)
		}
	}
	return f, canon.String(), nil
}

// entry holds the recorded attributes of one tree node.
type entry struct {
	path   string // relative to the walked root, always begins with "/"
	typ    byte   // d, f, l, c, b, p or s, as in find -printf %y
	mode   uint32 // full st_mode, file type bits included
	uid    uint32
	gid    uint32
	nlink  uint64
	size   int64  // regular files only
	mtime  int64  // seconds since the epoch
	ctime  int64  // seconds since the epoch
	target string // symlink target
	rdev   string // "major:minor", character and block devices only
	data   string // hex digest of the file content
}

// attrs renders the entry as space-separated key=value tokens. Every token is
// free of whitespace, so a manifest line splits cleanly even when the path or a
// symlink target contains a space.
func (e *entry) attrs(f fieldSet) string {
	var b strings.Builder
	fmt.Fprintf(&b, "type=%c", e.typ)
	if f.mode {
		fmt.Fprintf(&b, " mode=%06o", e.mode)
	}
	if f.uid {
		fmt.Fprintf(&b, " uid=%d", e.uid)
	}
	if f.gid {
		fmt.Fprintf(&b, " gid=%d", e.gid)
	}
	// Directory nlink counts subdirectories, so it depends on the filesystem
	// rather than on the tree's contents; leave it out.
	if e.typ != 'd' {
		fmt.Fprintf(&b, " nlink=%d", e.nlink)
	}
	switch e.typ {
	case 'f':
		fmt.Fprintf(&b, " size=%d", e.size)
		if f.data {
			fmt.Fprintf(&b, " data=%s", e.data)
		}
	case 'l':
		fmt.Fprintf(&b, " target=%s", escape(e.target))
	case 'c', 'b':
		fmt.Fprintf(&b, " rdev=%s", e.rdev)
	}
	if f.mtime {
		fmt.Fprintf(&b, " mtime=%d", e.mtime)
	}
	if f.ctime {
		fmt.Fprintf(&b, " ctime=%d", e.ctime)
	}
	return b.String()
}

func main() {
	writeTo := flag.String("w", "", "write a manifest for DIR to this file")
	readFrom := flag.String("r", "", "verify DIR against this manifest")
	fields := flag.String("fields", "ugomd", "attributes to record, any combination of "+fieldOrder)
	flag.Usage = func() {
		out := flag.CommandLine.Output()
		fmt.Fprintf(out, "usage: treesum -w MANIFEST [-fields %s] DIR\n", fieldOrder)
		fmt.Fprint(out, "       treesum -r MANIFEST DIR\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	code, err := run(*writeTo, *readFrom, *fields, flag.Args())
	if err != nil {
		fmt.Fprintf(os.Stderr, "treesum: %s\n", err)
	}
	os.Exit(code)
}

func run(writeTo, readFrom, fields string, args []string) (int, error) {
	// The flag package stops at the first non-flag argument, so "treesum -w
	// -fields x FILE DIR" silently makes "-fields" the manifest name and leaves
	// three positional arguments. Point at the real mistake instead.
	if strings.HasPrefix(writeTo, "-") || strings.HasPrefix(readFrom, "-") {
		flag.Usage()
		return exitFailed, errors.New("-w and -r take a manifest name; all flags must precede DIR")
	}
	if (writeTo == "") == (readFrom == "") {
		flag.Usage()
		return exitFailed, errors.New("exactly one of -w or -r is required")
	}
	if len(args) != 1 {
		flag.Usage()
		return exitFailed, errors.New("exactly one DIR argument is required")
	}
	root, err := resolveRoot(args[0])
	if err != nil {
		return exitFailed, err
	}

	if readFrom != "" {
		if fieldsFlagSet() {
			fmt.Fprint(os.Stderr, "treesum: warning: -fields ignored in -r mode, using the manifest's\n")
		}
		if err := verify(readFrom, root); err != nil {
			return exitMismatch, err
		}
		return exitOK, nil
	}

	f, canon, err := parseFields(fields)
	if err != nil {
		return exitFailed, err
	}
	entries, err := walk(root, f)
	if err != nil {
		return exitFailed, err
	}
	if err := writeManifest(writeTo, canon, f, entries); err != nil {
		return exitFailed, err
	}
	fmt.Printf("treesum: wrote %d entries from %s to %s\n", len(entries), root, writeTo)
	return exitOK, nil
}

func fieldsFlagSet() bool {
	set := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "fields" {
			set = true
		}
	})
	return set
}

// resolveRoot follows a symlinked DIR so the walk descends into its target
// instead of stopping at the link.
func resolveRoot(dir string) (string, error) {
	root, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", err
	}
	fi, err := os.Stat(root)
	if err != nil {
		return "", err
	}
	if !fi.IsDir() {
		return "", fmt.Errorf("%s is not a directory", dir)
	}
	return root, nil
}

// walk records every node below root, sorted by path.
func walk(root string, f fieldSet) ([]entry, error) {
	rootSt, err := lstat(root)
	if err != nil {
		return nil, err
	}
	var out []entry
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		st, err := lstat(path)
		if err != nil {
			return err
		}
		if st.Dev != rootSt.Dev {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		e, err := newEntry(root, path, st, f)
		if err != nil {
			return err
		}
		out = append(out, e)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, nil
}

func newEntry(root, path string, st *syscall.Stat_t, f fieldSet) (entry, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return entry{}, err
	}
	e := entry{
		path:  "/" + rel,
		typ:   typeLetter(st.Mode),
		mode:  st.Mode,
		uid:   st.Uid,
		gid:   st.Gid,
		nlink: uint64(st.Nlink),
		mtime: st.Mtim.Sec,
		ctime: st.Ctim.Sec,
	}
	switch e.typ {
	case 'f':
		e.size = st.Size
		if f.data {
			if e.data, err = hashFile(path); err != nil {
				return entry{}, err
			}
		}
	case 'l':
		if e.target, err = os.Readlink(path); err != nil {
			return entry{}, err
		}
	case 'c', 'b':
		e.rdev = formatRdev(st.Rdev)
	}
	return e, nil
}

func writeManifest(file, canon string, f fieldSet, entries []entry) error {
	out, err := os.Create(file)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(out)
	fmt.Fprintf(w, "%s fields=%s\n", manifestHeader, canon)
	for i := range entries {
		fmt.Fprintf(w, "%s %s\n", escape(entries[i].path), entries[i].attrs(f))
	}
	if err := w.Flush(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// manifest is a parsed manifest file: the field set it was written with, plus
// the recorded attributes of every entry keyed by path.
type manifest struct {
	fields  string
	f       fieldSet
	entries map[string]string
}

func readManifest(file string) (*manifest, error) {
	fp, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	m := &manifest{entries: make(map[string]string)}
	sc := bufio.NewScanner(fp)
	sc.Buffer(make([]byte, 0, 64<<10), manifestPathLimit)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Text()
		if lineNo == 1 {
			if !strings.HasPrefix(line, manifestHeader) {
				return nil, fmt.Errorf("%s is not a treesum manifest", file)
			}
			if m.fields, err = headerFields(line); err != nil {
				return nil, fmt.Errorf("%s: %s", file, err)
			}
			if m.f, _, err = parseFields(m.fields); err != nil {
				return nil, fmt.Errorf("%s: %s", file, err)
			}
			continue
		}
		encPath, attrs, ok := strings.Cut(line, " ")
		if !ok {
			return nil, fmt.Errorf("%s:%d: malformed entry", file, lineNo)
		}
		path, err := unescape(encPath)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %s", file, lineNo, err)
		}
		if _, dup := m.entries[path]; dup {
			return nil, fmt.Errorf("%s:%d: duplicate entry %s", file, lineNo, path)
		}
		m.entries[path] = attrs
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if lineNo == 0 {
		return nil, fmt.Errorf("%s is empty", file)
	}
	return m, nil
}

func headerFields(line string) (string, error) {
	for _, tok := range strings.Fields(line) {
		if v, ok := strings.CutPrefix(tok, "fields="); ok {
			return v, nil
		}
	}
	return "", errors.New("header has no fields= attribute")
}

// verify walks root and reports every difference against the manifest.
func verify(file, root string) error {
	m, err := readManifest(file)
	if err != nil {
		return err
	}
	entries, err := walk(root, m.f)
	if err != nil {
		return err
	}
	actual := make(map[string]string, len(entries))
	for i := range entries {
		actual[entries[i].path] = entries[i].attrs(m.f)
	}

	paths := make(map[string]struct{}, len(m.entries)+len(actual))
	for path := range m.entries {
		paths[path] = struct{}{}
	}
	for path := range actual {
		paths[path] = struct{}{}
	}

	var diffs []string
	for _, path := range sortedKeys(paths) {
		want, inManifest := m.entries[path]
		got, inTree := actual[path]
		switch {
		case !inTree:
			diffs = append(diffs, "missing: "+path)
		case !inManifest:
			diffs = append(diffs, "unexpected: "+path)
		case want != got:
			diffs = append(diffs, path+": "+diffAttrs(want, got))
		}
	}
	if len(diffs) == 0 {
		fmt.Printf("treesum: %s matches %s (%d entries)\n", root, file, len(m.entries))
		return nil
	}

	reported := diffs
	if len(reported) > maxReportedDiffs {
		reported = reported[:maxReportedDiffs]
	}
	for _, d := range reported {
		fmt.Fprintln(os.Stderr, "  "+d)
	}
	if len(diffs) > len(reported) {
		fmt.Fprintf(os.Stderr, "  ... and %d more\n", len(diffs)-len(reported))
	}
	noun := "differences"
	if len(diffs) == 1 {
		noun = "difference"
	}
	return fmt.Errorf("%d %s between %s and %s", len(diffs), noun, root, file)
}

// diffAttrs explains a mismatch attribute by attribute.
func diffAttrs(want, got string) string {
	w, g := parseAttrs(want), parseAttrs(got)
	keys := make(map[string]struct{}, len(w)+len(g))
	for k := range w {
		keys[k] = struct{}{}
	}
	for k := range g {
		keys[k] = struct{}{}
	}
	var parts []string
	for _, k := range sortedKeys(keys) {
		wv, inWant := w[k]
		gv, inGot := g[k]
		switch {
		case !inGot:
			parts = append(parts, fmt.Sprintf("%s: want %s, absent in tree", k, wv))
		case !inWant:
			parts = append(parts, fmt.Sprintf("%s: absent in manifest, got %s", k, gv))
		case wv != gv:
			parts = append(parts, fmt.Sprintf("%s: want %s got %s", k, wv, gv))
		}
	}
	return strings.Join(parts, "; ")
}

func parseAttrs(s string) map[string]string {
	m := make(map[string]string)
	for _, tok := range strings.Fields(s) {
		if k, v, ok := strings.Cut(tok, "="); ok {
			m[k] = v
		}
	}
	return m
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func lstat(path string) (*syscall.Stat_t, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("%s: unexpected stat type %T", path, fi.Sys())
	}
	return st, nil
}

func typeLetter(mode uint32) byte {
	switch mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		return 'd'
	case syscall.S_IFREG:
		return 'f'
	case syscall.S_IFLNK:
		return 'l'
	case syscall.S_IFCHR:
		return 'c'
	case syscall.S_IFBLK:
		return 'b'
	case syscall.S_IFIFO:
		return 'p'
	case syscall.S_IFSOCK:
		return 's'
	}
	return 'u'
}

// formatRdev decodes the Linux dev_t encoding of syscall.Stat_t.Rdev.
func formatRdev(rdev uint64) string {
	major := (rdev >> 8) & 0xfff
	minor := (rdev & 0xff) | ((rdev >> 12) & 0xfff00)
	return strconv.FormatUint(major, 10) + ":" + strconv.FormatUint(minor, 10)
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	// MD5 is a content checksum here, not a security digest: it only has to
	// tell two trees apart. The smoke tests hash the sparse fixture files
	// often enough that a slower digest is a measurable cost.
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// escape makes s safe as a single whitespace-separated manifest token: every
// byte outside printable ASCII, and every backslash, becomes \xNN. Tree paths
// may contain spaces, quotes and backslashes, so no byte can be taken literally.
func escape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c > 0x20 && c < 0x7f && c != '\\' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, `\x%02x`, c)
		}
	}
	return b.String()
}

func unescape(s string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			continue
		}
		if i+3 >= len(s) || s[i+1] != 'x' {
			return "", fmt.Errorf("invalid escape sequence in %q", s)
		}
		v, err := strconv.ParseUint(s[i+2:i+4], 16, 8)
		if err != nil {
			return "", fmt.Errorf("invalid escape sequence in %q", s)
		}
		b.WriteByte(byte(v))
		i += 3
	}
	return b.String(), nil
}
