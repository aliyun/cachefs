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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Parses paths from r, one per line. Empty lines and lines starting with '#' are skipped.
func ParsePathsFromReader(r io.Reader) ([]string, error) {
	var paths []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		if path := scanner.Text(); path != "" {
			if strings.HasPrefix(path, "#") {
				continue
			}
			paths = append(paths, path)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return paths, nil
}

// Reads a path list from a file, one per line. Empty lines and '#' comment lines are skipped.
func ReadPathsFromFile(fname string) ([]string, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ParsePathsFromReader(file)
}

// Executes cmdStr and parses its stdout as a path list (one per line, '#' comments and empty lines
// skipped). The command string is split on whitespace into program and arguments.  A positive
// timeout limits the command's execution time; zero or negative means no timeout.  name is a
// human-readable label (e.g. "Pin cache") used to prefix log messages.
//
// On success it also returns the command's stdout (trailing newlines trimmed, or "<empty>" when
// there is none) and how long the command took.
func ReadPathsFromCmd(name, cmdStr string, timeout time.Duration) ([]string, string, time.Duration, error) {
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		logger.Warnf("%s command %q is empty", name, cmdStr)
		return nil, "", 0, fmt.Errorf("empty command")
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()
	cmd := exec.CommandContext(ctx, parts[0], parts[1:]...)
	cmd.WaitDelay = 2 * time.Second

	// Capture stdout/stderr into our own buffers rather than using cmd.Output(), so that
	// whatever the command printed is available even on timeout (where the error is ctx.Err()
	// and cmd.Output() would surface neither partial stdout nor stderr).
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	start := time.Now()
	err := cmd.Run()
	elapsed := time.Since(start)
	outStr := fmtOutput(stdout.Bytes())
	if err != nil {
		errStr := fmtOutput(stderr.Bytes())
		if ctx.Err() != nil {
			logger.Warnf("%s command %q timed out after %s, stdout: %s, stderr: %s",
				name, cmdStr, elapsed, outStr, errStr)
			return nil, "", 0, ctx.Err()
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			logger.Warnf("%s command %q failed after %s: exit code %d, stdout: %s, stderr: %s",
				name, cmdStr, elapsed, exitErr.ExitCode(), outStr, errStr)
		} else {
			logger.Warnf("%s command %q failed after %s: %s, stdout: %s, stderr: %s",
				name, cmdStr, elapsed, err, outStr, errStr)
		}
		return nil, "", 0, err
	}
	paths, err := ParsePathsFromReader(bytes.NewReader(stdout.Bytes()))
	if err != nil {
		return nil, "", 0, err
	}
	return paths, outStr, elapsed, nil
}

// Formats command output for logging: trims trailing newlines, or returns "<empty>" when there is
// nothing to show.
func fmtOutput(b []byte) string {
	b = bytes.TrimRight(b, "\n")
	if len(b) == 0 {
		return "<empty>"
	}
	return string(b)
}

// Return true if two string sets contain the same elements.
func StringSetEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// Convert a string slice to a set (map[string]struct{}), deduplicating entries.
func ToStringSet(paths []string) map[string]struct{} {
	s := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		s[p] = struct{}{}
	}
	return s
}
