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

package cmd

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/vfs/apiclient"
	"github.com/stretchr/testify/assert"
)

func TestApiClient(t *testing.T) {
	srcDir := t.TempDir()
	mntDir := t.TempDir()
	sockDir := t.TempDir()
	sockFile := filepath.Join(sockDir, "test-api.sock")
	args := []string{"", "cache", "--source-dir", srcDir, "--cache-port", "8900", "--serf-port", "8800", "--serf-node-name", "testnode", "--enable-api-server", "--api-server-sock", sockFile, "--serf-leave-delay", "0", "testcachefs", mntDir}

	go func() {
		err := Main(args)
		if err != nil {
			t.Errorf("Start test cachefs: %s", err)
			return
		}
	}()
	time.Sleep(time.Millisecond * 500)

	clientArgs := []string{"", "apiclient", "getpid", "--api-server-sock", sockFile}
	err := Main(clientArgs)
	assert.NoError(t, err)

	clientArgs = []string{"", "apiclient", "getpid", "--api-server-sock", "nosuchsock"}
	err = Main(clientArgs)
	assert.Error(t, err)

	clientArgs = []string{"", "apiclient", "server-info", "--api-server-sock", sockFile}
	err = Main(clientArgs)
	assert.NoError(t, err)

	clientArgs = []string{"", "apiclient", "server-info", "--api-server-sock", "nosuchsock"}
	err = Main(clientArgs)
	assert.Error(t, err)

	clientArgs = []string{"", "apiclient", "getpeers", "--api-server-sock", sockFile}
	err = Main(clientArgs)
	assert.NoError(t, err)

	clientArgs = []string{"", "apiclient", "getpeers", "--raw", "--api-server-sock", sockFile}
	err = Main(clientArgs)
	assert.NoError(t, err)

	clientArgs = []string{"", "apiclient", "getpeers", "--port", "--api-server-sock", sockFile}
	err = Main(clientArgs)
	assert.NoError(t, err)

	clientArgs = []string{"", "apiclient", "getpeers", "--api-server-sock", "nosuchsock"}
	err = Main(clientArgs)
	assert.Error(t, err)

	// clear: no paths and no --clear-burst → error (connected to running server, fails on path validation)
	clientArgs = []string{"", "apiclient", "clear", "--api-server-sock", sockFile}
	err = Main(clientArgs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one path")

	// clear --clear-burst: should succeed against the running server
	clientArgs = []string{"", "apiclient", "clear", "--clear-burst", "--api-server-sock", sockFile}
	err = Main(clientArgs)
	assert.NoError(t, err)

	// clear --clear-burst: invalid socket → connection error
	clientArgs = []string{"", "apiclient", "clear", "--clear-burst", "--api-server-sock", "nosuchsock"}
	err = Main(clientArgs)
	assert.Error(t, err)

	// set-cache: no flags → keep current values (no-op), should succeed
	clientArgs = []string{"", "apiclient", "set-cache", "--api-server-sock", sockFile}
	err = Main(clientArgs)
	assert.NoError(t, err)

	// set-cache: valid args against running server → success
	clientArgs = []string{"", "apiclient", "set-cache", "--cache-size", "100", "--api-server-sock", sockFile}
	err = Main(clientArgs)
	assert.NoError(t, err)

	// set-cache: invalid socket → connection error
	clientArgs = []string{"", "apiclient", "set-cache", "--cache-size", "100", "--api-server-sock", "nosuchsock"}
	err = Main(clientArgs)
	assert.Error(t, err)

	// set-cache --restore-burst-cache: burst was never enabled on this server,
	// restore is a no-op and should succeed
	clientArgs = []string{"", "apiclient", "set-cache", "--restore-burst-cache", "--api-server-sock", sockFile}
	err = Main(clientArgs)
	assert.NoError(t, err)

	// set-cache --restore-burst-cache with --burst-cache-size → conflict error
	clientArgs = []string{"", "apiclient", "set-cache", "--restore-burst-cache", "--burst-cache-size", "400", "--api-server-sock", sockFile}
	err = Main(clientArgs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "--restore-burst-cache conflicts with --burst-cache-size")

	// set-cache --restore-burst-cache: invalid socket → connection error
	clientArgs = []string{"", "apiclient", "set-cache", "--restore-burst-cache", "--api-server-sock", "nosuchsock"}
	err = Main(clientArgs)
	assert.Error(t, err)

	clientArgs = []string{"", "apiclient", "umount", "--api-server-sock", "nosuchsock"}
	err = Main(clientArgs)
	assert.Error(t, err)

	clientArgs = []string{"", "apiclient", "umount", "--api-server-sock", sockFile}
	err = Main(clientArgs)
	assert.NoError(t, err)
	time.Sleep(time.Millisecond * 500)
}

func TestGetfileRangeTasks(t *testing.T) {
	producer, err := newRangeProducer(10, 4)
	assert.NoError(t, err)
	var tasks []rangeTask
	for {
		task, ok := producer.nextTask()
		if !ok {
			break
		}
		tasks = append(tasks, task)
	}
	assert.Equal(t, []rangeTask{
		{start: 0, length: 4},
		{start: 4, length: 4},
		{start: 8, length: 2},
	}, tasks)

	empty, err := newRangeProducer(0, 4)
	assert.NoError(t, err)
	_, ok := empty.nextTask()
	assert.False(t, ok)

	_, err = newRangeProducer(10, 0)
	assert.ErrorContains(t, err, "range length must be positive")
}

func TestRunRangesUsesReaderInSourceOrder(t *testing.T) {
	source := []byte("0123456789")
	var ranges []apiclient.FileRange
	releases := 0
	reader := func(_ string, fileRange *apiclient.FileRange) (getfileRead, error) {
		ranges = append(ranges, *fileRange)
		start := int(fileRange.Start)
		end := start + int(fileRange.Length)
		return getfileRead{
			data: source[start:end],
			release: func() error {
				releases++
				return nil
			},
		}, nil
	}
	var output bytes.Buffer

	err := runRanges(reader, "/file", int64(len(source)), 4, make(chan struct{}, 1), 1, &output)

	assert.NoError(t, err)
	assert.Equal(t, "0123456789", output.String())
	assert.Equal(t, []apiclient.FileRange{
		{Start: 0, Length: 4},
		{Start: 4, Length: 4},
		{Start: 8, Length: 2},
	}, ranges)
	assert.Equal(t, 3, releases)
}

func TestRangeWorkerCount(t *testing.T) {
	assert.Equal(t, min(rangeWorkerLimit(), 3), rangeWorkerCount(10, 4))
	assert.LessOrEqual(t, rangeWorkerLimit(), maxRangeWorkers)
}

func TestAcquireRangeTask(t *testing.T) {
	t.Run("cancelled wait keeps queue head", func(t *testing.T) {
		producer, err := newRangeProducer(10, 4)
		assert.NoError(t, err)
		limiter := make(chan struct{}, 1)
		limiter <- struct{}{}
		done := make(chan struct{})
		close(done)

		_, ok := acquireRangeTask(producer, limiter, done)
		assert.False(t, ok)
		task, ok := producer.nextTask()
		assert.True(t, ok)
		assert.Equal(t, rangeTask{start: 0, length: 4}, task)
	})

	t.Run("empty producer refunds permit", func(t *testing.T) {
		producer, err := newRangeProducer(0, 4)
		assert.NoError(t, err)
		limiter := make(chan struct{}, 1)
		done := make(chan struct{})

		_, ok := acquireRangeTask(producer, limiter, done)
		assert.False(t, ok)
		select {
		case limiter <- struct{}{}:
		default:
			t.Fatal("range permit was not refunded")
		}
	})
}
