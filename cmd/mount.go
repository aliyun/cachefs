/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs/common"
)

func installHandler(mp string) {
	// Go will catch all the signals
	signal.Ignore(syscall.SIGPIPE)
	signalChan := make(chan os.Signal, 10)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		for {
			sig := <-signalChan
			logger.Infof("Received signal %s, exiting...", sig.String())
			go func() { _ = utils.Unmount(mp, true) }()
			go func() {
				time.Sleep(time.Second * time.Duration(gracePeriod))
				logger.Warnf("Umount not finished after %d seconds, force exit", gracePeriod)
				os.Exit(1)
			}()
		}
	}()
}

func prepareMp(mp string) {
	fi, err := os.Stat(mp)
	if err != nil {
		if err := os.MkdirAll(mp, 0777); err != nil {
			if os.IsExist(err) {
				// a broken mount point, umount it and continue to mount
				_ = utils.Unmount(mp, true)
				return
			}
			logger.Fatalf("create %s: %s", mp, err)
		}
		return
	}
	if fi.Size() == 0 {
		// a broken mount point, umount it and continue to mount
		_ = utils.Unmount(mp, true)
	}

	return
}

func checkMountpoint(name, mp, logPath string, background bool) {
	for i := 0; i < 20; i++ {
		time.Sleep(time.Millisecond * 500)
		if err := common.ValidateMountPoint(mp); err == nil {
			logger.Infof("\033[92mOK\033[0m, %s is ready at %s", name, mp)
			return
		}
		_, _ = os.Stdout.WriteString(".")
		_ = os.Stdout.Sync()
	}
	_, _ = os.Stdout.WriteString("\n")
	if background {
		logger.Fatalf("The mount point is not ready in 10 seconds, please check the log (%s) or re-mount in foreground", logPath)
	} else {
		logger.Fatalf("The mount point is not ready in 10 seconds, exit it")
	}
}

func disableUpdatedb() {
	path := "/etc/updatedb.conf"
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	fstype := "fuse.cachefs"
	if bytes.Contains(data, []byte(fstype)) {
		return
	}
	// assume that fuse.sshfs is already in PRUNEFS
	knownFS := "fuse.sshfs"
	p1 := bytes.Index(data, []byte("PRUNEFS"))
	p2 := bytes.Index(data, []byte(knownFS))
	if p1 > 0 && p2 > p1 {
		var nd []byte
		nd = append(nd, data[:p2]...)
		nd = append(nd, fstype...)
		nd = append(nd, ' ')
		nd = append(nd, data[p2:]...)
		err = os.WriteFile(path, nd, 0644)
		if err != nil {
			logger.Warnf("update %s: %s", path, err)
		} else {
			logger.Infof("Add %s into PRUNEFS of %s", fstype, path)
		}
	}
}
