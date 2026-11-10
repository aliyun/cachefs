/*
 * JuiceFS, Copyright 2018 Juicedata, Inc.
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
	"errors"
	"os/exec"
)

func Unmount(mp string, force bool) error {
	var cmd *exec.Cmd
	if _, err := exec.LookPath("fusermount"); err == nil {
		if force {
			cmd = exec.Command("fusermount", "-uz", mp)
		} else {
			cmd = exec.Command("fusermount", "-u", mp)
		}
	} else {
		if force {
			cmd = exec.Command("umount", "-l", mp)
		} else {
			cmd = exec.Command("umount", mp)
		}
	}
	out, err := cmd.CombinedOutput()
	if err != nil && len(out) != 0 {
		err = errors.New(string(out))
	}
	return err
}
