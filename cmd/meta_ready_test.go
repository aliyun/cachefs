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
	"os"
	"syscall"
	"testing"

	cfsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/utils"
)

// TestSendMetaReady verifies that sendMetaReady writes the correct binary
// control message: cmd=CmdMetaReady (1027) with zero payload size.
func TestSendMetaReady(t *testing.T) {
	tests := []struct {
		name            string
		wantCmd         uint32
		wantPayloadSize uint32
		wantTotalBytes  int
	}{
		{
			name:            "encodes CmdMetaReady with zero payload",
			wantCmd:         cfsmeta.CmdMetaReady,
			wantPayloadSize: 0,
			wantTotalBytes:  8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wb := utils.NewBuffer(8)
			sendMetaReady(wb)

			b := wb.Bytes()
			if len(b) != tt.wantTotalBytes {
				t.Errorf("expected %d bytes, got %d", tt.wantTotalBytes, len(b))
				return
			}

			rb := utils.ReadBuffer(b)
			cmd := rb.Get32()
			if cmd != tt.wantCmd {
				t.Errorf("expected cmd %d (CmdMetaReady), got %d", tt.wantCmd, cmd)
			}
			payloadSize := rb.Get32()
			if payloadSize != tt.wantPayloadSize {
				t.Errorf("expected payload size %d, got %d", tt.wantPayloadSize, payloadSize)
			}
		})
	}
}

// TestMetaReadyControlFileProtocol tests the full .control file round-trip for
// the meta-ready command. os.Pipe is used to simulate the bidirectional
// communication between the cmd side and the VFS side.
//
// Layout:
//
//	cmd  --cmdW/cmdR-->  vfs    (cmd writes the 8-byte control message)
//	vfs  --respW/respR--> cmd   (vfs writes back a 1-byte errno response)
func TestMetaReadyControlFileProtocol(t *testing.T) {
	tests := []struct {
		name      string
		response  byte          // errno byte the simulated VFS sends back
		wantErrno syscall.Errno // errno expected by readProgress
	}{
		{
			name:      "success",
			response:  0,
			wantErrno: 0,
		},
		{
			name:      "ENOTSUP when peer meta not enabled",
			response:  byte(syscall.ENOTSUP & 0xff),
			wantErrno: syscall.ENOTSUP,
		},
		{
			name:      "EIO on internal error",
			response:  byte(syscall.EIO & 0xff),
			wantErrno: syscall.EIO,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// cmdR/cmdW: cmd side writes the control message, vfs side reads it.
			cmdR, cmdW, err := os.Pipe()
			if err != nil {
				t.Errorf("create command pipe: %v", err)
				return
			}
			defer cmdR.Close()
			defer cmdW.Close()

			// respR/respW: vfs side writes the errno response, cmd side reads it.
			respR, respW, err := os.Pipe()
			if err != nil {
				t.Errorf("create response pipe: %v", err)
				return
			}
			defer respR.Close()
			defer respW.Close()

			// Simulated VFS goroutine: reads the command, validates it, then
			// writes back the response errno.
			vfsDone := make(chan struct{})
			go func() {
				defer close(vfsDone)

				buf := make([]byte, 8)
				n, readErr := cmdR.Read(buf)
				if readErr != nil || n != 8 {
					t.Errorf("vfs read: got %d bytes, err %v", n, readErr)
					_, _ = respW.Write([]byte{byte(syscall.EIO & 0xff)})
					return
				}

				rb := utils.ReadBuffer(buf)
				if cmd := rb.Get32(); cmd != cfsmeta.CmdMetaReady {
					t.Errorf("vfs: expected cmd %d (CmdMetaReady), got %d", cfsmeta.CmdMetaReady, cmd)
				}
				if size := rb.Get32(); size != 0 {
					t.Errorf("vfs: expected payload size 0, got %d", size)
				}

				_, _ = respW.Write([]byte{tt.response})
			}()

			// cmd side: encode and send the meta-ready command.
			wb := utils.NewBuffer(8)
			sendMetaReady(wb)
			if _, err := cmdW.Write(wb.Bytes()); err != nil {
				t.Errorf("write command: %v", err)
				return
			}

			// cmd side: wait for the VFS response via readProgress.
			errno := readProgress(respR, func(_, _ uint64) {})
			<-vfsDone

			if errno != tt.wantErrno {
				t.Errorf("expected errno %v, got %v", tt.wantErrno, errno)
			}
		})
	}
}
