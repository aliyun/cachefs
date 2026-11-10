/*
 * Copyright 2023 Alibaba Cloud, Inc. or its affiliates.
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

package fuse

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/eryugey/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"

	"github.com/aliyun/cachefs/pkg/meta"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs"
)

const (
	DefTimeout = 86400 // one day

	// Defined as 57 in Linux kernel headers (include/uapi/linux/prctl.h)
	PR_SET_IO_FLUSHER = 57

	// Requires kernel feature support.
	// Enables self-managed page cache for Buffer I/O operations.
	//
	// `-o dontcache` equals to `-o dontcache=auto`,
	// for large IO chunks (greater than 128K), immediately release page cache
	// after read(2) completes.
	// Alternatively, the FUSE daemon can explicitly enable optimization by
	// setting the FOPEN_DONTCACHE (1 << 31) flag. Files with this flag
	// will bypass the IO size restriction, even for small IOs, page cache will be
	// immediately released after read(2) completes for FOPEN_DONTCACHE-opened files.
	//
	// `-o dontcache=advise` only applies to files opened with FOPEN_DONTCACHE flag.
	// For these files, the page cache will be immediately released after read(2) completes.
	selfManagedPageCache = "dontcache"
)

var logger = utils.GetLogger("cachefs")

type cachefs struct {
	fuse.RawFileSystem
	conf *vfs.Config
	c    *vfs.Cachefs
}

func newCacheFileSystem(conf *vfs.Config, c *vfs.Cachefs) *cachefs {
	return &cachefs{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		conf:          conf,
		c:             c,
	}
}

type setTimeout func(time.Duration)

func (fs *cachefs) replyAttr(ctx *fuseContext, entry *meta.Entry, attr *fuse.Attr, set setTimeout) {
	if vfs.IsSpecialNode(entry.Inode) {
		set(time.Hour)
	} else {
		set(fs.conf.AttrTimeout)
	}
	attrToStat(entry.Inode, entry.Attr, attr)
}

func (fs *cachefs) replyEntry(ctx *fuseContext, out *fuse.EntryOut, e *meta.Entry) fuse.Status {
	out.NodeId = uint64(e.Inode)
	out.Generation = 1

	if out.NodeId == 0 {
		out.SetEntryTimeout(fs.conf.NegativeTimeout)
	} else {
		if e.Attr.Typ == meta.TypeDirectory {
			out.SetEntryTimeout(fs.conf.DirEntryTimeout)
		} else {
			out.SetEntryTimeout(fs.conf.EntryTimeout)
		}
		fs.replyAttr(ctx, e, &out.Attr, out.SetAttrTimeout)
	}
	return 0
}

func (fs *cachefs) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (status fuse.Status) {
	ctx := newContext(cancel, header)
	defer releaseContext(ctx)
	entry, err := fs.c.Lookup(ctx, Ino(header.NodeId), name)
	if err != 0 {
		// In lookup, zero means negative entry. Returning ENOENT also means negative entry,
		// but by setting zero ino the kernel may cache negative entries for entry_timeout
		// seconds.
		if err == syscall.ENOENT && fs.conf.NegativeTimeout > 0 {
			entry := &meta.Entry{}
			return fs.replyEntry(ctx, out, entry)
		}
		return fuse.Status(err)
	}
	return fs.replyEntry(ctx, out, entry)
}

func (fs *cachefs) GetAttr(cancel <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	var opened uint8
	if in.Fh() != 0 {
		opened = 1
	}
	entry, err := fs.c.GetAttr(ctx, Ino(in.NodeId), opened)
	if err != 0 {
		return fuse.Status(err)
	}
	fs.replyAttr(ctx, entry, &out.Attr, out.SetTimeout)
	return 0
}

func (fs *cachefs) SetAttr(cancel <-chan struct{}, in *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	var opened uint8
	if in.Fh != 0 {
		opened = 1
	}
	entry, err := fs.c.SetAttr(ctx, Ino(in.NodeId), int(in.Valid), opened, in.Mode, in.Uid, in.Gid, int64(in.Atime), int64(in.Mtime), in.Atimensec, in.Mtimensec, in.Size)
	if err != 0 {
		return fuse.Status(err)
	}
	fs.replyAttr(ctx, entry, &out.Attr, out.SetTimeout)
	return 0
}

func (fs *cachefs) Mknod(cancel <-chan struct{}, in *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	entry, err := fs.c.Mknod(ctx, Ino(in.NodeId), name, uint16(in.Mode), getUmask(in), in.Rdev)
	if err != 0 {
		return fuse.Status(err)
	}
	return fs.replyEntry(ctx, out, entry)
}

func (fs *cachefs) Mkdir(cancel <-chan struct{}, in *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	entry, err := fs.c.Mkdir(ctx, Ino(in.NodeId), name, uint16(in.Mode), uint16(in.Umask))
	if err != 0 {
		return fuse.Status(err)
	}
	return fs.replyEntry(ctx, out, entry)
}

func (fs *cachefs) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	ctx := newContext(cancel, header)
	defer releaseContext(ctx)
	err := fs.c.Unlink(ctx, Ino(header.NodeId), name)
	return fuse.Status(err)
}

func (fs *cachefs) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	ctx := newContext(cancel, header)
	defer releaseContext(ctx)
	err := fs.c.Rmdir(ctx, Ino(header.NodeId), name)
	return fuse.Status(err)
}

func (fs *cachefs) Rename(cancel <-chan struct{}, in *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	err := fs.c.Rename(ctx, Ino(in.NodeId), oldName, Ino(in.Newdir), newName, in.Flags)
	return fuse.Status(err)
}

func (fs *cachefs) Link(cancel <-chan struct{}, in *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	entry, err := fs.c.Link(ctx, Ino(in.Oldnodeid), Ino(in.NodeId), name)
	if err != 0 {
		return fuse.Status(err)
	}
	return fs.replyEntry(ctx, out, entry)
}

func (fs *cachefs) Symlink(cancel <-chan struct{}, header *fuse.InHeader, target string, name string, out *fuse.EntryOut) (code fuse.Status) {
	ctx := newContext(cancel, header)
	defer releaseContext(ctx)
	entry, err := fs.c.Symlink(ctx, target, Ino(header.NodeId), name)
	if err != 0 {
		return fuse.Status(err)
	}
	return fs.replyEntry(ctx, out, entry)
}

func (fs *cachefs) Readlink(cancel <-chan struct{}, header *fuse.InHeader) (out []byte, code fuse.Status) {
	ctx := newContext(cancel, header)
	defer releaseContext(ctx)
	path, err := fs.c.Readlink(ctx, Ino(header.NodeId))
	return path, fuse.Status(err)
}

func (fs *cachefs) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (sz uint32, code fuse.Status) {
	ctx := newContext(cancel, header)
	defer releaseContext(ctx)
	value, err := fs.c.GetXattr(ctx, Ino(header.NodeId), attr, uint32(len(dest)))
	if err != 0 {
		return 0, fuse.Status(err)
	}
	copy(dest, value)
	return uint32(len(value)), 0
}

func (fs *cachefs) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	ctx := newContext(cancel, header)
	defer releaseContext(ctx)
	data, err := fs.c.ListXattr(ctx, Ino(header.NodeId), len(dest))
	if err != 0 {
		return 0, fuse.Status(err)
	}
	copy(dest, data)
	return uint32(len(data)), 0
}

func (fs *cachefs) SetXAttr(cancel <-chan struct{}, in *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	err := fs.c.SetXattr(ctx, Ino(in.NodeId), attr, data, in.Flags)
	return fuse.Status(err)
}

func (fs *cachefs) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) (code fuse.Status) {
	ctx := newContext(cancel, header)
	defer releaseContext(ctx)
	err := fs.c.RemoveXattr(ctx, Ino(header.NodeId), attr)
	return fuse.Status(err)
}

func (fs *cachefs) Create(cancel <-chan struct{}, in *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	entry, fh, err := fs.c.Create(ctx, Ino(in.NodeId), name, uint16(in.Mode), 0, in.Flags)
	if err != 0 {
		return fuse.Status(err)
	}
	out.Fh = fh
	return fs.replyEntry(ctx, &out.EntryOut, entry)
}

func (fs *cachefs) Open(cancel <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	entry, fh, err := fs.c.Open(ctx, Ino(in.NodeId), in.Flags)
	if err != 0 {
		return fuse.Status(err)
	}
	out.Fh = fh
	if vfs.IsSpecialNode(Ino(in.NodeId)) {
		out.OpenFlags |= fuse.FOPEN_DIRECT_IO
	} else if entry.Attr.KeepCache {
		out.OpenFlags |= fuse.FOPEN_KEEP_CACHE
	}
	return 0
}

func (fs *cachefs) Read(cancel <-chan struct{}, in *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	n, err := fs.c.Read(ctx, Ino(in.NodeId), buf, in.Offset, in.Fh)
	if err != 0 {
		return nil, fuse.Status(err)
	}
	return fuse.ReadResultData(buf[:n]), 0
}

func (fs *cachefs) Release(cancel <-chan struct{}, in *fuse.ReleaseIn) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	fs.c.Release(ctx, Ino(in.NodeId), in.Fh)
}

func (fs *cachefs) Write(cancel <-chan struct{}, in *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	err := fs.c.Write(ctx, Ino(in.NodeId), data, in.Offset, in.Fh)
	if err != 0 {
		return 0, fuse.Status(err)
	}
	return uint32(len(data)), 0
}

func (fs *cachefs) Flush(cancel <-chan struct{}, in *fuse.FlushIn) fuse.Status {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	err := fs.c.Flush(ctx, Ino(in.NodeId), in.Fh, in.LockOwner)
	return fuse.Status(err)
}

func (fs *cachefs) Fsync(cancel <-chan struct{}, in *fuse.FsyncIn) (code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	err := fs.c.Fsync(ctx, Ino(in.NodeId), int(in.FsyncFlags), in.Fh)
	return fuse.Status(err)
}

func (fs *cachefs) Fallocate(cancel <-chan struct{}, in *fuse.FallocateIn) (code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	err := fs.c.Fallocate(ctx, Ino(in.NodeId), uint8(in.Mode), int64(in.Offset), int64(in.Length), in.Fh)
	return fuse.Status(err)
}

func (fs *cachefs) CopyFileRange(cancel <-chan struct{}, in *fuse.CopyFileRangeIn) (written uint32, code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	copied, err := fs.c.CopyFileRange(ctx, Ino(in.NodeId), in.FhIn, in.OffIn, Ino(in.NodeIdOut), in.FhOut, in.OffOut, in.Len, uint32(in.Flags))
	if err != 0 {
		return 0, fuse.Status(err)
	}
	return uint32(copied), 0
}

func (fs *cachefs) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) (code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	l := in.Lk
	err := fs.c.Getlk(ctx, Ino(in.NodeId), in.Fh, in.Owner, &l.Start, &l.End, &l.Typ, &l.Pid)
	if err == 0 {
		out.Lk = l
	}
	return fuse.Status(err)
}

func (fs *cachefs) SetLk(cancel <-chan struct{}, in *fuse.LkIn) (code fuse.Status) {
	return fs.setLk(cancel, in, false)
}

func (fs *cachefs) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) (code fuse.Status) {
	return fs.setLk(cancel, in, true)
}

func (fs *cachefs) setLk(cancel <-chan struct{}, in *fuse.LkIn, block bool) (code fuse.Status) {
	if in.LkFlags&fuse.FUSE_LK_FLOCK != 0 {
		return fs.Flock(cancel, in, block)
	}
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	l := in.Lk
	err := fs.c.Setlk(ctx, Ino(in.NodeId), in.Fh, in.Owner, l.Start, l.End, l.Typ, l.Pid, block)
	return fuse.Status(err)
}

func (fs *cachefs) Flock(cancel <-chan struct{}, in *fuse.LkIn, block bool) (code fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	err := fs.c.Flock(ctx, Ino(in.NodeId), in.Fh, in.Owner, in.Lk.Typ, block)
	return fuse.Status(err)
}

func (fs *cachefs) OpenDir(cancel <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	fh, err := fs.c.Opendir(ctx, Ino(in.NodeId))
	out.Fh = fh
	return fuse.Status(err)
}

func (fs *cachefs) ReadDir(cancel <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	entries, _, err := fs.c.Readdir(ctx, Ino(in.NodeId), in.Size, int(in.Offset), in.Fh, false)
	var de fuse.DirEntry
	for _, e := range entries {
		de.Ino = uint64(e.Inode)
		de.Name = string(e.Name)
		de.Mode = e.Attr.SMode()
		if !out.AddDirEntry(de) {
			break
		}
	}
	return fuse.Status(err)
}

func (fs *cachefs) ReadDirPlus(cancel <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx := newContext(cancel, &in.InHeader)
	defer releaseContext(ctx)
	entries, readAt, err := fs.c.Readdir(ctx, Ino(in.NodeId), in.Size, int(in.Offset), in.Fh, true)
	ctx.start = readAt
	var de fuse.DirEntry
	for _, e := range entries {
		de.Ino = uint64(e.Inode)
		de.Name = string(e.Name)
		de.Mode = e.Attr.SMode()
		eo := out.AddDirLookupEntry(de)
		if eo == nil {
			break
		}
		if e.Attr.Full {
			fs.replyEntry(ctx, eo, e)
		} else {
			eo.Ino = uint64(e.Inode)
			eo.Generation = 1
		}
	}
	return fuse.Status(err)
}

var cancelReleaseDir = make(chan struct{})

func (fs *cachefs) ReleaseDir(in *fuse.ReleaseIn) {
	ctx := newContext(cancelReleaseDir, &in.InHeader)
	defer releaseContext(ctx)
	fs.c.Releasedir(ctx, Ino(in.NodeId), in.Fh)
}

func (fs *cachefs) StatFs(cancel <-chan struct{}, in *fuse.InHeader, out *fuse.StatfsOut) (code fuse.Status) {
	ctx := newContext(cancel, in)
	defer releaseContext(ctx)
	st, err := fs.c.StatFS(ctx, Ino(in.NodeId))
	if err != 0 {
		return fuse.Status(err)
	}
	out.NameLen = 255
	out.Frsize = 4096
	out.Bsize = 4096
	out.Blocks = st.Total / uint64(out.Bsize)
	if out.Blocks < 1 {
		out.Blocks = 1
	}
	out.Bavail = st.Avail / uint64(out.Bsize)
	out.Bfree = out.Bavail
	out.Files = st.Files
	out.Ffree = st.Favail
	return 0
}

// Serve starts a server to serve requests from FUSE.
func ServeCachefs(c *vfs.Cachefs, options string) error {
	if err := syscall.Setpriority(syscall.PRIO_PROCESS, os.Getpid(), -19); err != nil {
		logger.Warnf("setpriority: %s", err)
	}

	conf := c.Conf
	imp := newCacheFileSystem(conf, c)

	var opt fuse.MountOptions
	opt.FsName = "CacheFS:" + conf.Meta.Name
	opt.Name = "cachefs"
	opt.SingleThreaded = false
	opt.MaxBackground = 128
	opt.EnableLocks = true
	opt.DisableXAttrs = true
	opt.IgnoreSecurityLabels = true
	opt.MaxPages = fuse.MAX_MAX_PAGES
	opt.MaxReadAhead = 1 << 20
	opt.DirectMount = true
	opt.AllowOther = os.Getuid() == 0
	opt.ExtraCapabilities = fuse.CAP_ASYNC_DIO
	for _, n := range strings.Split(options, ",") {
		if n == "allow_other" || n == "allow_root" {
			opt.AllowOther = true
		} else if n == "nonempty" {
		} else if n == "debug" {
			opt.Debug = true
		} else if n == "writeback_cache" || n == "writeback" {
			logger.Warn("Readonly filesystem, disable writeback cache")
		} else if strings.TrimSpace(n) != "" {
			if n == selfManagedPageCache {
				if err := handleSelfManagedPageCache(); err != nil {
					logger.Warnf("Failed to handle FUSE mount option %s", selfManagedPageCache)
					continue
				}
			}
			opt.Options = append(opt.Options, n)
		}
	}
	opt.Options = append(opt.Options, "default_permissions")
	fssrv, err := fuse.NewServer(imp, conf.Meta.MountPoint, &opt)
	if err != nil {
		return fmt.Errorf("fuse: %s", err)
	}

	c.SetMounted()
	fssrv.Serve()
	return nil
}

func setIOFlusher() error {
	err := unix.Prctl(PR_SET_IO_FLUSHER, 1, 0, 0, 0)
	if err != nil {
		logger.Warnf("prctl failed: %v", err)
		return err
	}
	logger.Info("PR_SET_IO_FLUSHER set successfully")

	return nil
}

// checkFuseFlag checks if the running kernel FUSE implementation
// supports the given mount flag (requires fsopen/fsconfig syscalls, Linux 5.2+).
func checkFuseFlag(flag string) error {
	fd, err := unix.Fsopen("fuse", unix.FSOPEN_CLOEXEC)
	if err != nil {
		return fmt.Errorf("fsopen(\"fuse\") failed: %w", err)
	}
	defer unix.Close(fd)

	if err := unix.FsconfigSetFlag(fd, flag); err != nil {
		return fmt.Errorf("flag %q not supported or fsconfig failed: %w", flag, err)
	}

	return nil
}

func handleSelfManagedPageCache() error {
	if err := checkFuseFlag(selfManagedPageCache); err != nil {
		logger.Warnf("unsupported FUSE option %s, drop it: %v", selfManagedPageCache, err)
		return err
	}

	// To enable FUSE cache write bandwidth 10GB optimization, the PR_SET_IO_FLUSHER
	// prctl(2) must be executed. Read bandwidth 40GB optimization depends on the
	// write bandwidth 10GB optimization being enabled.
	return setIOFlusher()
}

func getUmask(in *fuse.MknodIn) uint16 {
	return uint16(in.Umask)
}

func setBlksize(out *fuse.Attr, size uint32) {
	out.Blksize = size
}
