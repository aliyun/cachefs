import os
import subprocess
import fcntl
import struct
import sys

mnt = "/mnt/peer_cachefsci"
if len(sys.argv) > 1:
        mnt = sys.argv[1]
sample_files = [f for f in os.listdir(mnt) if f.endswith("test")]
sample_file = os.path.join(mnt, sample_files[0]) if sample_files else "/mnt/cachefs/linktest"
xattr_name = "user.test_attr"
xattr_value = b"test_value"

def safe_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        print(f"[IGNORED ERROR] {func.__name__}: {e}")
        return None

def basic_ops():
    print(f"Using sample file: {sample_file}")
    safe_call(os.stat, sample_file)
    safe_call(os.listdir, mnt)
    safe_call(os.readlink, sample_file)  # May fail if not symlink
    try:
        with open(sample_file, "r") as f:
            safe_call(f.read)
    except Exception as e:
        print(f"[IGNORED ERROR] read: {e}")

def open_read_flush():
    try:
        fd = os.open(sample_file, os.O_RDONLY)
        safe_call(os.read, fd, 4)
        safe_call(os.fsync, fd)
        safe_call(os.close, fd)
    except Exception as e:
        print(f"[IGNORED ERROR] open_read: {e}")

def xattr_ops():
    for cmd in [
        ["setfattr", "-n", xattr_name, "-v", xattr_value.decode(), sample_file],
        ["getfattr", "-n", xattr_name, sample_file],
        ["getfattr", "-d", sample_file],
        ["setfattr", "-x", xattr_name, sample_file],
    ]:
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"[IGNORED ERROR] xattr: {' '.join(cmd)} -> {e}")

def locking_ops():
    try:
        fd = os.open(sample_file, os.O_RDONLY)
        safe_call(fcntl.flock, fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        safe_call(fcntl.flock, fd, fcntl.LOCK_UN)

        # simulate fcntl F_GETLK
        flock_struct = struct.pack("hhqqi", fcntl.F_WRLCK, 0, 0, 0, 0)
        try:
            fcntl.fcntl(fd, fcntl.F_GETLK, flock_struct)
        except Exception as e:
            print(f"[IGNORED ERROR] fcntl F_GETLK: {e}")

        safe_call(os.close, fd)
    except Exception as e:
        print(f"[IGNORED ERROR] locking_ops: {e}")

def statfs_call():
    try:
        stat = os.statvfs(mnt)
        print(f"Statfs: block size = {stat.f_bsize}, blocks = {stat.f_blocks}")
    except Exception as e:
        print(f"[IGNORED ERROR] statfs_call: {e}")

def copy_file_range_emulate():
    try:
        with open(sample_file, 'rb') as src:
            while True:
                data = src.read(1024)
                if not data:
                    break
                # discard write due to read-only
                _ = data
    except Exception as e:
        print(f"[IGNORED ERROR] copy_file_range: {e}")

if __name__ == "__main__":
    print("🔧 Triggering all FUSE methods (read-only fallback, all errors ignored)...")
    basic_ops()
    open_read_flush()
    xattr_ops()
    locking_ops()
    statfs_call()
    copy_file_range_emulate()
    print("✅ Done. FUSE methods should have been triggered at least once.")
