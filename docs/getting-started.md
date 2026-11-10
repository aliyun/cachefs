# Getting started

This page gets a mount running: one node, then a two-node cluster, with the
commands to check each step. [Architecture](architecture.md) explains how it
all works and ends with a glossary of the terms used here.

## Before you start

Five terms cover everything the commands below mention:

- **Source directory** — the POSIX directory being mirrored: an NFS mount, an
  object-storage mount, or a local directory. CacheFS never writes to it.
- **Mount point** — the read-only FUSE filesystem CacheFS creates; applications
  read it like any other directory.
- **Cache backend** — where this node keeps blocks: a directory
  (`--cache-dir /path`) or host memory (`--cache-dir memory`, the default),
  capped by `--cache-size` in MiB.
- **Cluster** — the nodes that share one cache, joined by serf gossip
  (`--serf-join`); there is no master node.
- **Preload** — fetching paths into the cache ahead of demand; `--broadcast`
  makes every node in the cluster do it.

Two assumptions underlie all of it:

- The source directory's contents are expected not to change while it is
  served; metadata is re-validated only as often as `--readdir-cache` asks,
  which by default is never.
- Every node in a cluster serves the same source contents. Cache keys are
  paths relative to the source root, so the local `--source-dir` path may
  differ between nodes, but the tree and the data must not.

## Prerequisites

- Linux on x86_64 or aarch64.
- Root, or `CAP_SYS_ADMIN` with access to `/dev/fuse`, to create the mount.
- A built binary (see the [README](../README.md#build-and-development)).

## Single node

Mount `/mnt/models` — whatever backs it — at `/mnt/cachefs`, caching into host
memory:

```bash
sudo ./cachefs cache --source-dir /mnt/models --cache-dir memory cachefs /mnt/cachefs
```

The first positional argument (`cachefs` above) is the filesystem name, a label
used in logs and metrics; the second is the mount point. The command runs in
the foreground; add `-d` to daemonize.

Check that it works:

```bash
ls -l /mnt/cachefs                 # the source tree, read-only
cachefs stats /mnt/cachefs         # live counters, refreshed every second
cat /mnt/cachefs/.stats | head     # the same counters as Prometheus text
curl -s 127.0.0.1:9678/metrics     # the metrics endpoint (default address)
```

`cachefs stats` prints a column set chosen by `--schema` (default
`tufmpcsS`: time, usage, FUSE, metadata, block pool, cache, source, server);
`-l` raises verbosity.

Reads that hit the cache are served locally; reads that miss go to the source
directory and are cached on the way through, so a second read of the same data
is local.

To limit how much memory the cache may take, set `--cache-size` in MiB:

```bash
sudo ./cachefs cache --source-dir /mnt/models --cache-dir memory \
    --cache-size 32768 cachefs /mnt/cachefs
```

Unmount with the usual command:

```bash
sudo umount /mnt/cachefs
```

## Two-node cluster

Start the first node exactly as above. On the second node, point serf at the
first one:

```bash
sudo ./cachefs cache --source-dir /mnt/models --cache-dir memory \
    --serf-join <node-a-address> cachefs /mnt/cachefs
```

`--serf-join` takes one or more space-separated `host[:port]` values; the port
defaults to 7946. From then on the two nodes form one cluster: each gossips
membership and announces which blocks it caches, so a miss on one node resolves
to the other before either touches the source directory.

Both nodes must serve the same source contents: a block cached on one node is
offered to the other under its path relative to the source root, so differing
trees would serve wrong data. The local path of `--source-dir` may differ
between nodes; the contents must not.

Confirm they see each other:

```bash
curl -s 127.0.0.1:9678/serf_members    # on either node
curl -s 127.0.0.1:9678/peers           # peers this node will serve from / pull from
```

Reading a file on node B that node A already cached transfers the blocks from
A over the data plane (TCP by default; `--transport rdma` where the fabric
supports it) instead of re-reading the source. Adding more nodes is the same
one-line join against any existing member.

## Preload and clear

Pull a model into the local cache before anyone reads it:

```bash
./cachefs preload /mnt/cachefs/qwen2.5-72b
```

Paths are inside the mount point. Preload fills from peers first and only reads
the source for blocks nobody has. `--broadcast` asks every node in the cluster
to preload the same paths, which is what you want before a scale-out:

```bash
./cachefs preload --broadcast /mnt/cachefs/qwen2.5-72b
```

Drop cached data again with `cachefs clear [PATH ...]` (and `--clear-burst` to
also shrink a burst cache back to `--cache-size`).

## Observing a mount

`cachefs stats MOUNTPOINT` reads the mount's Prometheus metrics and renders
them as a refreshing table: FUSE operation rates, metadata cache state, block
pool and cache hit/miss counters, source-read rates, and the counters for
serving other nodes. `--schema` chooses the column groups (default `tufmpcsS`:
time, usage, FUSE, metadata, block pool, cache, source, server; `g` adds the Go
runtime), `--interval` the refresh period and `-l` the verbosity. It also
accepts a metrics URL instead of a mount point —
`cachefs stats 127.0.0.1:9678/metrics` — which is how you inspect a
`cachefs daemon` process.

`cachefs profile MOUNTPOINT` reads the FUSE access log and reports, per
interval, how many of each operation completed and their combined latency,
optionally filtered to specific consumers with `--uid`, `--gid` or `--pid`. It
works live on a mount or replays a saved `.accesslog` file, and `--interval 0`
collapses the whole log into a single summary.

Both tools read what the mount already exposes: the same counters are served as
Prometheus text at `/metrics` (default `127.0.0.1:9678`) and as the in-mount
`.stats` file, and `.config` dumps the effective configuration as JSON, so
anything visible here can also be scraped or diffed directly.

## Further reading

- [Architecture](architecture.md) — what happens inside a read, how the cache
  decides what to keep, and the other deployment shapes (sourceless nodes,
  fuse/store separation, CacheRoot).
