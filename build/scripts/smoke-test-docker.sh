#!/bin/bash -e

here=$(pwd)
workdir=/go/src/github.com/aliyun/cachefs
DEBUG=${DEBUG:-"false"}
source_dir=/tmp/source
sourceless_dir=/tmp/sourceless
gocover_dir=/tmp/gocover
cache_dir=/tmp/cache_dir
peer_cache_dir=/tmp/peer_cache_dir
main_log=/tmp/cachefs.log
peer_log=/tmp/peer.log
stats_log=/tmp/stats.log
preload_log=/tmp/preload.log
clear_log=/tmp/clear.log
profile_log=/tmp/profile.log
cacheroot_log=/tmp/cacheroot.log
mnt=/mnt/cachefsci
options="--cache-nic lo --cache-port 7788 --serf-port 7799 \
--liveness 127.0.0.1:7955 --serf-leave-delay 0 --free-space-ratio 0.01"
server_opt=" --api-server-sock /run/cachefsci-api.sock"
peer_mnt=/mnt/peer_cachefsci
peer_options="--cache-nic lo --serf-node-name test-peer --free-space-ratio 0.01 \
--cache-port 7787 --serf-port 7798 --serf-join 127.0.0.1:7799 \
--liveness 127.0.0.1:7956 --serf-leave-delay 0"
server_opt2=" --api-server-sock /run/cachefsci-peer.sock"
curl="curl -sf --noproxy localhost"

DB_IDX=0

cleanup()
{
	umount /mnt/cachefsci || umount -l /mnt/cachefsci || true
	umount /mnt/peer_cachefsci || umount -l /mnt/peer_cachefsci || true
}

smoke_result=0
trap "cleanup" 1 2 3 15

arch=$(uname -m)

log()
{
	echo -e "$(date +"%F %T.%N"): $@"
}

install_treesum()
{
	pushd $here
	go build -o /usr/local/bin/treesum ./build/src/treesum
	popd
}

wait_mount()
{
	local mnt=$1
	local cnt=0
	while true; do
		if mount | grep -q $mnt; then
			break
		elif [ $cnt -lt 10 ]; then
			sleep 1
			cnt=$((cnt+1)) || true
		else
			log "Cachefs not mounted on $mnt in 10s, abort"
			exit 1
		fi
	done
}

wait_daemon()
{
	local addr=$1
	local cnt=0
	while true; do
		if $curl $addr; then
			break
		elif [ $cnt -lt 10 ]; then
			sleep 1
			cnt=$((cnt+1)) || true
		else
			log "Cachefs daemon not ready in 10s, abort"
			exit 1
		fi
	done
}

ls_loop() {
	while true; do
		ls -l $1 > /dev/null 2>&1
	done
}

# read_loop reads a file repeatedly until the flag file is removed. It is stopped
# by removing the flag rather than by a signal, so it always finishes the read in
# progress and leaves no fd open on the mount for the caller to trip over when
# unmounting.
read_loop() {
	local file=$1
	local flag=$2
	while [ -e $flag ]; do
		cat $file > /dev/null 2>&1
	done
}

concurrent_ls()
{
	local options="--cache-nic lo --cache-port 7788 --serf-port 7799 --liveness 127.0.0.1:7955 --serf-leave-delay 0"
	local NUM_THREADS=16
	local DURATION=5
	local PIDS=()

	#
	# test concurrent ls with readdir cache = 1
	#
	log "Cachefs - concurrent ls with readdir cache = 1"
	$here/cachefs cache $options --source-dir $source_dir --cache-size 1024 \
		--readdir-cache 1 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	for ((i=1; i<=$NUM_THREADS; i++)); do
		# assume fsmark/00 exists
		ls_loop $mnt/fsmark/00 &

		PIDS+=($!)
		echo "Started Process $i (PID: $!)"
	done

	log "Running test for $DURATION seconds..."
	sleep "$DURATION"
	log "Test time elapsed. Killing $NUM_THREADS processes: ${PIDS[@]}"
	kill -TERM "${PIDS[@]}" 2>/dev/null
	log "All $NUM_THREADS ls processes finished. Test completed."
	run_test "concurrent_ls_readdir_cache_1" 1 grep -w panic $main_log

	umount $mnt
	kill $pid || true
	wait $pid
}

reload_keep_multi_prefix()
{
	#
	# test reload cache with multiple kept subpaths
	#
	log "Cachefs - reload cache with multiple kept subpaths"
	rm -rf /tmp/remove_keep_src /tmp/remove_keep_subpaths.txt
	mkdir -p /tmp/remove_keep_src/drop
	mkdir -p /tmp/remove_keep_src/keep1 /tmp/remove_keep_src/keep2 /tmp/remove_keep_src/keep3 \
		/tmp/remove_keep_src/keep4 /tmp/remove_keep_src/keep5
	for i in $(seq 1 64); do
		truncate -s 1048576 /tmp/remove_keep_src/drop/file_$i
		xfs_io -c "pwrite 0 4096" /tmp/remove_keep_src/drop/file_$i >/dev/null 2>&1
	done
	touch /tmp/remove_keep_src/keep1/file /tmp/remove_keep_src/keep2/file /tmp/remove_keep_src/keep3/file \
		/tmp/remove_keep_src/keep4/file /tmp/remove_keep_src/keep5/file
	echo "" > /tmp/remove_keep_subpaths.txt
	export GOGC=1 GOMEMLIMIT=64MiB; $here/cachefs cache $options --source-dir /tmp/remove_keep_src --cache-dir memory --cache-size 2048 cachefs $mnt \
		--source-subpath-file /tmp/remove_keep_subpaths.txt --source-subpath-reload 1 --readdir-cache 0 > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	sleep 2
	run_test "reload_keep_multi_prefix_preload_drop" 0 $here/cachefs preload $mnt/drop
	check_stat_value "reload_keep_multi_prefix_cache_blocks" "cachefs_cache_blocks" ">" 0 $mnt/.stats
	cat > /tmp/remove_keep_subpaths.txt <<EOF
/keep1
/keep2
/keep3
/keep4
/keep5
EOF
	sleep 3
	run_test "reload_keep_multi_prefix_evict_log" 0 \
		'grep "subpath restricted from allow-all" $main_log'
	check_stat_value "reload_keep_multi_prefix_evict_not_repeated" \
		"cachefs_blockcache_evicts_removebyprefix" "<" 128 $mnt/.stats
	sleep 3
	run_test "reload_keep_multi_prefix_refcount_log_check" 1 'grep "refcount of page" $main_log'
	umount $mnt
	run_test "reload_keep_multi_prefix_log_check" 1 grep -e ERROR -e FATAL $main_log
	kill $pid || true
	wait $pid
	rm -rf /tmp/remove_keep_src /tmp/remove_keep_subpaths.txt
}

regression_test(){
	concurrent_ls
	reload_keep_multi_prefix
}

# block_pool_sanity exercises the shared idle block pool and checks that
# asynchronous prefault starts before the first read while recycled blocks never
# serve stale bytes on either the fuse read path or vfs API GetFileChunks path.
#
# The API path is the interesting one: the client mmaps the daemon's memfd
# blocks and holds the mapping, so a block recycled while still mapped rewrites
# memory the client is reading. That is why the getfile rounds below run
# against a concurrent read loop that keeps evicting - and thus freeing into the
# pool - the whole time.
block_pool_sanity()
{
	local bp_src=/tmp/block_pool_src
	local bp_sum=/tmp/block_pool.sum
	local bp_out=/tmp/block_pool_out
	local bp_opt="--api-server-sock /run/cachefsci-blockpool.sock"
	local bp_paths="$mnt/files/f1 $mnt/files/f2 $mnt/files/f3 $mnt/files/f4"
	# --buffer-size 32 over the 4MiB block = 8 slots, so residency
	# never exceeds 32MiB.
	local bp_slots=8
	local bp_cap=$((32 * 1024 * 1024))
	local churn_pid churn_flag

	log "Cachefs - block pool: generate source with distinct data in every block"
	gen_block_pool_src $bp_src
	treesum -fields ugomcd -w $bp_sum $bp_src

	#
	# block pool enabled
	#
	# --cache-size is a fraction of the data read below, so eviction frees
	# blocks continuously and the pool is really exercised rather than merely
	# enabled. --enable-api-server switches page allocation to memfd, the only
	# backing whose reuse a client can observe, and provides the getfile path.
	#
	log "Cachefs - block pool enabled"
	$here/cachefs cache $options --block-size 4 --source-dir $bp_src --cache-dir memory --cache-size 256 \
		--buffer-size 32 --buffer-timeout 180 --share-reserve 0 \
		--enable-api-server $bp_opt cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "block pool: enabled log" 0 \
		'grep "Block pool enabled: 8 slots of 4194304 bytes" $main_log'
	check_stat_value "block pool: startup prefault blocks" \
		"cachefs_block_pool_prefault_blocks" "==" $bp_slots $mnt/.stats 20
	check_stat_value "block pool: startup prefault bytes" \
		"cachefs_block_pool_prefault_bytes" "==" $bp_cap $mnt/.stats
	check_stat_value "block pool: startup residency" "cachefs_block_pool_bytes" "==" $bp_cap $mnt/.stats

	# fuse read path: the first pass fills the cache and starts evicting, so the
	# second pass reads back blocks the pool served.
	run_test "block pool: treesum first pass" 0 treesum -r $bp_sum $mnt
	run_test "block pool: treesum second pass" 0 treesum -r $bp_sum $mnt
	check_stat_value "block pool: eviction really happened" \
		"cachefs_blockcache_evicts" ">" 0 $mnt/.stats
	check_stat_value "block pool: blocks were served from pool" \
		"cachefs_block_pool_reuse" ">" 0 $mnt/.stats
	# Nothing can be reaped yet (--buffer-timeout 180 outlasts this test), so
	# the pool is holding blocks, and never more than --buffer-size worth.
	check_stat_value "block pool: holding blocks" "cachefs_block_pool_bytes" ">" 0 $mnt/.stats
	check_stat_value "block pool: residency within --buffer-size" \
		"cachefs_block_pool_bytes" "<=" $bp_cap $mnt/.stats

	# vfs API GetFileChunks path, under concurrent eviction pressure
	churn_flag=$(mktemp)
	read_loop $mnt/churn $churn_flag &
	churn_pid=$!
	for round in 1 2 3; do
		rm -rf $bp_out
		run_test "block pool: getfile round $round" 0 \
			"$here/cachefs apiclient getfile $bp_opt -O $bp_out $bp_paths"
		# byte-exact against the source, not just readable
		run_test "block pool: getfile round $round data" 0 "diff -r $bp_src/files $bp_out/files"
	done
	rm -f $churn_flag
	wait $churn_pid

	# The blocks exported to the client were given back by the release RPC,
	# which is what lets an exported block re-enter the pool at all.
	check_stat_value "block pool: getfile released its shares" \
		"cachefs_vfs_api_share_release_rpc" ">" 0 $mnt/.stats
	check_stat_value "block pool: no share left behind" \
		"cachefs_vfs_api_shared_files" "==" 0 $mnt/.stats
	run_test "block pool: enabled log check" 1 grep -e ERROR -e FATAL $main_log
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# block pool disabled: --buffer-size 0 is the off switch
	#
	log "Cachefs - block pool disabled"
	$here/cachefs cache $options --block-size 4 --source-dir $bp_src --cache-dir memory --cache-size 256 \
		--buffer-size 0 --share-reserve 0 --enable-api-server $bp_opt cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "block pool: disabled log" 0 'grep "Block pool disabled" $main_log'
	run_test "block pool: treesum with pool off" 0 treesum -r $bp_sum $mnt
	rm -rf $bp_out
	run_test "block pool: getfile with pool off" 0 \
		"$here/cachefs apiclient getfile $bp_opt -O $bp_out $bp_paths"
	run_test "block pool: getfile with pool off data" 0 "diff -r $bp_src/files $bp_out/files"
	check_stat_value "block pool: no reuse when disabled" "cachefs_block_pool_reuse" "==" 0 $mnt/.stats
	check_stat_value "block pool: no prefault when disabled" "cachefs_block_pool_prefault_blocks" "==" 0 $mnt/.stats
	check_stat_value "block pool: no residency when disabled" "cachefs_block_pool_bytes" "==" 0 $mnt/.stats
	run_test "block pool: disabled log check" 1 grep -e ERROR -e FATAL $main_log
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# prefault only: returned blocks are always freed, while workers still warm
	# new blocks before either read path uses them.
	#
	log "Cachefs - block pool: prefault only"
	$here/cachefs cache $options --block-size 4 --source-dir $bp_src --cache-dir memory --cache-size 256 \
		--buffer-size 32 --buffer-timeout 180 --buffer-reuse=false --share-reserve 0 \
		--enable-api-server $bp_opt cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	check_stat_value "block pool: prefault-only startup blocks" \
		"cachefs_block_pool_prefault_blocks" "==" $bp_slots $mnt/.stats 20
	check_stat_value "block pool: prefault-only startup bytes" \
		"cachefs_block_pool_prefault_bytes" "==" $bp_cap $mnt/.stats
	check_stat_value "block pool: prefault-only startup residency" \
		"cachefs_block_pool_bytes" "==" $bp_cap $mnt/.stats
	run_test "block pool: prefault-only FUSE read" 0 treesum -r $bp_sum $mnt
	rm -rf $bp_out
	run_test "block pool: prefault-only getfile" 0 \
		"$here/cachefs apiclient getfile $bp_opt -O $bp_out $bp_paths"
	run_test "block pool: prefault-only getfile data" 0 "diff -r $bp_src/files $bp_out/files"
	run_test "block pool: prefault-only log check" 1 grep -e ERROR -e FATAL $main_log
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# reuse only: no worker is allowed to create warm blocks, but FUSE reads can
	# still recycle blocks and API reads must remain byte-exact.
	#
	log "Cachefs - block pool: reuse only"
	$here/cachefs cache $options --block-size 4 --source-dir $bp_src --cache-dir memory --cache-size 256 \
		--buffer-size 32 --buffer-timeout 180 --buffer-prefault=false --share-reserve 0 \
		--enable-api-server $bp_opt cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	check_stat_value "block pool: no prefault with prefault disabled" \
		"cachefs_block_pool_prefault_blocks" "==" 0 $mnt/.stats
	run_test "block pool: reuse-only FUSE first pass" 0 treesum -r $bp_sum $mnt
	run_test "block pool: reuse-only FUSE second pass" 0 treesum -r $bp_sum $mnt
	check_stat_value "block pool: reuse-only blocks were served from pool" \
		"cachefs_block_pool_reuse" ">" 0 $mnt/.stats
	rm -rf $bp_out
	run_test "block pool: reuse-only getfile" 0 \
		"$here/cachefs apiclient getfile $bp_opt -O $bp_out $bp_paths"
	run_test "block pool: reuse-only getfile data" 0 "diff -r $bp_src/files $bp_out/files"
	run_test "block pool: reuse-only log check" 1 grep -e ERROR -e FATAL $main_log
	umount $mnt
	kill $pid || true
	wait $pid

	rm -rf $bp_src $bp_out $bp_sum
}

sanity_test()
{
	#
	# use dir as cache
	#
	log "Cachefs - dir cache"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir /var/cachefs --cache-size 2048 cachefs $mnt &
	pid=$!
	wait_mount $mnt

	# check data integrity
	run_test "dir_cache" 0 treesum -r /tmp/source.sum $mnt
	check_stat_value "cached_inodes" "cachefs_meta_cached_inodes" ">" 0 $mnt/.stats
	vmrss_bytes=$(( $(awk '/VmRSS/ {print $2}' /proc/$pid/status) * 1024 ))
	lower=$((vmrss_bytes * 9 / 10))   # 90%
	upper=$((vmrss_bytes * 11 / 10))  # 110%
	check_stat_value "check $pid cachefs stats mem lower bound" "cachefs_memory" ">" $lower $mnt/.stats
	check_stat_value "check $pid cachefs stats mem upper bound" "cachefs_memory" "<" $upper $mnt/.stats
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# recycle inode attributes while keeping the inode skeleton
	#
	log "Cachefs - inode attr cache sweep"
	CFS_META_SWEEP_INTERVAL=1 $here/cachefs cache $options --source-dir $source_dir \
		--cache-dir memory --cache-size 2048 --readdir-cache 0 --attr-cleanup cachefs $mnt &
	pid=$!
	wait_mount $mnt
	run_test "attr_sweep_first_pass" 0 treesum -r /tmp/source.sum $mnt
	check_stat_value "cachefs attr sweep: inodes populated" "cachefs_meta_cached_inodes" ">" 0 $mnt/.stats
	check_stat_value "cachefs attr sweep: attrs populated" "cachefs_meta_cached_attrs" ">" 0 $mnt/.stats
	inode_count=$(awk '/^cachefs_meta_cached_inodes / {print $2}' $mnt/.stats)
	sleep 2
	# Reading .stats through FUSE may reload the root attribute during path
	# resolution and populate root attr, so there might be one attr cached.
	check_stat_value "cachefs attr sweep: attrs recycled" "cachefs_meta_cached_attrs" "<=" 1 $mnt/.stats
	check_stat_value "cachefs attr sweep: inode skeleton retained" "cachefs_meta_cached_inodes" "==" $inode_count $mnt/.stats
	run_test "attr_sweep_second_pass" 0 treesum -r /tmp/source.sum $mnt
	check_stat_value "cachefs attr sweep: attrs reloaded" "cachefs_meta_cached_attrs" ">" 0 $mnt/.stats
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# use memory as cache
	#
	log "Cachefs - memory cache"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 2048 cachefs $mnt &
	pid=$!
	wait_mount $mnt

	# check data integrity
	run_test "mem_cache" 0 treesum -r /tmp/source.sum $mnt
	umount $mnt
	kill $pid || true
	wait $pid
	kill $pid_stats || true
	wait $pid_stats

	#
	# source direct IO (O_DIRECT): force enable and verify data integrity.
	# The source fs must support O_DIRECT; if it silently downgrades to buffered IO
	# the no-downgrade assertion below fails, so this is a genuine forced-dio test.
	#
	log "Cachefs - source direct IO"
	$here/cachefs cache $options --source-dir $source_dir --source-direct-io --cache-dir memory --cache-size 2048 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# read all data first so any O_DIRECT open (and potential downgrade) has happened
	run_test "cachefs_source_direct_io" 0 treesum -r /tmp/source.sum $mnt
	# startup log must show direct IO enabled ...
	run_test "cachefs_source_direct_io_enabled" 0 'grep "Source direct IO (O_DIRECT) enabled" $main_log'
	# ... and it must not have been downgraded to buffered IO
	run_test "cachefs_source_direct_io_no_downgrade" 1 'grep "does not support O_DIRECT" $main_log'
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# cache direct IO (O_DIRECT): force enable read+write O_DIRECT on the local
	# disk cache and verify data integrity. Uses a disk cache dir (memory cache
	# has no disk backend); the cache fs must support O_DIRECT, otherwise the
	# no-downgrade assertion below fails, making this a genuine forced-dio test.
	#
	log "Cachefs - cache direct IO"
	rm -rf $cache_dir
	mkdir -p $cache_dir
	$here/cachefs cache $options --source-dir $source_dir --cache-read-direct-io --cache-write-direct-io \
		--cache-dir $cache_dir --cache-size 2048 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# read all data so cached blocks are written (write O_DIRECT) then read back
	# from disk (read O_DIRECT); re-read to force a local cache hit.
	run_test "cachefs_cache_direct_io" 0 treesum -r /tmp/source.sum $mnt
	run_test "cachefs_cache_direct_io_reread" 0 treesum -r /tmp/source.sum $mnt
	# startup log must show both read and write direct IO enabled ...
	run_test "cachefs_cache_read_direct_io_enabled" 0 'grep "Cache read direct IO (O_DIRECT) enabled" $main_log'
	run_test "cachefs_cache_write_direct_io_enabled" 0 'grep "Cache write direct IO (O_DIRECT) enabled" $main_log'
	# ... and it must not have been downgraded to buffered IO
	run_test "cachefs_cache_direct_io_no_downgrade" 1 'grep "O_DIRECT not supported for local cache" $main_log'
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# Read internal files
	#
	log "Cachefs - read internal files"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 2048 cachefs $mnt &
	pid=$!
	wait_mount $mnt
	$here/cachefs stats $mnt > >(tee $stats_log) 2>&1 &
	pid_stats=$!
	$here/cachefs profile --interval 1 $mnt > >(tee $profile_log) 2>&1 &
	pid_profile=$!
	run_test "read .config" 0 'grep "\"CacheDir\": \"memory\"" $mnt/.config'
	sleep 1
	run_test "read .stats" 1 grep -e WARNING -e ERROR -e FATAL $stats_log
	run_test "read .accesslog" 1 grep -e WARNING -e ERROR -e FATAL $profile_log
	kill $pid $pid_stats $pid_profile
	wait $pid $pid_stats $pid_profile

	#
	# CAI disables local cache
	#
	log "Cachefs - CAI disables local cache"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 0 --prefetch 0 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "read data without local cache" 0 cat $mnt/sizes/1048576 > /dev/null
	check_stat_value "no data cached w/o local cache" "cachefs_cache_blocks" "==" 0 $mnt/.stats
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# test preload
	#
	log "Cachefs - memory cache with preload"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 2048 cachefs $mnt --preload / > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# give preload a few seconds then check data integrity
	sleep 5
	check_stat_value "check preload source read" "cachefs_source_reads" ">" 0 $mnt/.stats
	check_stat_value "check preload cache block" "cachefs_cache_blocks" ">" 0 $mnt/.stats
	check_stat_value "check preload cleanup" "cachefs_blockcache_evicts" ">" 0 $mnt/.stats
	run_test "preload" 0 treesum -r /tmp/source.sum $mnt
	check_stat_value "check preload cache hit" "cachefs_cache_hits" ">" 0 $mnt/.stats
	umount $mnt
	run_test "preload_log_check" 1 grep WARNING $main_log
	kill $pid || true
	wait $pid

	log "Cachefs - disk cache with preload"
	rm -rf $cache_dir
	mkdir -p $cache_dir
	$here/cachefs cache $options --source-dir $source_dir --cache-dir $cache_dir --cache-size 2048 cachefs $mnt --preload / > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	sleep 5
	check_stat_value "check preload source read" "cachefs_source_reads" ">" 0 $mnt/.stats
	check_stat_value "check preload cache block" "cachefs_cache_blocks" ">" 0 $mnt/.stats
	check_stat_value "check preload cleanup" "cachefs_blockcache_evicts" ">" 0 $mnt/.stats
	check_stat_value "check disk cache drops" "cachefs_blockcache_drops" ">=" 0 $mnt/.stats
	run_test "treesum after preload with disk cache" 0 treesum -r /tmp/source.sum $mnt
	check_stat_value "check preload cache hit" "cachefs_cache_hits" ">" 0 $mnt/.stats
	umount $mnt
	run_test "preload_disk_cache_log_check" 1 grep WARNING $main_log
	kill $pid || true
	wait $pid

	#
	# test expire
	#
	log "Cachefs - expire"
	$here/cachefs cache $options --source-dir $source_dir/fsmark cachefs $mnt --preload / --cache-expire 2.9 > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# preload occurs after 2s, hardcoded to allow serf network formation.
	# first expiration occurs after 2.9s.
	# 2s is sufficient for preload since the data volume is small.
	sleep 2
	check_stat_value "check expire cache block > 0" "cachefs_cache_blocks" ">" 0 $mnt/.stats
	# The maximum allowed age is twice the expiration period (covering worst-case scenario
	# where new cache is generated right after previous expiration), plus a small execution
	# buffer. 6s is more than sufficient for this purpose.
	sleep 6
	check_stat_value "check expire expire" "cachefs_blockcache_evicts" ">" 0 $mnt/.stats
	check_stat_value "check expire cache block == 0" "cachefs_cache_blocks" "==" 0 $mnt/.stats
	umount $mnt
	run_test "expire_log_check" 1 grep WARNING $main_log
	kill $pid || true
	wait $pid

	#
	# test expire with pinned cache
	#
	log "Cachefs - expire skips pinned cache"
	$here/cachefs cache $options --source-dir $source_dir cachefs $mnt --preload /fsmark --cache-expire 2.9 --pin-cache /fsmark > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# Same timing as the expire test above: preload after 2s, first expiration after 2.9s.
	sleep 2
	check_stat_value "check expire pin cache block > 0" "cachefs_cache_blocks" ">" 0 $mnt/.stats
	sleep 6
	check_stat_value "check expire pin skip pinned" "cachefs_blockcache_evicts_skip_pinned" ">" 0 $mnt/.stats
	check_stat_value "check expire pin no eviction" "cachefs_blockcache_evicts" "==" 0 $mnt/.stats
	check_stat_value "check expire pin cache block kept" "cachefs_cache_blocks" ">" 0 $mnt/.stats
	umount $mnt
	run_test "expire_pin_log_check" 1 grep WARNING $main_log
	kill $pid || true
	wait $pid

	#
	# test periodical preload
	#
	log "Cachefs - periodical preload"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 2048 cachefs $mnt --preload "/fsmark sizes" \
		--preload-interval 2 > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# give preload a few seconds then check data integrity
	sleep 5
	run_test "periodical_preload" 0 treesum -r /tmp/source.sum  $mnt
	umount $mnt
	run_test "periodical_preload_log_check" 1 grep WARNING $main_log
	kill $pid || true
	wait $pid

	#
	# test cache root server & client
	#
	log "Cachefs - cache root server and client"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 2048 \
		--cacheroot-port 8833 --cacheroot-tracker localhost:8833 cachefs $mnt > >(tee $cacheroot_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	sleep 1
	# wildcard listener logs [::] with dual-stack sockets and 0.0.0.0 when
	# IPv6 is unavailable, accept both
	run_test "cacheroot_server_check" 0 \
		'grep -E "Serving RPC service on (\[::\]|0\.0\.0\.0):8833" $cacheroot_log'
	run_test "cacheroot_client_check" 0 \
		'grep "Remote cache root tracker servers: \[localhost:8833\]" $cacheroot_log'
	run_test "cacheroot_force_crc32" 0 \
		'grep "Force enable data checksum with crc32" $cacheroot_log'

	# populate this node's cache, then exercise the cacheroot cluster management
	# CLI (ratio / stats) against the single-node cluster on the cacheroot-port.
	run_test "cacheroot: preload for ratio" 0 $here/cachefs preload $mnt

	# stats: plain output lists the single instance without error
	run_test "cacheroot stats plain" 0 \
		'$here/cachefs cacheroot stats --addr localhost:8833'
	# stats: json output parses to one ok instance with non-zero capacity
	$here/cachefs cacheroot stats --addr localhost:8833 -f json > /tmp/cacheroot_stats.json
	run_test "cacheroot stats json ok instance" 0 \
		'python3 -c "import json; d=json.load(open(\"/tmp/cacheroot_stats.json\")); assert len(d)>=1 and all(x[\"ok\"] for x in d) and any(x[\"capacity\"]>0 for x in d), d"'

	# ratio: relative root path, plain output
	run_test "cacheroot ratio plain relative" 0 \
		'$here/cachefs cacheroot ratio / --relative --addr localhost:8833'
	# ratio: absolute mount path (exercises path->relative conversion), json;
	# total_blocks is derived from the meta walk so it is always > 0 for a
	# non-empty source tree.
	$here/cachefs cacheroot ratio $mnt --addr localhost:8833 -f json > /tmp/cacheroot_ratio.json
	run_test "cacheroot ratio json total blocks" 0 \
		'python3 -c "import json; d=json.load(open(\"/tmp/cacheroot_ratio.json\")); assert len(d)==1 and d[0][\"ok\"] and d[0][\"total_blocks\"]>0, d"'
	# ratio: csv output with per-node breakdown must succeed
	run_test "cacheroot ratio csv list-nodes" 0 \
		'$here/cachefs cacheroot ratio / --relative --list-nodes -f csv --addr localhost:8833'

	# invalid format is rejected
	run_test "cacheroot ratio bad format" 1 \
		'$here/cachefs cacheroot ratio / --relative -f bogus --addr localhost:8833'
	# ratio without a path argument is rejected
	run_test "cacheroot ratio missing path" 1 \
		'$here/cachefs cacheroot ratio --addr localhost:8833'

	# cachestate: per-instance CachefsState service is served on the cachePort
	# (7788) by the same running instance. Query its two methods directly.
	# state: plain output lists the single instance without error
	run_test "cachestate state plain" 0 \
		'$here/cachefs cachestate state --addr localhost:7788'
	# state: json parses to one ok node with non-zero capacity
	$here/cachefs cachestate state --addr localhost:7788 -f json > /tmp/cachestate_state.json
	run_test "cachestate state json ok node" 0 \
		'python3 -c "import json; d=json.load(open(\"/tmp/cachestate_state.json\")); assert d[\"ok\"] and d[\"capacity\"]>0, d"'
	# state: csv output succeeds
	run_test "cachestate state csv" 0 \
		'$here/cachefs cachestate state --addr localhost:7788 -f csv'

	# tracker: plain output succeeds (single-node cluster: local tracker may be empty)
	run_test "cachestate tracker plain" 0 \
		'$here/cachefs cachestate tracker --addr localhost:7788'
	# tracker: relative path filter, plain output succeeds
	run_test "cachestate tracker plain relative" 0 \
		'$here/cachefs cachestate tracker / --relative --addr localhost:7788'
	# tracker: absolute mount path (exercises path->relative conversion), json;
	# the result is a (possibly empty) list of targets on a single-node cluster.
	$here/cachefs cachestate tracker $mnt --addr localhost:7788 -f json > /tmp/cachestate_tracker.json
	run_test "cachestate tracker json list" 0 \
		'python3 -c "import json; d=json.load(open(\"/tmp/cachestate_tracker.json\")); assert isinstance(d, list), d"'
	# tracker: csv output succeeds
	run_test "cachestate tracker csv" 0 \
		'$here/cachefs cachestate tracker --addr localhost:7788 -f csv'
	# tracker: local mode (-l) reads this node's own cache distribution; plain and
	# json (a possibly-empty list of targets) both succeed on a single node
	run_test "cachestate tracker local plain" 0 \
		'$here/cachefs cachestate tracker -l --addr localhost:7788'
	$here/cachefs cachestate tracker -l --addr localhost:7788 -f json > /tmp/cachestate_tracker_local.json
	run_test "cachestate tracker local json list" 0 \
		'python3 -c "import json; d=json.load(open(\"/tmp/cachestate_tracker_local.json\")); assert isinstance(d, list), d"'

	# invalid format is rejected for both subcommands
	run_test "cachestate state bad format" 1 \
		'$here/cachefs cachestate state -f bogus --addr localhost:7788'
	run_test "cachestate tracker bad format" 1 \
		'$here/cachefs cachestate tracker -f bogus --addr localhost:7788'

	# the server logs each management call at Info level
	run_test "cacheroot server GetCacheStats log" 0 'grep "GetCacheStats" $cacheroot_log'
	run_test "cacheroot server GetCacheRatio log" 0 'grep "GetCacheRatio" $cacheroot_log'
	run_test "cacheroot server GetCacheState log" 0 'grep "GetCacheState called" $cacheroot_log'
	run_test "cachestate server GetTrackerInfo log" 0 'grep "GetTrackerInfo called" $cacheroot_log'

	# server-side metrics reflect the calls made above. The cluster counters and
	# their latency histograms live on the coordinator (cacheroot-port); the
	# cache-state counter lives in the store and is bumped once per fanout call.
	check_stat_value "cacheroot metric: getcacheratio count" \
		"cachefs_cacheroot_cluster_getcacheratio" ">" 0 $mnt/.stats
	check_stat_value "cacheroot metric: getcacheratio hist samples" \
		"cachefs_cacheroot_cluster_getcacheratio_durations_histogram_seconds_total" ">" 0 $mnt/.stats
	check_stat_value "cacheroot metric: getcachestats count" \
		"cachefs_cacheroot_cluster_getcachestats" ">" 0 $mnt/.stats
	check_stat_value "cacheroot metric: getcachestats hist samples" \
		"cachefs_cacheroot_cluster_getcachestats_durations_histogram_seconds_total" ">" 0 $mnt/.stats
	# GetCacheState is invoked once per member during each stats fanout.
	check_stat_value "cacheroot metric: cache_state_query count" \
		"cachefs_cache_state_query" ">" 0 $mnt/.stats

	run_test "cacheroot cli log check" 1 grep -e ERROR -e FATAL $cacheroot_log

	rm -f /tmp/cacheroot_stats.json /tmp/cacheroot_ratio.json \
		/tmp/cachestate_state.json /tmp/cachestate_tracker.json \
		/tmp/cachestate_tracker_local.json
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# test meta operation timeout
	#
	log "Cachefs - meta operation timeout"
	CFS_READDIR_FAULT=true $here/cachefs cache $options --source-dir $source_dir --cache-size 1024 \
		--readdir-cache 1 --metaop-timeout 1 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# add new file to source, on first readdir, it should be cached
	touch $source_dir/meta-op-timeout-test
	run_test "metaop_timeout_1st_readdir" 0 'ls $mnt | grep meta-op-timeout-test'

	# sleep to let readdir cache expire, then remove test file
	sleep 1.5
	rm -f $source_dir/meta-op-timeout-test

	# cachefs will inject error to subsequent readdir and return cached
	# result, which still contains test file, log should contains readdir
	# error warning
	run_test "metaop_timeout_2nd_readdir" 0 'ls $mnt | grep meta-op-timeout-test'
	# sleep 1s to let readdir timeout, and 0.1 to give goroutine some time to run
	sleep 1.1
	run_test "metaop_timeout_timeout_log" 0 'grep "readdir / timeout" $main_log'
	sleep 1.5
	run_test "metaop_timeout_3rd_readdir" 0 'ls $mnt | grep meta-op-timeout-test'
	sleep 1.1
	run_test "metaop_timeout_enoent_log" 0 'grep "readdir / failed: no such file or directory" $main_log'
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# test liveness probe
	#
	log "Cachefs - liveness probe"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 2048 cachefs $mnt &
	pid=$!
	wait_mount $mnt
	run_test "liveness_probe_ok" 0 $curl localhost:7955/liveness
	umount $mnt
	kill $pid || true
	wait $pid
	run_test "liveness_probe_refused" 1 $curl localhost:7955/liveness

	#
	# test reload cache on subpath list change
	#
	log "Cachefs - reload cache on subpath change"
	echo "" > /tmp/subpaths.txt
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 2048 cachefs $mnt \
		--preload / --source-subpath-file /tmp/subpaths.txt \
		--source-subpath-reload 1 --readdir-cache 0 > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# give initial preload a few seconds to finish, to avoid preload in progress error
	sleep 5
	# update subpath.txt, should trigger cache reload
	echo "/fsmark" > /tmp/subpaths.txt
	sleep 2
	run_test "reload_cache_event" 0 'grep "triggered by reload event" $main_log'
	run_test "reload_cache_evict" 0 'grep "Evicted" $main_log'
	umount $mnt
	run_test "reload_cache_log_check" 1 grep WARNING $main_log
	kill $pid || true
	wait $pid

	#
	# test self_managed_page_cache (fuse option `-o dontcache`)
	#
	log "Cachefs - fuse mount with dontcache option"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 2048 cachefs $mnt -o dontcache > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	if check_fuse_oot_support; then
		run_test "support_dontcache_1" 1 grep WARNING $main_log
		run_test "support_dontcache_2" 0 'grep "PR_SET_IO_FLUSHER set successfully" $main_log'
	else
		run_test "skip_dontcache_1" 1 'grep "PR_SET_IO_FLUSHER set successfully" $main_log'
		run_test "skip_dontcache_2" 0 'grep "unsupported FUSE option dontcache, drop it" $main_log'
		run_test "skip_dontcache_3" 0 'grep "Failed to handle FUSE mount option dontcache" $main_log'
	fi
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# test inode attr timeout
	#
	log "Cachefs - inode attr timeout"
	tmp=/tmp/$$_src
	mkdir -p $tmp/sizes
	echo 11 > $tmp/sizes/testfile
	$here/cachefs cache $options --source-dir $tmp --readdir-cache 1 --cache-dir memory --cache-size 2048 cachefs $mnt &
	pid=$!
	wait_mount $mnt
	cfs_old_inode=$(stat -c%i $mnt/sizes/testfile)
	src_old_mtime=$(stat -c%Y $tmp/sizes/testfile)
	cfs_old_mtime=$(stat -c%Y $mnt/sizes/testfile)
	run_test "src old mtime == cfs old mtime" 0 [ $src_old_mtime -eq $cfs_old_mtime ]
	cp -f $mnt/sizes/testfile $tmp/sizes/testfile.bak
	run_test "src old content == cfs old content" 0 cmp -s $tmp/sizes/testfile $mnt/sizes/testfile
	echo 22 > $tmp/sizes/testfile
	# not timed out, should hit inode attr cache
	src_new_mtime=$(stat -c%Y $tmp/sizes/testfile)
	cfs_new_mtime=$(stat -c%Y $mnt/sizes/testfile)
	run_test "src new mtime > cfs new mtime" 0 [ $src_new_mtime -gt $cfs_new_mtime ]
	run_test "cfs new mtime == cfs old mtime" 0 [ $cfs_new_mtime -eq $cfs_old_mtime ]
	run_test "cachefs new content == cfs old content" 0 cmp -s $mnt/sizes/testfile $tmp/sizes/testfile.bak
	run_test "cachefs new content != src new content" 1 cmp -s $mnt/sizes/testfile $tmp/sizes/testfile
	sleep 1
	# timed out, get new mtime from source
	cfs_new_inode=$(stat -c%i $mnt/sizes/testfile)
	src_new_mtime=$(stat -c%Y $tmp/sizes/testfile)
	cfs_new_mtime=$(stat -c%Y $mnt/sizes/testfile)
	run_test "cfs new inode == cfs old inode" 0 [ $cfs_new_inode -eq $cfs_old_inode ]
	run_test "src new mtime == cfs new mtime" 0 [ $src_new_mtime -eq $cfs_new_mtime ]
	run_test "cfs new mtime > cfs old mtime" 0 [ $cfs_new_mtime -gt $cfs_old_mtime ]
	run_test "cachefs new content != cfs old content" 1 cmp -s $mnt/sizes/testfile $tmp/sizes/testfile.bak
	run_test "cachefs new content == src new content" 0 cmp -s $mnt/sizes/testfile $tmp/sizes/testfile
	ls $mnt/sizes >/dev/null
	run_test "cachefs new file content == cfs old content" 0 cmp -s $mnt/sizes/testfile.bak $tmp/sizes/testfile.bak
	umount $mnt
	kill $pid || true
	wait $pid
	rm -rf $tmp

	#
	# preload and clear command
	#
	log "Cachefs - preload and clear subcommand"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 2048 \
		--enable-api-server $server_opt cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "preload subcommand" 0 $here/cachefs preload $mnt
	run_test "preload subcommand via api" 0 $here/cachefs preload $server_opt $mnt
	run_test "preload subcommand: check cachefs log" 1 grep -e WARNING -e ERROR $main_log
	check_stat_value "preload subcommand: cache metrics" "cachefs_cache_bytes" ">" 0 $mnt/.stats
	check_stat_value "preload subcommand: source metrics" "cachefs_source_reads" ">" 0 $mnt/.stats
	run_test "preload subcommand: --noall" 0 $here/cachefs preload --noall $mnt
	run_test "preload subcommand: -A" 0 $here/cachefs preload -A $mnt
	run_test "preload subcommand: --noforce" 0 $here/cachefs preload --noforce $mnt
	run_test "preload subcommand: -F" 0 $here/cachefs preload -F $mnt

	# confilct options
	run_test "preload subcommand: --all and --noall" 1 $here/cachefs preload --noall --all $mnt
	run_test "preload subcommand: --force and --noforce" 1 $here/cachefs preload --force --noforce $mnt

	run_test "clear subcommand" 0 $here/cachefs clear $mnt
	run_test "clear subcommand via api" 0 $here/cachefs apiclient clear $server_opt -r /
	run_test "clear subcommand: check cachefs log" 1 grep -e WARNING -e ERROR $main_log
	check_stat_value "clear subcommand: evict metrics" "cachefs_blockcache_evicts" ">" 0 $mnt/.stats
	check_stat_value "clear subcommand: cache metrics" "cachefs_cache_bytes" "==" 0 $mnt/.stats

	echo -e "$mnt/sizes\n$mnt/fsmark/" > /tmp/preload.txt
	$here/cachefs preload --file /tmp/preload.txt |& tee $preload_log
	run_test "preload subcommand file: check preload log" 1 grep -e WARNING -e ERROR -e FATAL $preload_log
	run_test "preload subcommand file: check cachefs log" 1 grep -e WARNING -e ERROR $main_log

	echo -e "$mnt/sizes\n$mnt/fsmark/" > /tmp/clear.txt
	$here/cachefs clear --file /tmp/clear.txt |& tee $clear_log
	run_test "clear subcommand file: check clear log" 1 grep -e WARNING -e ERROR -e FATAL $clear_log
	run_test "clear subcommand file: check cachefs log" 1 grep -e WARNING -e ERROR $main_log
	$here/cachefs clear $mnt/nosuchfile |& tee $clear_log
	run_test "clear subcommand: expect clear FATAL" 0 grep FATAL $clear_log

	$here/cachefs preload $mnt/nosuchfile |& tee $preload_log
	run_test "preload subcommand: expect preload FATAL" 0 grep FATAL $preload_log
	$here/cachefs preload --background $mnt/nosuchfile |& tee $preload_log
	run_test "preload subcommand background: expect preload FATAL" 0 grep FATAL $preload_log
	run_test "preload subcommand -d ENOENT" 1 $here/cachefs preload -d $mnt/nosuchfile
	# run successful background preload in the end to avoid concurrent preload
	run_test "preload subcommand -d" 0 $here/cachefs preload -d $mnt
	sleep 0.5
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# preload stops when hash-hit blocks are being evicted from a full cache
	#
	log "Cachefs - preload: stopWalkDir triggered by hash-hit blocks eviction"
	# reduce entry size to 1 so stageEntries holds less entries and
	# stopWalkDir could trigger stop walk dir logging
	CFS_PRELOAD_ENTRY_SIZE=1 $here/cachefs cache $options --source-dir $source_dir \
		--cache-dir memory --cache-size 256 --pin-cache /sizes \
		cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# --noall hash-hit-only path activates eviction-based stopWalkDir
	run_test "preload_evict_stop: preload --noall exits cleanly" 0 \
		$here/cachefs preload --noall $mnt
	run_test "preload_evict_stop: stop reason logged" 0 \
		'grep "Preload stop walking dir: pinned blocks being evicted" $main_log'
	check_stat_value "preload_evict_stop: blockcache_evicts_hashhit > 0" \
		"cachefs_blockcache_evicts_hashhit" ">" 0 $mnt/.stats
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# burst cache: defer startup burst until restore
	#
	defer_burst_test()
	{
		local mode=$1
		local start_cmd=$2
		local server_opt=" --api-server-sock /run/cachefsci-api.sock"
		local mib128=$(( 128 * 1024 * 1024 ))
		local mib512=$(( 512 * 1024 * 1024 ))

		log "Cachefs - burst cache defer via $mode"
		eval "$start_cmd > >(tee $main_log) 2>&1 &"
		pid=$!
		wait_mount $mnt

		check_store_config "defer-burst $mode: initial Store.CacheSize" \
			"CacheSize" "==" $mib128
		check_store_config "defer-burst $mode: initial Store.BurstCacheSize deferred to cache-size" \
			"BurstCacheSize" "==" $mib128
		check_store_config "defer-burst $mode: Store.InitialBurstSize keeps the configured size" \
			"InitialBurstSize" "==" $mib512
		run_test "defer-burst $mode: recorded burst size logged" 0 \
			'grep "Burst cache deferred: effective 128MB, recorded 512MB" $main_log'
		run_test "defer-burst $mode: preload before restore" 0 $here/cachefs preload $mnt
		check_stat_value "defer-burst $mode: burst_bytes == 0 before restore" \
			"cachefs_blockcache_burst_bytes" "==" 0 $mnt/.stats
		run_test "defer-burst $mode: restore startup burst" 0 \
			$here/cachefs apiclient set-cache --restore-burst-cache $server_opt
		check_store_config "defer-burst $mode: Store.BurstCacheSize restored to 512 MiB" \
			"BurstCacheSize" "==" $mib512
		run_test "defer-burst $mode: preload after restore" 0 $here/cachefs preload $mnt
		check_stat_value "defer-burst $mode: burst_bytes > 0 after restore" \
			"cachefs_blockcache_burst_bytes" ">" 0 $mnt/.stats
		run_test "defer-burst $mode: check cachefs log" 1 grep -e WARNING -e ERROR $main_log

		umount $mnt
		kill $pid || true
		wait $pid
	}

	server_opt=" --api-server-sock /run/cachefsci-api.sock"
	# --block-size 4 keeps the default share-reserve (prefetch x block size) under the burst cap.
	base_defer_cmd="$here/cachefs cache $options --block-size 4 --source-dir $source_dir --cache-dir memory \
		--cache-size 128 --burst-cache-size 512 --enable-api-server $server_opt cachefs $mnt"
	defer_burst_test "cli" "$base_defer_cmd --defer-burst"
	defer_burst_test "env" "CFS_DEFER_BURST=true $base_defer_cmd"

	#
	# burst cache: clear burst cache
	#
	log "Cachefs - burst cache clear"
	server_opt=" --api-server-sock /run/cachefsci-api.sock"
	# Use a small cache-size (128 MiB) and larger burst-cache-size (512 MiB) so that
	# preloading fills the burst zone (used > cache-size) without triggering auto-eviction.
	# --block-size 4 keeps the default share-reserve (prefetch x block size) under the burst cap.
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory \
		--cache-size 128 --burst-cache-size 512 --block-size 4 \
		--enable-api-server $server_opt cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# Preload data to fill beyond cache-size, activating the burst zone.
	run_test "burst cache: preload to fill burst zone" 0 $here/cachefs preload $mnt
	check_stat_value "burst cache: burst_bytes > 0 after preload" \
		"cachefs_blockcache_burst_bytes" ">" 0 $mnt/.stats

	# Test clear --clear-burst via control file.
	run_test "clear --clear-burst subcommand" 0 $here/cachefs clear --clear-burst $mnt
	run_test "clear --clear-burst: check cachefs log" 1 grep -e WARNING -e ERROR $main_log
	check_stat_value "clear --clear-burst: burst_evict_bytes > 0" \
		"cachefs_blockcache_burst_evict_bytes" ">" 0 $mnt/.stats
	check_stat_value "clear --clear-burst: burst_bytes == 0 after clear" \
		"cachefs_blockcache_burst_bytes" "==" 0 $mnt/.stats

	# Refill burst zone, then test apiclient clear --clear-burst via API.
	$here/cachefs preload $mnt > /dev/null
	check_stat_value "burst cache: burst_bytes > 0 before api clear" \
		"cachefs_blockcache_burst_bytes" ">" 0 $mnt/.stats
	run_test "apiclient clear --clear-burst" 0 \
		$here/cachefs apiclient clear --clear-burst $server_opt
	run_test "apiclient clear --clear-burst: check cachefs log" 1 grep -e WARNING -e ERROR $main_log
	check_stat_value "apiclient clear --clear-burst: burst_bytes == 0" \
		"cachefs_blockcache_burst_bytes" "==" 0 $mnt/.stats

	umount $mnt
	kill $pid || true
	wait $pid

	#
	# apiclient set-cache: dynamic capacity adjustment
	#
	log "Cachefs - apiclient set-cache"
	server_opt=" --api-server-sock /run/cachefsci-api.sock"
	# Start with cache=128 MiB, burst=512 MiB.
	# --block-size 4 keeps the default share-reserve (prefetch x block size) under the burst cap.
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory \
		--cache-size 128 --burst-cache-size 512 --block-size 4 \
		--enable-api-server $server_opt cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# Byte equivalents used in config assertions below.
	local mib128=$(( 128 * 1024 * 1024 ))   # 134217728
	local mib256=$(( 256 * 1024 * 1024 ))   # 268435456
	local mib512=$(( 512 * 1024 * 1024 ))   # 536870912
	local mib1024=$(( 1024 * 1024 * 1024 )) # 1073741824
	local mib2048=$(( 2048 * 1024 * 1024 )) # 2147483648

	# Verify initial Store config values (distinct from Meta.CacheSize).
	check_store_config "set-cache: initial Store.CacheSize" \
		"CacheSize" "==" $mib128
	check_store_config "set-cache: initial Store.BurstCacheSize" \
		"BurstCacheSize" "==" $mib512

	# Preload source data so burst zone is active before adjustments.
	$here/cachefs preload $mnt > /dev/null
	check_stat_value "set-cache: initial burst_bytes > 0" \
		"cachefs_blockcache_burst_bytes" ">" 0 $mnt/.stats

	# --- Scenario 1: increase cache-size; burst is larger so it stays unchanged ---
	run_test "set-cache: --cache-size 256" 0 \
		$here/cachefs apiclient set-cache --cache-size 256 $server_opt
	check_store_config "set-cache: sc1: Store.CacheSize updated to 256 MiB" \
		"CacheSize" "==" $mib256
	check_store_config "set-cache: sc1: Store.BurstCacheSize unchanged at 512 MiB" \
		"BurstCacheSize" "==" $mib512

	# --- Scenario 2: increase burst-size only; cache unchanged ---
	run_test "set-cache: --burst-cache-size 1024" 0 \
		$here/cachefs apiclient set-cache --burst-cache-size 1024 $server_opt
	check_store_config "set-cache: sc2: Store.CacheSize unchanged at 256 MiB" \
		"CacheSize" "==" $mib256
	check_store_config "set-cache: sc2: Store.BurstCacheSize updated to 1024 MiB" \
		"BurstCacheSize" "==" $mib1024
	# Preload and verify burst zone is still usable with the new limits.
	$here/cachefs preload $mnt > /dev/null
	check_stat_value "set-cache: sc2: burst_bytes > 0 (data above 256 MiB base)" \
		"cachefs_blockcache_burst_bytes" ">" 0 $mnt/.stats

	# --- Scenario 3: both flags default to -1 (keep current values unchanged) ---
	run_test "set-cache: no flags is a no-op" 0 \
		$here/cachefs apiclient set-cache $server_opt
	check_store_config "set-cache: sc3: -1/-1: Store.CacheSize still 256 MiB" \
		"CacheSize" "==" $mib256
	check_store_config "set-cache: sc3: -1/-1: Store.BurstCacheSize still 1024 MiB" \
		"BurstCacheSize" "==" $mib1024

	# --- Scenario 4: burst < cache -> burst normalized up to cache ---
	# Request burst=50 MiB which is < cache=256 MiB; burst must be raised to 256 MiB.
	run_test "set-cache: burst < cache -> normalized to cache" 0 \
		$here/cachefs apiclient set-cache --burst-cache-size 50 $server_opt
	check_store_config "set-cache: sc4: Store.BurstCacheSize normalized to cache (256 MiB)" \
		"BurstCacheSize" "==" $mib256
	check_store_config "set-cache: sc4: Store.CacheSize unchanged at 256 MiB" \
		"CacheSize" "==" $mib256
	# With burst == cache there is no burst headroom; burst_bytes must be 0.
	check_stat_value "set-cache: sc4: burst_bytes == 0 after normalization" \
		"cachefs_blockcache_burst_bytes" "==" 0 $mnt/.stats

	# --- Scenario 5: restore burst-size to startup-configured value ---
	run_test "set-cache: restore burst cache" 0 \
		$here/cachefs apiclient set-cache --restore-burst-cache $server_opt
	check_store_config "set-cache: sc5: Store.CacheSize unchanged at 256 MiB" \
		"CacheSize" "==" $mib256
	check_store_config "set-cache: sc5: Store.BurstCacheSize restored to 512 MiB" \
		"BurstCacheSize" "==" $mib512
	$here/cachefs preload $mnt > /dev/null
	check_stat_value "set-cache: sc5: burst_bytes > 0 after restore" \
		"cachefs_blockcache_burst_bytes" ">" 0 $mnt/.stats

	# --- Scenario 6: new cache > current burst -> burst auto-raised ---
	# cache=2048 MiB > current burst=512 MiB; burst must be raised to match cache.
	run_test "set-cache: cache > burst -> burst auto-raised" 0 \
		$here/cachefs apiclient set-cache --cache-size 2048 $server_opt
	check_store_config "set-cache: sc6: Store.CacheSize updated to 2048 MiB" \
		"CacheSize" "==" $mib2048
	check_store_config "set-cache: sc6: Store.BurstCacheSize auto-raised to 2048 MiB" \
		"BurstCacheSize" "==" $mib2048

	run_test "set-cache: check cachefs log" 1 grep -e WARNING -e ERROR $main_log

	umount $mnt
	kill $pid || true
	wait $pid

	#
	# vfs api
	#
	log "Cachefs - vfs API"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 4096 \
		--share-reserve 0 --enable-api-server $server_opt cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# test get pid
	run_test "getpid request" 0 $here/cachefs apiclient getpid $server_opt

	# test preload relative path
	run_test "preload abs path" 1 $here/cachefs apiclient preload $server_opt /
	run_test "preload relative path success" 0 $here/cachefs apiclient preload $server_opt -r /

	# test get file
	run_test "getfile empty file" 0 $here/cachefs apiclient getfile $server_opt $mnt/sizes/emptyfile
	run_test "getfile read file" 0 $here/cachefs apiclient getfile $server_opt $mnt/sizes/4096 -o /tmp/4096file
	run_test "getfile larger than cache" 1 $here/cachefs apiclient getfile $server_opt $mnt/sizes/4294967297 >/dev/null
	run_test "getfile too large for share cap (no cache limit)" 1 $here/cachefs apiclient getfile --no-cache-limit $server_opt $mnt/sizes/4294967297 >/dev/null
	run_test "getfile big file" 0 $here/cachefs apiclient getfile $server_opt $mnt/sizes/4294967296 >/dev/null
	md5=`md5sum $mnt/sizes/4294967296 | awk '{print $1}'`
	gotmd5=`$here/cachefs apiclient getfile $server_opt $mnt/sizes/4294967296 | md5sum | awk '{print $1}'`
	run_test "getfile 4G+1 checksum" 0 [ "$gotmd5" == "$md5" ]

	# getfile with an explicit byte range: the output must equal the same
	# interval read from the source, both within one 4MiB block and across
	# the block boundary.
	run_test "getfile range within a block" 0 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/1048576 --range-start 1000 --range-length 100000 -o /tmp/range-in-block"
	dd if=$source_dir/sizes/1048576 of=/tmp/range-in-block.want bs=1000 skip=1 count=100 status=none
	run_test "getfile range within a block data" 0 "cmp /tmp/range-in-block /tmp/range-in-block.want"

	run_test "getfile range across blocks" 0 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/1073741824 --range-start 4194000 --range-length 1000 -o /tmp/range-xblock"
	dd if=$source_dir/sizes/1073741824 of=/tmp/range-xblock.want bs=1000 skip=4194 count=1 status=none
	run_test "getfile range across blocks data" 0 "cmp /tmp/range-xblock /tmp/range-xblock.want"

	# A range of a file larger than the cache is admitted: only the selected
	# blocks count against the share budget (whole-file getfile of it fails
	# above).
	run_test "getfile range of oversized file" 0 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/4294967297 --range-start 0 --range-length 4096 -o /tmp/range-oversized"
	dd if=$source_dir/sizes/4294967297 of=/tmp/range-oversized.want bs=4096 count=1 status=none
	run_test "getfile range of oversized file data" 0 "cmp /tmp/range-oversized /tmp/range-oversized.want"

	# --range-length alone copies the whole file in ranges of that size; the
	# 4MiB+1 file makes the last chunk cross the block boundary by one byte.
	md5=`md5sum $source_dir/sizes/4194305 | awk '{print $1}'`
	gotmd5=`$here/cachefs apiclient getfile $server_opt $mnt/sizes/4194305 --range-length 700000 | md5sum | awk '{print $1}'`
	run_test "getfile range copy checksum" 0 [ "$gotmd5" == "$md5" ]

	# Concurrent getfile applies the same range to every input file.
	run_test "getfile range multiple files" 0 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/4096 $mnt/sizes/4097 --range-start 1 --range-length 4000 -O /tmp/range-multi"
	dd if=$source_dir/sizes/4096 of=/tmp/range-multi-4096.want bs=1 skip=1 count=4000 status=none
	dd if=$source_dir/sizes/4097 of=/tmp/range-multi-4097.want bs=1 skip=1 count=4000 status=none
	run_test "getfile range multiple files data" 0 \
		"cmp /tmp/range-multi/sizes/4096 /tmp/range-multi-4096.want && cmp /tmp/range-multi/sizes/4097 /tmp/range-multi-4097.want"

	# Invalid range arguments are rejected before any request is sent.
	run_test "getfile range-start without length" 1 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/1048576 --range-start 10 >/dev/null 2>&1"
	run_test "getfile zero range length" 1 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/1048576 --range-start 0 --range-length 0 >/dev/null 2>&1"
	run_test "getfile range outside file" 1 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/1048576 --range-start 2000000 --range-length 100 >/dev/null 2>&1"

	# getfile --fd copies through direct cache block FDs instead of mmap.
	md5=`md5sum $source_dir/sizes/1048576 | awk '{print $1}'`
	gotmd5=`$here/cachefs apiclient getfile $server_opt $mnt/sizes/1048576 --fd | md5sum | awk '{print $1}'`
	run_test "getfile fd whole file checksum" 0 [ "$gotmd5" == "$md5" ]

	# mmap and --fd must produce the same bytes; the 4MiB+1 file also crosses
	# a block boundary.
	run_test "getfile mmap copy for fd compare" 0 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/4194305 -o /tmp/compare-mmap"
	run_test "getfile fd copy for fd compare" 0 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/4194305 --fd -o /tmp/compare-fd"
	run_test "getfile fd matches mmap data" 0 "cmp /tmp/compare-mmap /tmp/compare-fd"

	# --fd with an explicit range reads the selected spans with pread; the
	# intervals and their reference files are shared with the range tests.
	run_test "getfile fd range across blocks" 0 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/1073741824 --fd --range-start 4194000 --range-length 1000 -o /tmp/fd-range-xblock"
	run_test "getfile fd range across blocks data" 0 "cmp /tmp/fd-range-xblock /tmp/range-xblock.want"
	run_test "getfile fd range of oversized file" 0 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/4294967297 --fd --range-start 0 --range-length 4096 -o /tmp/fd-range-oversized"
	run_test "getfile fd range of oversized file data" 0 "cmp /tmp/fd-range-oversized /tmp/range-oversized.want"

	# The fd path reports the same daemon-side rejections as mmap.
	run_test "getfile fd larger than cache" 1 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/4294967297 --fd >/dev/null 2>&1"

	# Chunked range copy through direct FDs, checksum over stdout.
	md5=`md5sum $source_dir/sizes/4194305 | awk '{print $1}'`
	gotmd5=`$here/cachefs apiclient getfile $server_opt $mnt/sizes/4194305 --fd --range-length 700000 | md5sum | awk '{print $1}'`
	run_test "getfile fd range copy checksum" 0 [ "$gotmd5" == "$md5" ]

	# An empty file needs no fd transfer at all.
	run_test "getfile fd empty file" 0 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/emptyfile --fd -o /tmp/fd-empty"
	run_test "getfile fd empty file data" 0 "[ -f /tmp/fd-empty ] && [ ! -s /tmp/fd-empty ]"

	# Concurrent getfile with direct FDs.
	run_test "getfile fd multiple files" 0 \
		"$here/cachefs apiclient getfile $server_opt $mnt/sizes/4096 $mnt/sizes/4097 --fd -O /tmp/fd-multi"
	run_test "getfile fd multiple files data" 0 \
		"cmp /tmp/fd-multi/sizes/4096 $source_dir/sizes/4096 && cmp /tmp/fd-multi/sizes/4097 $source_dir/sizes/4097"

	# Remove the range/--fd intermediates, so a reused container cannot pass
	# their cmp assertions on stale files from a previous run.
	rm -rf /tmp/range-in-block /tmp/range-in-block.want \
		/tmp/range-xblock /tmp/range-xblock.want \
		/tmp/range-oversized /tmp/range-oversized.want \
		/tmp/range-multi /tmp/range-multi-4096.want /tmp/range-multi-4097.want \
		/tmp/compare-mmap /tmp/compare-fd \
		/tmp/fd-range-xblock /tmp/fd-range-oversized \
		/tmp/fd-empty /tmp/fd-multi

	# Verify share quota metrics: the getfile calls above went through share
	# admission and release; the oversized file was rejected as too large.
	check_stat_value "share quota: admits > 0" "cachefs_vfs_api_share_admits" ">" 0 $mnt/.stats
	check_stat_value "share quota: release rpc > 0" "cachefs_vfs_api_share_release_rpc" ">" 0 $mnt/.stats
	check_stat_value "share quota: shared files == 0 after release" "cachefs_vfs_api_shared_files" "==" 0 $mnt/.stats
	check_stat_value "share quota: too_large > 0" "cachefs_vfs_api_share_too_large" ">" 0 $mnt/.stats

	#
	# apiclient loadstats: data loading report (source vs peers)
	#
	# The preload and getfile calls above already read data through the source,
	# so the counters are non-zero by now.
	run_test "loadstats request" 0 \
		"$here/cachefs apiclient loadstats $server_opt > /tmp/loadstats.txt"
	# text must be the rendered detail line, printable as-is by any client
	run_test "loadstats: text is the detail line" 0 \
		'grep -q "^Load stats detail: source\[.*\] peer\[.*\]$" /tmp/loadstats.txt'
	# byte values must not contain a space, or awk-splitting the line breaks
	run_test "loadstats: byte values are single tokens" 1 \
		'grep -qE "=[0-9.]+ (B|[KMGTPE]iB)" /tmp/loadstats.txt'

	$here/cachefs apiclient loadstats --json $server_opt > /tmp/loadstats.json
	run_test "loadstats --json: schema and non-zero counters" 0 \
		'python3 -c "import json; d=json.load(open(\"/tmp/loadstats.json\")); assert d[\"uptime_seconds\"]>0, d; s=d[\"source\"]; assert s[\"reads\"]>0 and s[\"bytes\"]>0, s; assert s[\"latency\"][\"count\"]>0 and s[\"latency\"][\"buckets\"], s; p=d[\"peer\"]; assert p[\"hits\"]>=0 and \"latency\" in p, p"'
	# latency observation and the read counter sit on the same success path, so
	# a mismatch means one of the two increments got moved or lost
	run_test "loadstats --json: latency count tracks reads" 0 \
		'python3 -c "import json; s=json.load(open(\"/tmp/loadstats.json\"))[\"source\"]; assert s[\"latency\"][\"count\"]==s[\"reads\"], s"'
	# buckets are cumulative and must never exceed the total sample count
	run_test "loadstats --json: buckets are cumulative and bounded" 0 \
		'python3 -c "import json; l=json.load(open(\"/tmp/loadstats.json\"))[\"source\"][\"latency\"]; b=[x[\"n\"] for x in l[\"buckets\"]]; assert b==sorted(b), b; assert b[-1]<=l[\"count\"], (b, l[\"count\"])"'

	# Cross-check the two independent read paths against each other: this API
	# reads the metric objects directly, while .stats goes through the Prometheus
	# registry. Counters only grow and .stats is read second, so json <= .stats
	# holds without any tolerance. A silent zero here (from a renamed metric or a
	# changed registry prefix) would otherwise look like "no data was loaded".
	json_reads=$(python3 -c 'import json; print(json.load(open("/tmp/loadstats.json"))["source"]["reads"])')
	stats_reads=$(awk '/^cachefs_source_reads / {print $2}' $mnt/.stats)
	run_test "loadstats: source reads agree with .stats" 0 \
		"python3 -c \"assert 0 < $json_reads <= $stats_reads, ($json_reads, $stats_reads)\""

	# --log must put both records in the mount's own log; that is the whole point
	# of the flag, since .stats and /metrics die with the process.
	run_test "loadstats --log request" 0 \
		"$here/cachefs apiclient loadstats --log $server_opt > /dev/null"
	run_test "loadstats --log: summary line in cachefs log" 0 \
		'grep -q "Load stats since mount" $main_log'
	run_test "loadstats --log: detail line in cachefs log" 0 \
		'grep -q "Load stats detail:" $main_log'
	# the summary contains a percent sign; if it were passed as a format string
	# the logger would leave %!(MISSING) behind
	run_test "loadstats --log: summary not mangled by printf" 1 \
		'grep -e "MISSING" -e "%!" $main_log'
	run_test "loadstats: check cachefs log" 1 grep -e WARNING -e ERROR $main_log
	check_stat_value "loadstats: rpc counter > 0" "cachefs_vfs_api_getloadstats" ">" 0 $mnt/.stats
	check_stat_value "loadstats: no request left in flight" "cachefs_vfs_api_getloadstats_flight" "==" 0 $mnt/.stats

	# test unmount
	run_test "unmount request" 0 $here/cachefs apiclient umount $server_opt
	run_test "check umount result" 1 grep $mnt /proc/mounts
	umount $mnt || true
	kill $pid || true
	wait $pid

	#
	# test virtual node
	#
	log "Cachefs - virtual node count"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory \
		--cache-size 2048 --virtual-node-count 10000 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "check virtual node count" 0 \
		'grep "Creating consistent hash ring with 10000 virtual nodes" $main_log'
	umount $mnt || true
	kill $pid || true
	wait $pid

	#
	# test pin cache
	#
	log "Cachefs - pin cache protection"
	# Blocks in the recent-add/load LRU window (--prefetch blocks, 24 x 4MiB)
	# are evicted only after pinned blocks. The cache must therefore hold the pinned
	# /fsmark data, the LRU window and extra non-pinned blocks; otherwise pinned blocks
	# get evicted. Keep it far below the ~10GiB source so eviction still occurs.
	$here/cachefs cache $options --block-size 4 --source-dir $source_dir --cache-dir memory --cache-size 256 \
		--pin-cache /fsmark cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "pin: preload" 0 $here/cachefs preload $mnt
	check_stat_value "pin: eviction occurred" "cachefs_blockcache_evicts" ">" 0 $mnt/.stats
	check_stat_value "pin: no pinned eviction" "cachefs_blockcache_evicts_pinned" "==" 0 $mnt/.stats
	run_test "pin: read pinned data" 0 ls $mnt/fsmark
	umount $mnt
	run_test "pin_cache_log_check" 1 grep -e ERROR -e WARNING $main_log
	kill $pid || true
	wait $pid

	#
	# periodic preload without hash ring triggers pinned eviction stop
	#
	log "Cachefs - periodic preload stops when pinned blocks are evicted (no hash ring)"
	# Single node (no serf/hash ring), pin /sizes, small cache
	CFS_PRELOAD_ENTRY_SIZE=1 $here/cachefs cache $options --source-dir $source_dir \
		--cache-dir memory --cache-size 512 --preload / --preload-interval 3 \
		--pin-cache /sizes cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	sleep 5
	check_stat_value "IT13: blockcache_evicts_pinned > 0" \
		"cachefs_blockcache_evicts_pinned" ">" 0 $mnt/.stats
	run_test "IT13: stop reason logged" 0 \
		'grep "Preload stop walking dir: pinned blocks being evicted" $main_log'
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# test pin cache cmd with reload
	#
	log "Cachefs - pin cache cmd"
	pin_list_file=$(mktemp)
	echo /fsmark > $pin_list_file
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 20 \
		--pin-cache-cmd "cat $pin_list_file" --pin-cache-reload 2 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "pin-cache-cmd: log shows initial path" 0 'grep "Pin cache initialized.*fsmark" $main_log'
	echo /fsmark2 > $pin_list_file
	sleep 4
	run_test "pin-cache-cmd: log shows reloaded path" 0 'grep "Reloaded pin cache paths.*fsmark2" $main_log'
	umount $mnt
	rm -f $pin_list_file
	kill $pid || true
	wait $pid

	#
	# test source subpath cmd with reload
	#
	log "Cachefs - source subpath cmd"
	subpath_list_file=$(mktemp)
	echo /fsmark > $subpath_list_file
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 20 \
		--source-subpath-cmd "cat $subpath_list_file" --source-subpath-reload 2 \
		--readdir-cache 0 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "source-subpath-cmd: initial subpath accessible" 0 ls $mnt/fsmark
	echo /fsmark2 > $subpath_list_file
	sleep 4
	run_test "source-subpath-cmd: log shows reloaded subpath" 0 'grep "Reloaded new source subpath.*fsmark2" $main_log'
	umount $mnt
	rm -f $subpath_list_file
	kill $pid || true
	wait $pid

	#
	# freed-block reuse pool, own source dir so block reuse bugs are detectable
	#
	block_pool_sanity
}

# Sanity test for multiple source dirs (priority-ordered replicas). The second
# replica is a full copy of $source_dir (cp -a preserves sparse files and
# hardlinks). Replica failures are simulated with CFS_SOURCE_READ_FAULT, which
# injects infrastructure errnos into the source data read path only, so mounts
# and metadata operations keep working.
multi_source_test()
{
	local source_dir2=/tmp/source2
	rm -rf $source_dir2
	cp -a $source_dir $source_dir2

	#
	# two replicas, all reads served by the first (highest priority) one
	#
	log "Cachefs - multiple source dirs"
	$here/cachefs cache $options --source-dir $source_dir --extra-source-dirs $source_dir2 --cache-dir memory --cache-size 2048 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "multi_source_basic" 0 treesum -r /tmp/source.sum $mnt
	check_stat_value "multi_source_first_replica_reads" "cachefs_source_reads_per_source_$source_dir" ">" 0 $mnt/.stats
	run_test "multi_source_second_replica_idle" 1 "grep -q \"^cachefs_source_reads_per_source_$source_dir2 \" $mnt/.stats"
	check_stat_value "multi_source_active_dir_first" "cachefs_source_replica_active_source_$source_dir" "==" 1 $mnt/.stats
	check_stat_value "multi_source_active_dir_second_inactive" "cachefs_source_replica_active_source_$source_dir2" "==" 0 $mnt/.stats
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# first replica fails with EIO, reads fall back to the second one
	#
	# After the failover, attrs are served from the surviving replica; the
	# shared treesum snapshot intentionally skips ctime, which a replica copy
	# can never preserve.
	#
	# The replica is passed via the CFS_EXTRA_SOURCE_DIRS env var rather than
	# the flag: if the env wiring were broken, only one replica would exist
	# and the failover treesum below would fail with EIO.
	log "Cachefs - multiple source dirs, first replica fails"
	CFS_SOURCE_READ_FAULT=eio:$source_dir CFS_EXTRA_SOURCE_DIRS=$source_dir2 $here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 2048 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "multi_source_fault_injection_logged" 0 'grep "Env \"CFS_SOURCE_READ_FAULT\"" $main_log'
	run_test "multi_source_failover" 0 treesum -r /tmp/source.sum $mnt
	run_test "multi_source_failover_logged" 0 'grep "is marked failed" $main_log'
	check_stat_value "multi_source_outage_metric" "cachefs_source_replica_outage_total_$source_dir" ">" 0 $mnt/.stats
	check_stat_value "multi_source_second_replica_reads" "cachefs_source_reads_per_source_$source_dir2" ">" 0 $mnt/.stats
	check_stat_value "multi_source_active_dir_switched" "cachefs_source_replica_active_source_$source_dir2" "==" 1 $mnt/.stats
	check_stat_value "multi_source_active_dir_first_inactive" "cachefs_source_replica_active_source_$source_dir" "==" 0 $mnt/.stats
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# all replicas fail: mount still works (the fault injector doesn't touch
	# the metadata path) but reads fail; every replica reports its outage
	# exactly once
	#
	log "Cachefs - multiple source dirs, all replicas fail"
	CFS_SOURCE_READ_FAULT=eio:* $here/cachefs cache $options --source-dir $source_dir --extra-source-dirs $source_dir2 --cache-dir memory --cache-size 2048 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "multi_source_all_dead_read_fails" 1 'cat $mnt/sizes/1048576 > /dev/null'
	run_test "multi_source_all_dead_outages" 0 'test $(grep -c "is marked failed" $main_log) -eq 2'
	run_test "multi_source_all_dead_log_details" 0 'grep "is marked failed, read on /sizes/1048576" $main_log'
	umount $mnt
	kill $pid || true
	wait $pid

	#
	# a missing extra source dir only loses that replica: warn and mount
	# anyway; a missing primary source dir is a config error and must fail
	#
	log "Cachefs - multiple source dirs, missing extra dir tolerated"
	$here/cachefs cache $options --source-dir $source_dir --extra-source-dirs /nonexistent_source_dir --cache-dir memory --cache-size 2048 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "multi_source_missing_extra_log" 0 'grep "Ignoring extra source dir: stat source dir /nonexistent_source_dir" $main_log'
	run_test "multi_source_missing_extra_read" 0 'cat $mnt/sizes/1048576 > /dev/null'
	umount $mnt
	kill $pid || true
	wait $pid

	log "Cachefs - multiple source dirs, missing primary dir rejected"
	$here/cachefs cache $options --source-dir /nonexistent_source_dir --extra-source-dirs $source_dir2 --cache-dir memory --cache-size 2048 cachefs $mnt &> $main_log &
	pid=$!
	rc=0
	wait $pid || rc=$?
	run_test "multi_source_missing_dir_exit" 0 "test $rc -ne 0"
	run_test "multi_source_missing_dir_log" 0 'grep "stat source dir /nonexistent_source_dir" $main_log'

	#
	# a nested extra source dir is a config error: startup must fail
	#
	log "Cachefs - multiple source dirs, nested dir rejected"
	mkdir $source_dir/nested-replica
	$here/cachefs cache $options --source-dir $source_dir --extra-source-dirs $source_dir/nested-replica --cache-dir memory --cache-size 2048 cachefs $mnt &> $main_log &
	pid=$!
	rc=0
	wait $pid || rc=$?
	run_test "multi_source_nested_dir_exit" 0 "test $rc -ne 0"
	run_test "multi_source_nested_dir_log" 0 'grep "must not be the same or have a parent/child relationship" $main_log'
	rmdir $source_dir/nested-replica

	#
	# --extra-source-dirs requires --source-dir
	#
	log "Cachefs - multiple source dirs, extra without source-dir rejected"
	$here/cachefs cache $options --extra-source-dirs $source_dir2 --cache-dir memory --cache-size 2048 cachefs $mnt &> $main_log &
	pid=$!
	rc=0
	wait $pid || rc=$?
	run_test "multi_source_extra_only_exit" 0 "test $rc -ne 0"
	run_test "multi_source_extra_only_log" 0 'grep "extra-source-dirs requires --source-dir" $main_log'

	#
	# sourceless mode doesn't support multiple source dirs
	#
	log "Cachefs - multiple source dirs, sourceless rejected"
	$here/cachefs cache $options --source-dir $source_dir --extra-source-dirs $source_dir2 --sourceless --cache-dir memory --cache-size 2048 cachefs $mnt &> $main_log &
	pid=$!
	rc=0
	wait $pid || rc=$?
	run_test "multi_source_sourceless_exit" 0 "test $rc -ne 0"
	run_test "multi_source_sourceless_log" 0 'grep "Sourceless mode doesn.t support multiple source dirs" $main_log'

	#
	# a file present only in a lower-priority replica is ENOENT: semantic
	# errors are returned as-is, never rescued by another replica
	#
	log "Cachefs - multiple source dirs, incomplete replica"
	touch $source_dir2/only-in-second-replica
	$here/cachefs cache $options --source-dir $source_dir --extra-source-dirs $source_dir2 --cache-dir memory --cache-size 2048 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	run_test "multi_source_no_fallback_on_enoent" 1 'cat $mnt/only-in-second-replica'
	umount $mnt
	kill $pid || true
	wait $pid

	rm -rf $source_dir2
}

integration_test()
{
	#
	# IT1: cache + cache, cache dir == memory, transport == tcp
	#
	log "IT1: cache + cache, cache dir == memory, transport == tcp"
	$here/cachefs cache $options --source-dir $source_dir cachefs $mnt &
	pid=$!
	wait_mount $mnt
	$here/cachefs cache $peer_options --source-dir $source_dir cachefs $peer_mnt &
	pid_1=$!
	wait_mount $peer_mnt

	# check data integrity
	run_test "IT1: first read, all source" 0 "find $mnt -type f -print0 | xargs -0 -n1 -P16 -I% cat % >/dev/null"
	sleep 5
	run_test "IT1: second read, source and peer load" 0 treesum -r /tmp/source.sum $peer_mnt
	check_stat_value "check IT1 all source" "cachefs_remote_hits" "==" 0 $mnt/.stats
	check_stat_value "check IT1 source and peer load" "cachefs_remote_hits" ">" 0 $peer_mnt/.stats
	umount $mnt
	kill $pid || true
	wait $pid
	umount $peer_mnt
	kill $pid_1 || true
	wait $pid_1

	#
	# IT2: cache + cache, cache dir == filebackend, transport == tcp
	#
	log "IT2: cache + cache, cache dir == filebackend, transport == tcp"
	mkdir -p $cache_dir $peer_cache_dir
	$here/cachefs cache --free-space-ratio 0.001 $options --transport tcp --source-dir $source_dir --cache-dir $cache_dir cachefs $mnt &
	pid=$!
	wait_mount $mnt
	$here/cachefs cache --free-space-ratio 0.001 $peer_options --transport tcp --source-dir $source_dir --cache-dir $peer_cache_dir cachefs $peer_mnt &
	pid_1=$!
	wait_mount $peer_mnt

	# check data integrity
	run_test "IT2: first read, all source" 0 "find $mnt -type f -print0 | xargs -0 -n1 -P16 -I% cat % >/dev/null"
	sleep 5
	run_test "IT2: second read, source and peer load" 0 treesum -r /tmp/source.sum $peer_mnt
	check_stat_value "check IT2 all source" "cachefs_remote_hits" "==" 0 $mnt/.stats
	check_stat_value "check IT2 source and peer load" "cachefs_remote_hits" ">" 0 $peer_mnt/.stats
	umount $mnt
	kill $pid || true
	wait $pid
	umount $peer_mnt
	kill $pid_1 || true
	wait $pid_1

	#
	# IT3: daemon + cache
	#
	log "IT3: daemon + cache"
	$here/cachefs daemon $options &
	pid=$!
	wait_daemon localhost:7955/liveness

	$here/cachefs cache $peer_options --source-dir $source_dir cachefs $peer_mnt &
	pid_1=$!
	wait_mount $peer_mnt
	run_test "test fuse operation" 0 python3 $here/build/scripts/test_fuse.py
	kill $pid || true
	wait $pid
	umount $peer_mnt
	kill $pid_1 || true
	wait $pid_1

	#
	# IT4: check cache with safe cache key and unsafe cache key
	#
	log "IT4: check unsafe cache key"
	$here/cachefs cache $options --source-dir $source_dir --cache-size 2048 cachefs $mnt &
	pid=$!
	wait_mount $mnt
	$here/cachefs cache $peer_options --source-dir $source_dir --cache-size 2048 --unsafe-cache-key cachefs $peer_mnt &
	pid="$pid $!"
	wait_mount $peer_mnt

	run_test "populate cache with safe cache key" 0 "find $mnt -type f -print0 | xargs -0 -n1 -P16 -I% cat % >/dev/null"
	run_test "read on peer" 0 "find $peer_mnt -type f -print0 | xargs -0 -n1 -P16 -I% cat % >/dev/null"
	check_stat_value "all read on peer should back source" "cachefs_remote_hits" "==" 0 $peer_mnt/.stats
	{ umount $mnt || true; } &
	umount $peer_mnt
	kill $pid || true
	wait $pid

	#
	# IT5: sourceless
	#
	log "IT5: sourceless mode"
	$here/cachefs cache $options --source-dir $sourceless_dir --sourceless --enable-api-server $server_opt cachefs $mnt &
	pid=$!
	wait_mount $mnt
	run_test "set source ready" 0 $here/cachefs source-ready $mnt
	run_test "set source ready via api" 0 $here/cachefs source-ready $server_opt $mnt
	run_test "set source ready in background" 0 $here/cachefs source-ready -d $mnt
	run_test "sourceless meta provider data ready" 1 $here/cachefs apiclient data-ready $server_opt

	# remove mount point to make sure mnt will be created in sourceless mode
	rm -rf $peer_mnt
	$here/cachefs cache $peer_options --sourceless --enable-api-server $server_opt2 cachefs $peer_mnt &
	pid="$pid $!"
	wait_mount $peer_mnt
	# make sure we handle hardlink correctly, we may cache /linktest in meta and read the
	# link should not fail
	run_test "sourceless meta consumer read hardlink" 0 cat $peer_mnt/linktest.hardlink
	run_test "sourceless meta consumer check data" 0 treesum -r /tmp/sourceless.sum $peer_mnt
	run_test "sourceless meta consumer data ready" 0 $here/cachefs apiclient data-ready $server_opt2
	{ umount $mnt || true; } &
	umount $peer_mnt
	kill $pid || true
	wait $pid

	#
	# IT6: broadcast preload
	#
	log "IT6: broadcast preload request"
	$here/cachefs cache $options --source-dir $source_dir --enable-api-server $server_opt --cache-size 1024 cachefs $mnt &
	pid=$!
	wait_mount $mnt
	check_stat_value "check preload request on $mnt" "cachefs_vfs_api_loadcache" "==" 0 $mnt/.stats
	check_stat_value "check source read on $mnt" "cachefs_source_reads" "==" 0 $mnt/.stats

	$here/cachefs cache $peer_options --source-dir $source_dir --enable-api-server $server_opt2 --cache-size 1024 \
		cachefs $peer_mnt > >(tee $peer_log) 2>&1 &
	pid="$pid $!"
	wait_mount $peer_mnt
	check_stat_value "check preload request on $peer_mnt" "cachefs_vfs_api_loadcache" "==" 0 $peer_mnt/.stats
	check_stat_value "check source read on $peer_mnt" "cachefs_source_reads" "==" 0 $peer_mnt/.stats

	# trigger and broadcast preload on first instance
	run_test "broadcast preload" 0 $here/cachefs preload $server_opt -b $mnt
	run_test "broadcast preload via api" 0 $here/cachefs preload -b $mnt
	check_stat_value "check source read after prelad on $mnt" "cachefs_source_reads" ">" 0 $mnt/.stats
	check_stat_value "check preload request after preload on $mnt" "cachefs_vfs_api_loadcache" "==" 1 $mnt/.stats

	# check preload status on second instance, broadcasted serf event won't increase vfs api counter
	check_stat_value "check source read after preload on $peer_mnt" "cachefs_source_reads" ">" 0 $peer_mnt/.stats
	check_stat_value "check preload request after preload on $peer_mnt" "cachefs_vfs_api_loadcache" "==" 0 $peer_mnt/.stats
	run_test "check preload serf event" 0 'grep "Received preload broadcast from" $peer_log'

	{ umount $mnt || true; } &
	umount $peer_mnt
	kill $pid || true
	wait $pid

	#
	# IT7: check two peers reload round 2: stale stop signal
	#
	log "IT7: check two peers reload round 2: stale stop signal"
	find "$sourceless_dir" -type f -print0 | xargs -0 cat > /dev/null
	$here/cachefs cache $options --source-dir $sourceless_dir --block-size 4 --cache-size 16 --unsafe-cache-key --cache-expire 3 --preload / --preload-interval 2 cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	$here/cachefs cache $peer_options --source-dir $sourceless_dir --block-size 4 --cache-size 16 --unsafe-cache-key --cache-expire 3 --preload / --preload-interval 2 cachefs $peer_mnt > >(tee $peer_log) 2>&1 &
	pid="$pid $!"
	wait_mount $peer_mnt

	sleep 6
	# log example: Preload scanned 0MiB data from 0/0 files 0/0 blocks
	run_test "stop_signal_log_check" 1 'grep "0/0 files 0/0 blocks" $main_log'
	run_test "peer_stop_signal_log_check" 1 'grep "0/0 files 0/0 blocks" $peer_log'
	umount $mnt
	umount $peer_mnt
	kill $pid || true
	wait $pid

	#
	# IT8: broadcast preload --cancel
	#
	log "IT8: broadcast preload cancel request"
	$here/cachefs cache $options --source-dir $source_dir --enable-api-server $server_opt cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	$here/cachefs cache $peer_options --source-dir $source_dir --enable-api-server $server_opt2 \
		cachefs $peer_mnt > >(tee $peer_log) 2>&1 &
	pid="$pid $!"
	wait_mount $peer_mnt

	# trigger and broadcast preload on first instance
	run_test "broadcast preload cancel without progress" 0 $here/cachefs preload $server_opt -b -c
	sleep 0.2
	run_test "broadcast preload" 0 $here/cachefs preload $server_opt -b -d $mnt
	run_test "broadcast preload cancel with progress" 0 $here/cachefs apiclient preload $server_opt -b -c
	sleep 0.5
	run_test "check cancel preload api server" 0 'grep "API Server: Cancel preload, broadcast=true" $main_log'
	run_test "check preload cancellation serf event" 0 'grep "Received preload cancellation broadcast from" $peer_log'
	run_test "check cancel preload without progress" 0 'grep "No preload in progress, cancel request ignored" $main_log'
	run_test "check cancel preload without progress" 0 'grep "No preload in progress, cancel request ignored" $main_log'
	run_test "check cancel preload done" 0 'grep "Current preload cancelled" $main_log'

	{ umount $mnt || true; } &
	umount $peer_mnt
	kill $pid || true
	wait $pid

	#
	# IT9: peer meta test
	#
	log "IT9: load source dir meta from peer - peer meta"
	$here/cachefs cache $options --source-dir $source_dir --enable-api-server $server_opt \
		--source-peermeta 1 cachefs $mnt &
	pid=$!
	wait_mount $mnt

	# set meta ready, so peer could load meta from it
	run_test "set meta ready" 0 $here/cachefs apiclient meta-ready $server_opt

	# start peer and enable preload, should preload from "cached filesystem meta"
	$here/cachefs cache $peer_options --source-dir $source_dir --preload / --source-peermeta 1 \
		cachefs $peer_mnt > >(tee $peer_log) 2>&1 &
	pid="$pid $!"
	wait_mount $peer_mnt
	run_test "check preload base dir" 0 'grep "Preload base dir: cached filesystem meta" $peer_log'

	# sleep a while to avoid EBUSY on umount
	sleep 1
	{ umount $mnt || true; } &
	umount $peer_mnt
	kill $pid || true
	wait $pid

	#
	# IT10: serf group mismatch - nodes with a different serf group are rejected,
	# and the rejected addresses are printed in the summary log.
	#
	log "IT10: serf group mismatch via memberlist-cmd"
	local memberlist_script=/tmp/serf_memberlist_grp.sh

	# Instance 1: serf-group=groupA
	$here/cachefs cache --cache-nic lo --cache-port 7788 --serf-port 7799 \
		--liveness 127.0.0.1:7955 --serf-leave-delay 0 --serf-group groupA \
		--source-dir $source_dir cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# Script that outputs instance 1's serf address.
	printf '#!/bin/sh\necho "127.0.0.1:7799"\n' > $memberlist_script
	chmod +x $memberlist_script

	# Instance 2: serf-group=groupB, discovers instance 1 via memberlist-cmd.
	$here/cachefs cache --cache-nic lo --serf-node-name test-peer \
		--cache-port 7787 --serf-port 7798 --liveness 127.0.0.1:7956 \
		--serf-leave-delay 0 --serf-group groupB \
		--serf-memberlist-cmd $memberlist_script \
		--source-dir $source_dir cachefs $peer_mnt > >(tee $peer_log) 2>&1 &
	pid="$pid $!"
	wait_mount $peer_mnt

	# The first memberlist-cmd attempt fires immediately; wait briefly for it to complete.
	sleep 3

	# Instance 2 should log the mismatch summary including instance 1's address.
	run_test "group mismatch rejection logged with address" 0 \
		'grep "members rejected due to group mismatch.*127.0.0.1:7799" $peer_log'

	# Instance 1 should NOT log any "Failed push/pull merge" ERR from memberlist,
	# verifying that groupMismatchLogFilter suppresses the noise.
	run_test "group mismatch ERR suppressed on instance 1" 1 \
		'grep "Failed push/pull merge.*member not in same group" $main_log'

	{ umount $mnt || true; } &
	umount $peer_mnt
	kill $pid || true
	wait $pid
	rm -f $memberlist_script

	#
	# IT11: periodic preload with full cache triggers hash-hit eviction stop
	#
	log "IT11: periodic preload stops when non-pinned hash-hit blocks are evicted from full cache"
	# Use a small cache (512 MiB) and CFS_PRELOAD_ENTRY_SIZE=1 so that
	# stageEntries hold only one file at a time; this ensures stopWalkDir is
	# checked frequently enough to catch the rising blockcache_evicts_hashhit
	# counter while the directory walk is still in progress.
	# Instance A: memory backend
	CFS_PRELOAD_ENTRY_SIZE=1 $here/cachefs cache $options --source-dir $source_dir \
		--cache-dir memory --cache-size 512 --preload / --preload-interval 3 \
		cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	# Instance B: disk backend
	mkdir -p $cache_dir
	CFS_PRELOAD_ENTRY_SIZE=1 $here/cachefs cache $peer_options --source-dir $source_dir \
		--cache-dir $cache_dir --cache-size 512 --preload / --preload-interval 3 \
		cachefs $peer_mnt > >(tee $peer_log) 2>&1 &
	pid="$pid $!"
	wait_mount $peer_mnt

	# Give periodic preload some time to run at least 2-3 rounds.  With a
	# 512 MiB cache and a multi-GiB source, the cache fills quickly and
	# eviction of hash-hit blocks is expected within the first round.
	sleep 5
	run_test "IT11: mem instance stop reason logged" 0 \
		'grep "Preload stop walking dir: non-pinned hash-hit blocks being evicted" $main_log'
	run_test "IT11: disk instance stop reason logged" 0 \
		'grep "Preload stop walking dir: non-pinned hash-hit blocks being evicted" $peer_log'
	umount $mnt
	umount $peer_mnt
	kill $pid || true
	wait $pid

	#
	# IT12: periodic preload with full cache triggers pinned hash-hit eviction stop
	#
	log "IT12: periodic preload stops when pinned hash-hit blocks are evicted from full cache"
	CFS_PRELOAD_ENTRY_SIZE=1 $here/cachefs cache $options --source-dir $source_dir \
		--cache-dir memory --cache-size 512 --preload / --preload-interval 3 \
		--pin-cache /sizes cachefs $mnt > >(tee $main_log) 2>&1 &
	pid=$!
	wait_mount $mnt

	mkdir -p $cache_dir
	CFS_PRELOAD_ENTRY_SIZE=1 $here/cachefs cache $peer_options --source-dir $source_dir \
		--cache-dir $cache_dir --cache-size 512 --preload / --preload-interval 3 \
		--pin-cache /sizes cachefs $peer_mnt > >(tee $peer_log) 2>&1 &
	pid="$pid $!"
	wait_mount $peer_mnt

	sleep 5
	check_stat_value "IT12: mem blockcache_evicts_pinned_hashhit > 0" \
		"cachefs_blockcache_evicts_pinned_hashhit" ">" 0 $mnt/.stats
	check_stat_value "IT12: disk blockcache_evicts_pinned_hashhit > 0" \
		"cachefs_blockcache_evicts_pinned_hashhit" ">" 0 $peer_mnt/.stats
	run_test "IT12: mem instance stop reason logged" 0 \
		'grep "Preload stop walking dir: pinned hash-hit blocks being evicted" $main_log'
	run_test "IT12: disk instance stop reason logged" 0 \
		'grep "Preload stop walking dir: pinned hash-hit blocks being evicted" $peer_log'
	umount $mnt
	umount $peer_mnt
	kill $pid || true
	wait $pid

	#
	# IT13: cacheroot cluster management across two nodes.
	# The main node exposes the cacheroot-port and acts as coordinator; both nodes
	# join the same serf cluster. GetCacheStats fans out to every serf member's
	# cachePort, while GetCacheRatio judges cluster coverage from the coordinator's
	# gossiped tracker view.
	#
	log "IT13: cacheroot cluster management (stats/ratio across two nodes)"
	$here/cachefs cache $options --source-dir $source_dir --cache-dir memory --cache-size 2048 \
		--cacheroot-port 8833 --cacheroot-tracker localhost:8833 cachefs $mnt > >(tee $cacheroot_log) 2>&1 &
	pid=$!
	wait_mount $mnt
	$here/cachefs cache $peer_options --source-dir $source_dir --cache-dir memory --cache-size 2048 \
		cachefs $peer_mnt > >(tee $peer_log) 2>&1 &
	pid="$pid $!"
	wait_mount $peer_mnt

	# Populate cache on both nodes, then let gossip publish block ownership into
	# the coordinator's tracker view (5s matches the gossip settle time used above).
	run_test "IT13: preload on main" 0 $here/cachefs preload $mnt
	run_test "IT13: read on peer" 0 "find $peer_mnt -type f -print0 | xargs -0 -n1 -P16 -I% cat % >/dev/null"
	sleep 5

	# stats: both serf members are reported and healthy.
	$here/cachefs cacheroot stats --addr localhost:8833 -f json > /tmp/cacheroot_stats_it13.json
	run_test "IT13: stats lists both nodes ok" 0 \
		'python3 -c "import json; d=json.load(open(\"/tmp/cacheroot_stats_it13.json\")); assert len(d)==2 and all(x[\"ok\"] and x[\"cached_blocks\"]>0 for x in d), d"'

	# ratio: cluster coverage of the whole tree is non-zero once gossip settled.
	$here/cachefs cacheroot ratio / --relative --addr localhost:8833 -f json > /tmp/cacheroot_ratio_it13.json
	run_test "IT13: ratio cluster cached > 0" 0 \
		'python3 -c "import json; d=json.load(open(\"/tmp/cacheroot_ratio_it13.json\")); assert len(d)==1 and d[0][\"ok\"] and d[0][\"total_blocks\"]>0 and d[0][\"cached_blocks\"]>0, d"'

	# ratio --list-nodes: per-node breakdown is populated (at least the coordinator).
	$here/cachefs cacheroot ratio / --relative --list-nodes --addr localhost:8833 -f json > /tmp/cacheroot_ratio_nodes_it13.json
	run_test "IT13: ratio per-node breakdown populated" 0 \
		'python3 -c "import json; d=json.load(open(\"/tmp/cacheroot_ratio_nodes_it13.json\")); r=d[0]; assert r.get(\"nodes\") and all(n[\"cached_blocks\"]>0 for n in r[\"nodes\"]), r"'

	# cachestate tracker: the coordinator's local tracker (metaMap), populated via
	# gossip, exposes which nodes hold which blocks. Query the CachefsState service
	# on the coordinator's cachePort (7788): at least one target has a node with
	# blocks once the cluster view settled.
	$here/cachefs cachestate tracker / --relative --addr localhost:7788 -f json > /tmp/cachestate_tracker_it13.json
	run_test "IT13: cachestate tracker populated" 0 \
		'python3 -c "import json; d=json.load(open(\"/tmp/cachestate_tracker_it13.json\")); assert len(d)>=1 and any(any(n[\"blocks\"] for n in t[\"nodes\"]) for t in d), d"'

	# cachestate tracker -l: the coordinator's own authoritative cache distribution
	# (local block cache). Since the coordinator has cached blocks, at least one
	# target reports a node holding blocks.
	$here/cachefs cachestate tracker / --relative -l --addr localhost:7788 -f json > /tmp/cachestate_tracker_local_it13.json
	run_test "IT13: cachestate tracker local populated" 0 \
		'python3 -c "import json; d=json.load(open(\"/tmp/cachestate_tracker_local_it13.json\")); assert len(d)>=1 and any(any(n[\"blocks\"] for n in t[\"nodes\"]) for t in d), d"'

	run_test "IT13: cacheroot log check" 1 grep -e ERROR -e FATAL $cacheroot_log

	rm -f /tmp/cacheroot_stats_it13.json /tmp/cacheroot_ratio_it13.json \
		/tmp/cacheroot_ratio_nodes_it13.json /tmp/cachestate_tracker_it13.json \
		/tmp/cachestate_tracker_local_it13.json
	umount $mnt
	umount $peer_mnt
	kill $pid || true
	wait $pid
}

# Checks whether OOT FUSE module is supported.
# Conditions:
#   - FUSE module must be from the 'extra' directory
#   - Kernel version must be >= 5.2 (for fsopen/fsconfig syscalls)
# Returns:
#   0 (true) - both conditions satisfied
#   1 (false) - otherwise
check_fuse_oot_support() {
	# assume extra fuse.ko is ours
	if ! modinfo fuse | grep -q 'filename:.*/extra/fs/fuse/fuse.ko'; then
		log "FUSE module not found in 'extra' directory."
		return 1
	fi

	# parse kernel version: major.minor (e.g., 5.10)
	local kernel_version_str=$(uname -r)
	local major=$(echo "$kernel_version_str" | awk -F. '{print $1}')
	local minor=$(echo "$kernel_version_str" | awk -F. '{print $2}')

	if ! [[ "$major" =~ ^[0-9]+$ ]] || ! [[ "$minor" =~ ^[0-9]+$ ]]; then
		log "Failed to parse kernel version: $kernel_version_str"
		return 1
	fi

	if [ "$major" -lt 5 ] || { [ "$major" -eq 5 ] && [ "$minor" -lt 2 ]; }; then
		log "Kernel version $major.$minor < 5.2, fsopen/fsconfig not supported."
		return 1
	fi

	log "OOT FUSE supported: kernel $major.$minor >= 5.2 and using extra/fuse.ko"
	return 0
}

# generate test source dir, create files with various types and sizes
gen_test_src()
{
	local src=$1
	local lite=${2:-false}
	mkdir -p $src
	local off=0
	local cnt=0
	local blksz=4194304
	local sizes="1 10 4095 4096 4097 1048576 1048577 4194304 4194305 1073741824 1073741825 4294967296 4294967297"
	if $lite; then
		sizes="1 10 4095 4096 4097 1048576 1048577 4194304 4194305 14194305"
	fi

	log "Generating import source dir $src"

	# use fs_mark to generate various dir hierarchies randomly. Run it from a
	# scratch cwd: besides stdout, fs_mark appends its report to ./fs_log.txt,
	# which would otherwise be dropped in the repo, as the container's cwd is
	# the bind-mounted source tree.
	(cd /tmp && fs_mark -k -s 128 -D 20 -N 10 -L 1 -p 16 -r 4 -t 4 -w 3 -d $src/fsmark)

	# create files with various sizes
	mkdir $src/sizes
	touch $src/sizes/emptyfile
	for s in $sizes; do
		truncate -s $s $src/sizes/$s
		cnt=$((s / blksz))
		if [ $s -gt 4000 ]; then
			for i in $(seq 0 $cnt); do
				off=$((RANDOM % blksz))
				off=$((i*blksz+off))
				xfs_io -c "pwrite $off 2" $src/sizes/$s >/dev/null 2>&1
			done
		else
			xfs_io -c "pwrite 0 1" $src/sizes/$s >/dev/null 2>&1
		fi
		truncate -s $s $src/sizes/$s
	done

	# create files with various types
	mkdir $src/testdir
	echo "symlink" > $src/symlinktest
	ln -sf symlinktest $src/symlinktest.symlink
	echo "escaped symlink target" > $src/escape%20symlinktest
	ln -sf escape%20symlinktest $src/escape%20symlinktest.symlink
	echo "hardlink" > $src/linktest
	ln $src/linktest $src/linktest.hardlink
	echo "odd name" > $src/test_\#\ name-with\?\!strangetchar%%20\(\){}\[\]\`~\=+\|\\
	mkdir $src/dev
	mknod $src/dev/chardev c 1 3
	mknod $src/dev/blkdev b 0 0
	mknod $src/dev/pipe p
	socat UNIX-LISTEN:$src/dev/socket.new - &
	sleep 1
	mv $src/dev/socket.new $src/dev/socket
	kill $!
}

# gen_block_pool_src generates the source dir for block_pool_sanity.
#
# The shared $source_dir cannot detect block reuse bugs: its files are almost
# entirely holes, so a block still holding some other block's bytes and a
# correctly filled block are both zeros. Here every 4MiB block differs from
# every other one, which makes any stale byte a checksum mismatch.
gen_block_pool_src()
{
	local src=$1
	rm -rf $src
	mkdir -p $src/files
	# 4 whole blocks each, mapped through the vfs API below
	for i in 1 2 3 4; do
		dd if=/dev/urandom of=$src/files/f$i bs=1M count=16 status=none
	done
	# read in a loop to keep the cache full, so blocks are evicted (hence freed
	# into the pool) for the whole test instead of only at the start
	dd if=/dev/urandom of=$src/churn bs=1M count=512 status=none
}

# check that every --long-option in './cachefs cache -h' is present in
# safe_cachefs.sh's long_opts= line.  This catches flags added to flags.go
# but forgotten in the wrapper script.
check_safe_cachefs_long_opts()
{
	local ret=0
	local tmp_help=/tmp/safe_cachefs_help_flags.$$
	local tmp_opts=/tmp/safe_cachefs_long_opts.$$

	# Extract --long-name flags from cache -h (ignore short-only like -o, -d)
	$here/cachefs cache -h | grep -oE -- '--[a-z][a-z0-9-]*' | sed 's/^--//' | sort -u > "$tmp_help"

	# Extract entries from "long_opts=", strip trailing ':' (required argument) and '::' (optional argument).
	grep '^long_opts=' "$here/build/scripts/safe_cachefs.sh" | sed 's/^long_opts="//; s/"$//' | tr ',' '\n' | sed 's/:*$//' | sort -u > "$tmp_opts"

	# Known exceptions: wrapper handles these via short options or specially
	local exceptions="background"

	local missing=""
	while read -r flag; do
		[ -z "$flag" ] && continue
		# Skip known exceptions
		local skip=false
		for ex in $exceptions; do
			if [ "$flag" = "$ex" ]; then
				skip=true
				break
			fi
		done
		$skip && continue

		if ! grep -qx "$flag" "$tmp_opts"; then
			missing="$missing --$flag"
			ret=1
		fi
	done < "$tmp_help"

	rm -f "$tmp_help" "$tmp_opts"

	if [ $ret -ne 0 ]; then
		log "FAIL: safe_cachefs.sh long_opts missing flags:$missing"
	fi
	return $ret
}

# Golang test style output, e.g:
#   === RUN   TestName
#   --- PASS: TestName (Xs)
#   --- FAIL: TestName (Ys)
#
# Arguments:
#   $1: Test name (e.g., TestAtime)
#   $2: Expected exit code (0 for success, 1 for failure)
#   $@: Command to run (the actual test)
#
# Returns:
#   0 if test result matches expected exit code (PASS)
#   1 if test result does not match expected exit code (FAIL)
run_test()
{
	local tname=$1
	local expect=$2
	shift 2
	log "Start $tname: $@"
	echo "=== RUN   $tname"
	local start=$(date +%s)
	eval $@
	local result=$?
	local end=$(date +%s)
	if [ $result -ne 0 ]; then
		result=1
	fi
	if [ $result -ne $expect ]; then
		result=1
	else
		result=0
	fi
	if [ $result -ne 0 ]; then
		smoke_result=$result
		echo "--- FAIL: $tname ($((end-start))s)"
	else
		echo "--- PASS: $tname ($((end-start))s)"
	fi
	return $result
}

# Usage: check_stat_value TESTNAME STAT_KEY OPERATOR EXPECTED [STAT_FILE] [WAIT_SECONDS]
check_stat_value() {
	local tname=$1
	local stat_key=$2
	local operator=$3
	local compare_value=$4
	local stat_file=${5:-stats.log}
	local wait_seconds=${6:-0}
	local deadline=$((SECONDS + wait_seconds))
	local val result

	case $operator in
		"==") op="==" ;;
		"!=") op="!=" ;;
		">") op="-gt" ;;
		"<") op="-lt" ;;
		">=") op="-ge" ;;
		"<=") op="-le" ;;
		*) echo "--- ERROR: $tname (Invalid operator '$operator')"
		   smoke_result=1
		   return 1 ;;
	esac

	echo "=== RUN   $tname"
	while :; do
		val=$(awk -v key="^$stat_key " '$0 ~ key {print $2}' "$stat_file" 2>/dev/null)
		result=$?
		if [ $result -eq 0 ] && [ -n "$val" ] && [ "$val" $op "$compare_value" ]; then
			echo "--- PASS: $tname (${val} ${operator} ${compare_value})"
			return 0
		fi
		if [ $SECONDS -ge $deadline ]; then
			break
		fi
		sleep 0.1
	done

	if [ $result -ne 0 ] || [ -z "$val" ]; then
		echo "--- FAIL: $tname (key '$stat_key' or file '$stat_file' not found)"
	else
		echo "--- FAIL: $tname (${val} ${operator} ${compare_value}: False)"
	fi
	smoke_result=1
	return 1
}

# check_store_config: read Store.<field> from $mnt/.config (JSON) and compare.
# Usage: check_store_config TESTNAME FIELD OPERATOR EXPECTED [CONFIG_FILE]
# FIELD is the key under "Store" in .config, e.g. CacheSize or BurstCacheSize.
# Note: Meta also has a CacheSize; this function reads only Store.<field>.
check_store_config() {
	local tname=$1
	local field=$2
	local operator=$3
	local expected=$4
	local config_file=${5:-${mnt}/.config}

	echo "=== RUN   $tname"

	local val
	val=$(python3 -c "
import json, sys
with open('${config_file}') as f:
    d = json.load(f)
print(d['Store']['${field}'])
" 2>/dev/null)
	if [ $? -ne 0 ] || [ -z "$val" ]; then
		echo "--- FAIL: $tname (failed to read Store.${field} from ${config_file})"
		smoke_result=1
		return 1
	fi

	case $operator in
		"==") op="-eq" ;;
		"!=") op="-ne" ;;
		">")  op="-gt" ;;
		"<")  op="-lt" ;;
		">=") op="-ge" ;;
		"<=") op="-le" ;;
		*) echo "--- ERROR: $tname (Invalid operator '$operator')"
		   smoke_result=1
		   return 1 ;;
	esac

	if [ "$val" $op "$expected" ]; then
		echo "--- PASS: $tname (Store.${field}=${val} ${operator} ${expected})"
		return 0
	else
		echo "--- FAIL: $tname (Store.${field}=${val} ${operator} ${expected}: False)"
		smoke_result=1
		return 1
	fi
}

# not in docker, start docker and run test script
if [ "$DOCKER_ENV" != "true" ]; then
	source build/common
	image=alibaba-cloud-linux-3-registry.cn-hangzhou.cr.aliyuncs.com/alinux3/alinux3:3.11

	if ! which docker >/dev/null 2>&1; then
		log "No docker cmd installed, aborting"
		exit 1
	fi

	tmpfs_mount=""
	if [ "$USETMPFS" != "" ]; then
		if [[ "$USETMPFS" =~ ^[0-9]+[GMK]?$ ]]; then
			size_spec="$USETMPFS"
		else
			size_spec="4G"
		fi
		tmpfs_mount="--tmpfs /tmp:rw,exec,size=$size_spec"
	fi

	gocover_volume=""
	if [ "$DEBUG" != "false" ]; then
		gocover_dir=/gocover
		rm -rf "$PWD/gocover"
		mkdir -p "$PWD/gocover"
		gocover_volume="-v $PWD/gocover:$gocover_dir"
	fi

	if [ "$PREPARE_ENV" != "" ]; then
		prepare_env_arg=" -e PREPARE_ENV=true"
	fi
	docker run --privileged --rm -it \
		-v /dev/fuse:/dev/fuse \
		-v $PWD:$workdir \
		$gocover_volume \
		-w $workdir \
		-e DOCKER_ENV=true --net host $prepare_env_arg \
		$tmpfs_mount \
		$image ./build/scripts/$(basename "$0")
	exit $?
fi

# in docker env, run the real test, create /.dockerenv so we could skip some
# tests not suitable in docker
touch /.dockerenv

# install build and test dependencies (the smoke image is a plain base OS)
./build/scripts/dnf-install.sh golang make cmake git gcc gcc-c++ \
	libstdc++-static rpm openssl-devel fuse3-devel fuse3-libs rdma-core-devel \
	fuse autoconf xfsprogs vmtouch fs_mark tree socat sudo which python3.11

# config go proxy and install goimports
go env -w GOPROXY="https://goproxy.cn,direct"
go install golang.org/x/tools/cmd/goimports@v0.15.0
# Install gocovmerge from a throwaway module with both it and x/tools pinned:
# `go get` in the repository module would rewrite this repo's go.mod/go.sum
# (and could bump shared deps of the code under test), while the @version
# install form floats x/tools to a latest that may need a newer toolchain.
covtools_dir=$(mktemp -d)
(cd $covtools_dir && go mod init covtools \
	&& go get github.com/wadey/gocovmerge@v0.0.0-20160331181800-b5bfa59ec0ad golang.org/x/tools@v0.15.0 \
	&& go install github.com/wadey/gocovmerge)
rm -rf $covtools_dir
export PATH="$PATH:$(go env GOPATH)/bin"

set +e
export EXTRA_FLAGS="-buildvcs=false -cover"
export GOCOVERDIR=$gocover_dir
mkdir -p $gocover_dir

if [ "$PREPARE_ENV" == "true" ]; then
	bash
	exit
fi

#
# go vet
#
run_test "go_vet" 0 make vet

#
# goimports
#
run_test "goimports" 0 make fmt.check

#
# basic build test
#
git config --global --add safe.directory $workdir
git submodule update --init --recursive
make clean

run_test "build" 0 make cachefs

#
# run coro-rpc unit test
#
rm -rf coro_rpc_lib/build coro_rpc_lib/libcoro_rpc.a
run_test "build_coro_test" 0 CORO_RPC_FLAGS="-DENABLE_TEST=ON" make cachefs
run_test "coro_rpc_test" 0 ./coro_rpc_lib/build/test_rpc_lib

#
# run unit test
#
run_test "unit_test" 0 make test

#
# run benchmark
#
run_test "benchmark_test" 0 make benchmark

#
# Prepare sourece dir for cachefs and import test
#
log "Prepare source dir"
install_treesum
rm -rf $source_dir $sourceless_dir
mkdir -p $source_dir $sourceless_dir
gen_test_src $source_dir
gen_test_src $sourceless_dir true
log "Generating treesum manifests"
# No ctime check ('c'): replicas of this tree (multi_source_test's cp -a copy)
# get fresh ctimes from the kernel at copy time, so after a source dir failover
# attrs served from a replica would mismatch on ctime. cp -a does preserve
# uid/gid/mode/mtime, so those stay checked.
treesum -w /tmp/source.sum $source_dir
treesum -fields ugomcd -w /tmp/sourceless.sum $sourceless_dir

#
# Run sanity test for cachefs
#
sanity_test

#
# Run multiple source dirs test for cachefs
#
multi_source_test

#
# Run integration test for cachefs
#
integration_test

#
# Run regression test for cachefs
#
regression_test

#
# Make sure we're not missing new options in safe_cachefs.sh
#
run_test "check_safe_cachefs_long_opts" 0 check_safe_cachefs_long_opts

run_test "textfmt IT covdata" 0 go tool covdata textfmt -i=$gocover_dir -o=cov-cachefs2.out

gocovmerge cov-cachefs1.out cov-cachefs2.out > merged-coverage.out

python3 $here/build/scripts/total-cov.py

exit $smoke_result
