#!/bin/bash

# A wrapper script to start cachefs and bindmount source dir to cachefs
# mountpoint if cachefs exits abnormally.

status=0
tmp=/tmp/$$
fuse_conn_id=""
cleanup_done=false

cleanup()
{
	# Prevent cleanup from being executed multiple times
	if [ "$cleanup_done" = true ]; then
		return
	fi
	cleanup_done=true

	info "cachefs exits, umount $mp"
	umount $mp || umount -l $mp

	# Check and abort FUSE connection if it still exists
	if [ -n "$fuse_conn_id" ]; then
		check_and_abort_fuse_conn "$mp" "$fuse_conn_id" 5
	fi

	rm -f $tmp.*
	exit $status
}

trap cleanup 0 1 2 3 15

logid=""
log()
{
	if [ "$logid" == "" ]; then
		echo -e "`date`: $@"
	else
		echo -e "[$logid] `date`: $@"
	fi
}
info()
{
	log "[INFO] $@"
}
warn()
{
	log "[WARN] $@"
}
err()
{
	log "[ERROR] $@"
}

wait_cachefs_mount()
{
	local mnt=$1
	local max=$2
	local cnt=0
	local fstype=""
	while true; do
		fstype=$(findmnt -n -o FSTYPE -f -d backward $mnt)
		if [ "$fstype" == "fuse.cachefs" ]; then
			echo -n "true"
			return 0
		elif [ $cnt -lt $max ]; then
			sleep 1
			cnt=$((cnt+1))
		else
			echo -n "false"
			return
		fi
	done
}

# get_fuse_conn_id retrieves the FUSE connection ID from a mount point.
# Returns the connection ID (minor number) or empty string on failure.
get_fuse_conn_id()
{
	local mnt=$1

	# Get device ID of the mount point
	local dev_id
	dev_id=$(stat -c '%d' "$mnt" 2>/dev/null)
	if [ $? -ne 0 ] || [ -z "$dev_id" ]; then
		warn "Failed to get device ID for $mnt"
		echo ""
		return 1
	fi

	# Extract minor number using the same logic as Go's devMinor function
	# minor = (dev & 0xff) | ((dev >> 12) & 0xffffff00)
	local conn_id=$(( (dev_id & 0xff) | ((dev_id >> 12) & 0xffffff00) ))

	echo "$conn_id"
	return 0
}

# abort_fuse_conn aborts all pending FUSE requests by writing 1 to /sys/fs/fuse/connections/<id>/abort
abort_fuse_conn()
{
	local conn_id=$1
	local abort_path="/sys/fs/fuse/connections/${conn_id}/abort"

	if [ ! -e "$abort_path" ]; then
		warn "FUSE connection abort path $abort_path does not exist"
		return 1
	fi

	if ! echo 1 > "$abort_path"; then
		warn "Failed to abort FUSE connection $conn_id"
		return 1
	fi

	info "Successfully aborted FUSE connection $conn_id"
	return 0
}

# check_and_abort_fuse_conn checks if FUSE connection still exists after timeout, and aborts if needed
check_and_abort_fuse_conn()
{
	local mnt=$1
	local conn_id=$2
	local timeout=${3:-10}

	info "FUSE connection ID for $mnt is $conn_id, waiting ${timeout}s for cleanup"

	# Wait for the connection to disappear, check every 100ms
	local abort_path="/sys/fs/fuse/connections/${conn_id}/abort"
	local interval_ms=100
	local max_checks=$((timeout * 1000 / interval_ms))
	local checks=0
	while [ $checks -lt $max_checks ]; do
		if [ ! -e "$abort_path" ]; then
			# Calculate elapsed time: checks * 100ms, display as X.Y seconds
			local total_ms=$((checks * interval_ms))
			local seconds=$((total_ms / 1000))
			local tenths=$(( (total_ms % 1000) / 100 ))
			info "FUSE connection $conn_id has been cleaned up after ${seconds}.${tenths}s"
			return 0
		fi
		sleep 0.1
		checks=$((checks + 1))
	done

	# Connection still exists after timeout, abort it
	warn "FUSE connection $conn_id still exists after ${timeout}s, aborting to prevent hung tasks"
	abort_fuse_conn "$conn_id"
	return 0
}

# Parse all options supported by cachefs. Only the following listed options are
# not cachefs native options, but for this wrapper script:
#
# --auto-unmount: auto umount cachefs mount if cachefs exited/crashed, don't
#                 bindmount source dir to cachefs mount point
#
# Note that we use "$@" to let each command-line parameter expand to a
# separate word. The quotes around "$@" are essential!
# We need parsed_options as the 'eval set --' would nuke the return value of getopt.
long_opts="no-syslog,log:,source-dir:,extra-source-dirs:,block-size:,buffer-size:,buffer-reuse::,buffer-prefault::,cache-dir:,cache-size:,prefetch:,prefetch-workers:,free-space-ratio:,cleanup-cache,upload-limit:,download-limit:,total-limit:,single-nic-limit:,peer-timeout:,cache-nic:,cache-port:,serf-join:,serf-memberlist-cmd:,readdir-cache:,attr-cleanup,metaop-timeout:,socket:,source-id:,metrics:,cache-eviction:,help,debug,quiet,trace,log-id:,no-agent,pyroscope:,no-color,auto-unmount,serf-port:,serf-initnode,preload:,checksum:,serf-group:,serf-reconnect-timeout:,serf-leave-delay:,buffer-timeout:,serf-maxretry:,transport:,thp-policy:,no-pagecache,preload-workers:,rdma-buffer-count:,rdma-buffer-size:,rdma-max-mem:,rdma-fallback-tcp,rdma-group:,transport-nic:,serf-node-name:,cacheroot-tracker:,cacheroot-port:,blacklist-time:,source-subpath:,source-subpath-file:,source-subpath-cmd:,key-prefix:,liveness:,preload-interval:,source-subpath-reload:,cache-expire:,cacheroot-transport:,cacheroot-nic:,unsafe-cache-key,sourceless,enable-api-server,api-server-sock:,share-reserve:,virtual-node-count:,writeback-buffer:,writeback-worker:,negative-timeout:,exchange-interval:,source-peermeta:,peermeta-mode:,force-preload,burst-cache-size:,defer-burst,pin-cache:,pin-cache-file:,pin-cache-cmd:,pin-cache-reload:,source-direct-io,cache-read-direct-io,cache-write-direct-io,go-gc:,go-memlimit:"
parsed_options=$(getopt -n "$0" -o 'dho:qv' --long "$long_opts" -- "$@")
if [ $? -ne 0 ]; then
	status=1
	exit $status
fi

# If we run this script with the following options to mount cachefs
#
# safe_cachefs.sh cache --source-dir /mnt/ossfs --cache-dir memory --serf-join "host1 host2" cachefs /mnt/cachefs
#
# The parsed options would be like:
# "--source-dir /mnt/ossfs --cache-dir memory --serf-join 'host1 host2' -- cache cachefs /mnt/cachefs
#
# The first part is options, followed by '--', then arguments.
#
# Note the quotes around "$parsed_options": they are essential!
eval set -- "$parsed_options"
unset parsed_options

help=false
umount_only=false
sourceless=false

# re-construct options from parsed options
# Note:
# - These options could be passed through to cachefs directly without handling.
# - Here we parse some of them only to extract their values for later use in the script,
#   not to change the way they are passed.
# - For options not explicitly handled here, it means their values are not needed later,
#   so they are passed through untouched.
# - Quoting (single/double quotes) needs to be handled case by case.
new_opts=""
while true; do
	case "$1" in
	'--source-dir')
		src=$2
		new_opts+=" --source-dir $src"
		shift 2
		continue
	;;
	'--extra-source-dirs')
		# quote options to --extra-source-dirs with '' is required, otherwise multiple paths
		# will be treated as separated tokens.
		new_opts+=" --extra-source-dirs '$2'"
		shift 2
		continue
	;;
	'--sourceless')
		sourceless=true
		new_opts+=" --sourceless"
		shift 1
		continue
	;;
	'--source-subpath')
		# quote options to --source-subpath with '' is required, otherwise multiple paths
		# will be treated as separated tokens.
		new_opts+=" --source-subpath '$2'"
		shift 2
		continue
	;;
	'--serf-join')
		# quote options to --serf-join with '' is required, otherwise multiple hosts will be
		# treated as separated tokens.
		new_opts+=" --serf-join '$2'"
		shift 2
		continue
	;;
	'--log-id')
		logid="$2"
		new_opts+=" --log-id '$logid'"
		shift 2
		continue
	;;
	'--help')
		help=true
		new_opts+=" --help"
		shift
		break
	;;
	'--auto-unmount')
		# only do umount, don't bindmount source to cachefs mountpoint.
		# cachefs doesn't support this option, so don't append it to new_opts
		umount_only=true
		shift
		continue
	;;
	'--transport-nic')
		# quote options to --transport-nic with '' is required, otherwise multiple devices will be
		# treated as separated tokens.
		new_opts+=" --transport-nic '$2'"
		shift 2
		continue
	;;
	'--cacheroot-tracker')
		# quote options to --cacheroot-tracker with '' is required, otherwise multiple addrs
		# will be treated as separated tokens.
		new_opts+=" --cacheroot-tracker '$2'"
		shift 2
		continue
	;;
	'--cacheroot-nic')
		new_opts+=" --cacheroot-nic '$2'"
		shift 2
		continue
	;;
	'--preload')
		new_opts+=" --preload '$2'"
		shift 2
		continue
	;;
	'--pin-cache')
		# quote options to --pin-cache with '' is required, otherwise multiple paths
		# will be treated as separated tokens.
		new_opts+=" --pin-cache '$2'"
		shift 2
		continue
	;;
	'--pin-cache-cmd')
		# quote to preserve spaces in command string
		new_opts+=" --pin-cache-cmd '$2'"
		shift 2
		continue
	;;
	'--source-subpath-cmd')
		# quote to preserve spaces in command string
		new_opts+=" --source-subpath-cmd '$2'"
		shift 2
		continue
	;;
	'--buffer-reuse'|'--buffer-prefault')
		if [ -n "$2" ]; then
			new_opts+=" $1=$2"
		else
			new_opts+=" $1"
		fi
		# GNU getopt emits an empty value token for a bare optional long option.
		shift 2
		continue
	;;
	'--')
		# only re-construct options, arguments will be constructed later
		shift
		break
	;;
	*)
		# other options are copied as-is
		new_opts+=" $1"
		shift
		continue
	;;
	esac
done

# re-construct arguments, and last argument is mount point
new_args=""
for arg; do
	new_args+=" $arg"
	mp=$arg
done

if [ "$src" == "" ] && ! $sourceless ; then
	err "source is not set"
	status=1
	exit $status
fi
if [ "$mp" == "" ]; then
	err "mount point is not set"
	status=1
	exit $status
fi

info "mount cachefs source $src to $mp"

# use eval to expand options like --serf-join 'host1 host2' correctly
info cachefs $new_args $new_opts
eval cachefs $new_args $new_opts &
pid=$!

# if help is true, there's no need to monitor cachefs process
if $help; then
	exit 0
fi

mntsts=$(wait_cachefs_mount $mp 10)
if [ "$mntsts" != "true" ]; then
	warn "cachefs not mounted for 10s, exiting"
	kill $pid
	exit 1
fi

# Get FUSE connection ID after successful mount
fuse_conn_id=$(get_fuse_conn_id "$mp")
if [ -n "$fuse_conn_id" ]; then
	# Verify that the fusectl filesystem is mounted and connection directory exists
	abort_path="/sys/fs/fuse/connections/${fuse_conn_id}/abort"
	if [ -e "$abort_path" ] || [ -d "/sys/fs/fuse/connections/${fuse_conn_id}" ]; then
		info "FUSE connection established with ID: $fuse_conn_id"
	else
		warn "FUSE connection ID $fuse_conn_id found but /sys/fs/fuse/connections/${fuse_conn_id} not available (fusectl not mounted?), abort on exit will be disabled"
		fuse_conn_id=""
	fi
else
	warn "Failed to get FUSE connection ID, will not be able to abort pending requests on exit"
fi

wait $pid
ret=$?
if [ $ret -ne 0 ]; then
	if $umount_only; then
		warn "cachefs($pid) crashed? umount and exit"
		status=$ret
		exit 1
	else
		warn "cachefs($pid) crashed? fallback to source dir $src"
		umount $mp >/dev/null 2>&1
		mount --bind $src $mp
		mount -o remount,bind,ro $mp
		# sleep forever to keep this bindmount around, as we'll umount $mp at script exits
		sleep infinity
	fi
fi
exit 0
