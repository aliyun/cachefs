#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

# enable bash debug output
DEBUG=${DEBUG:-}

if [[ -n "$DEBUG" ]]; then
	set -x
fi

if [ $(id -u) -ne 0 ]; then
	echo "Require root"
	exit 1
fi

curDir=$(cd "$(dirname "$0")" && pwd)
cd "${curDir}/../../" || return

if [[ -z "${TARGETARCH:-}" ]]; then
	TARGETARCH="$(uname -m)"
	echo "TARGETARCH unset, performed auto-detection: $TARGETARCH"
else
	echo "TARGETARCH set by user: $TARGETARCH"
fi

case "${TARGETARCH}" in
	x86_64 | amd64)
		TARGETARCH="amd64"
	;;
	aarch64 | arm64)
		TARGETARCH="arm64"
	;;
	*)
		echo "Error: TARGETARCH '${TARGETARCH}' is not a recognized architecture (Supported: amd64/x86_64, arm64/aarch64)."
		exit 1
	;;
esac
echo "normalized target arch: $TARGETARCH"

source build/common
echo "build mode: $BUILD_MODE"
build_args=(--build-arg BUILD_MODE=$BUILD_MODE)

dockerfile="build/image/Dockerfile"
if [ "$TARGETARCH" = "arm64" ]; then
	dockerfile="${dockerfile}.arm64"
fi

if [[ "${BINARY_ONLY:-}" == "1" ]]; then
	# Build only the builder stage and extract the binary to repo root
	builder_tag="cachefs-builder-${TARGETARCH}"
	docker build --target builder -t $builder_tag "${build_args[@]}" -f "${dockerfile}" .
	workdir=$(docker inspect --format '{{.Config.WorkingDir}}' $builder_tag)
	cid=$(docker create $builder_tag)
	docker cp $cid:${workdir}/cachefs ./cachefs
	docker rm $cid > /dev/null
	echo "==> Binary: ./cachefs"
else
	image_name=$(get_image_name)-${TARGETARCH}
	docker build -t $image_name "${build_args[@]}" -f "${dockerfile}" .
fi
