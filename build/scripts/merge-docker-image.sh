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

source build/common

echo "--- Merge Manifest ---"
ARM="arm64"
AMD="amd64"
# Allow callers to override the target image name via IMAGE_NAME env var.
FINAL_IMAGE="${IMAGE_NAME:-$(get_image_name)}"
ARM_IMAGE="${FINAL_IMAGE}-${ARM}"
AMD_IMAGE="${FINAL_IMAGE}-${AMD}"

echo "1. Creating Manifest List: ${FINAL_IMAGE}"
# Remove any existing manifest or local image with this name before creating,
# so the command works with both Docker (--amend) and Podman (requires --replace
# or prior removal when a local image occupies the same name).
docker manifest rm "${FINAL_IMAGE}" 2>/dev/null || true
docker rmi "${FINAL_IMAGE}" 2>/dev/null || true
docker manifest create "${FINAL_IMAGE}"

echo "2. Adding architecture annotations..."
docker manifest add "${FINAL_IMAGE}" "${AMD_IMAGE}"
docker manifest add "${FINAL_IMAGE}" "${ARM_IMAGE}"

echo "3. Pushing Multi-Arch Manifest to Registry..."
docker manifest push "${FINAL_IMAGE}"

echo "--- Success! Multi-Arch image ${FINAL_IMAGE} created. ---"
