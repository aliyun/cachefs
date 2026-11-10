#!/bin/bash
# Install rpm packages with dnf, working around an unreachable internal
# mirror: the base image repos list mirrors.cloud.aliyuncs.com first,
# which hangs outside the VPC. Probe it with a short timeout and rewrite
# the repo files to the public mirror in place when unreachable.
set -e

if ! curl -sSL --connect-timeout 3 mirrors.cloud.aliyuncs.com >/dev/null 2>&1; then
	sed -i 's|mirrors\.cloud\.aliyuncs\.com|mirrors.aliyun.com|g' /etc/yum.repos.d/*.repo
fi

dnf install -y "$@"
