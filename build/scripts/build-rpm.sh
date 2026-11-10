#!/bin/bash

set -o nounset
set -o errexit
set -o pipefail

export LANG=en_US.UTF-8
tmp=/tmp/$$
arch=$(arch)
trap "rm -f $tmp.*" 0 1 2 3 15

if [ $(id -u) -ne 0 ]; then
	echo "Require root"
	exit 1
fi

curDir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
srcroot="${curDir}/../../"
cd "${srcroot}" || exit 1

source build/common

RELEASE="1"
if [ "$BUILD_MODE" != "release" ]; then
	RELEASE+=".$GIT_COMMIT"
fi

# Build the RPM from $tmp.spec and copy the resulting package to the current
# directory.
do_rpmbuild() {
	rpmbuild -bb "$tmp.spec" >"$tmp.build" 2>&1
	cat "$tmp.build"
	local rpm
	rpm=$(grep "Wrote:.*/RPMS/" "$tmp.build" | awk '{print $2}')
	cp "$rpm" ./
	rm -rf ./rpm
}

cat >$tmp.spec << EOF
Name: cachefs
Version: $VERSION
Release: $RELEASE
License: Apache License 2.0
Summary: File system that cache & accelerate any POSIX directory with distributed cache

BuildArch: $arch

%description

%build

%install
mkdir -p %{buildroot}/usr/bin/
install -m 0755 $srcroot/cachefs %{buildroot}/usr/bin/cachefs
mkdir -p %{buildroot}/usr/share/licenses/cachefs
install -m 0644 $srcroot/LICENSE %{buildroot}/usr/share/licenses/cachefs/LICENSE
install -m 0644 $srcroot/NOTICE %{buildroot}/usr/share/licenses/cachefs/NOTICE

%files
/usr/bin/cachefs
/usr/share/licenses/cachefs/LICENSE
/usr/share/licenses/cachefs/NOTICE

%changelog
* Tue Sep 29 2026 Eryu Guan <eguan@linux.alibaba.com> - v3.7
- initial open source release

EOF

make
do_rpmbuild
exit 0
