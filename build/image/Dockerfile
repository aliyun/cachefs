ARG BASE_IMAGE=alibaba-cloud-linux-3-registry.cn-hangzhou.cr.aliyuncs.com/alinux3/alinux3:3.11
ARG GOPATH=/root/go

# build image
FROM ${BASE_IMAGE} as builder

ARG GOPATH
ARG GOPROXY="https://goproxy.cn,direct"
ARG BUILD_MODE

WORKDIR ${GOPATH}/src/cachefs

COPY build/scripts/dnf-install.sh /tmp/dnf-install.sh
RUN /tmp/dnf-install.sh golang make cmake git g++ libstdc++-static rdma-core-devel alibaba-cloud-compiler rpm

COPY . ${GOPATH}/src/cachefs

RUN git config --global --add safe.directory ${GOPATH}/src/cachefs
RUN git submodule update --init
RUN make clean && make

# build final image
FROM ${BASE_IMAGE}

ARG GOPATH

ENV LANG C.UTF-8
COPY build/scripts/dnf-install.sh /tmp/dnf-install.sh
RUN /tmp/dnf-install.sh fuse fuse-libs bc curl nmap-ncat ca-certificates iputils libibverbs rpm procps-ng && rm -rf /var/cache/dnf
COPY --from=builder ${GOPATH}/src/cachefs/cachefs /bin/cachefs
COPY build/scripts/safe_cachefs.sh /bin/safe_cachefs.sh
COPY LICENSE NOTICE /usr/share/licenses/cachefs/
RUN ln /bin/cachefs /bin/mount.cachefs
