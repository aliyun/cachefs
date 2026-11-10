export GO111MODULE=on

all: cachefs

REVISION := $(shell git rev-parse --short HEAD 2>/dev/null)
REVISIONDATE := $(shell git log -1 --pretty=format:'%cd' --date short 2>/dev/null)
PKG := github.com/aliyun/cachefs/pkg/version
LDFLAGS = -s -w
ifneq ($(strip $(REVISION)),) # Use git clone
	LDFLAGS += -X $(PKG).revision=$(REVISION) \
		   -X $(PKG).revisionDate=$(REVISIONDATE)
endif

SHELL = /bin/sh

ifdef STATIC
	LDFLAGS += -linkmode external -extldflags '-static'
	CC = /usr/bin/musl-gcc
	export CC
endif

ARCH ?= $(shell uname -m)

# Only support x86_64 and aarch64 (arm64)
SUPPORTED_ARCHS := x86_64 aarch64 arm64
ifneq ($(filter $(ARCH), $(SUPPORTED_ARCHS)),)
$(info Building for supported architecture: $(ARCH))
else
$(error Unsupported architecture '$(ARCH)'. Only '$(SUPPORTED_ARCHS)' are supported.)
endif

# parse release from build/version file and apend "dev" to release string
# if BUILD_MODE is not "release"
VERSION := $(shell grep '^VERSION=' build/version | cut -d'=' -f2)
BUILD_MODE ?= dev
LDFLAGS += -X $(PKG).major=$(shell echo $(VERSION) | cut -d'.' -f1) \
	   -X $(PKG).minor=$(shell echo $(VERSION) | cut -d'.' -f2) \
	   -X $(PKG).patch=$(shell echo $(VERSION) | cut -d'.' -f3) \
	   -X $(PKG).release=$(if $(filter-out release,$(strip $(BUILD_MODE))),dev,)

COMMON_DEPS = Makefile cmd/*.go pkg/*/*/*.go pkg/*/*.go *.go go.*

CORO_RPC_DIR := $(CURDIR)/coro_rpc_lib
CORO_RPC_SRC := $(shell find $(CORO_RPC_DIR)/coro_rpc_lib $(CURDIR)/deps/yalantinglibs -type f \( -name '*.cpp' -o -name '*.h' -o -name '*.hpp' -o -name CMakeLists.txt \))
CORO_RPC_OUT_LIB := $(CORO_RPC_DIR)/libcoro_rpc.a
CORO_RPC_BUILD := $(CORO_RPC_DIR)/build
CORO_RPC_FLAGS ?=

$(CORO_RPC_OUT_LIB): $(CORO_RPC_SRC)
	@echo "==> Rebuilding coro_rpc_lib..."
	@rm -rf $(CORO_RPC_BUILD)
	@mkdir -p $(CORO_RPC_BUILD)
	@if test -x /opt/alibaba-cloud-compiler/bin/clang++; then \
		echo "-- Enable Alibaba Cloud Compiler"; \
		export CXX=/opt/alibaba-cloud-compiler/bin/clang++; \
	fi; \
	cd $(CORO_RPC_BUILD) && cmake .. $(CORO_RPC_FLAGS) -DENABLE_RDMA=ON -DCMAKE_BUILD_TYPE=Release && make -j$$(nproc)
	cp $(CORO_RPC_BUILD)/libcoro_rpc.a $@

cachefs: $(COMMON_DEPS) $(CORO_RPC_OUT_LIB)
	go build $(EXTRA_FLAGS) -ldflags="$(LDFLAGS)" -o cachefs .

test:
	go test -v -count=1 -cover -coverprofile=cov-cachefs1.out ./pkg/... ./cmd/... ./build/src/treesum
	go tool cover -func=cov-cachefs1.out -o coverage-result.txt

# Run cachefs Go benchmarks.
BENCH_TIME ?= 1s
benchmark:
	go test -run='^$$' -bench=. -benchmem -benchtime=$(BENCH_TIME) -timeout=5m ./pkg/... ./cmd/...

build.docker:
	BINARY_ONLY=1 ./build/scripts/build-docker-image.sh

smoke.docker:
	./build/scripts/smoke-test-docker.sh

vet:
	go vet ./pkg/... ./cmd/... ./build/src/treesum

fmt:
	goimports -w -d ./pkg ./cmd ./build/src/treesum

fmt.check:
	@out=$$(goimports -l -d ./pkg ./cmd ./build/src/treesum) && test -z "$$out" || { echo "$$out"; exit 1; }

build-image:
	./build/scripts/build-docker-image.sh

merge-image:
	./build/scripts/merge-docker-image.sh

build-rpm:
	./build/scripts/build-rpm.sh

protoc:
	./build/scripts/protoc.sh

clean:
	rm -rf $(CORO_RPC_BUILD) $(CORO_RPC_OUT_LIB) cachefs

.PHONY: protoc cachefs clean test benchmark fmt fmt.check build-image build.docker merge-image build-rpm
