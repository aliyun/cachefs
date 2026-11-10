#!/bin/bash

PROTOC_ALL_IMAGE=${PROTOC_ALL_IMAGE:-"eci-nydus-registry.cn-hangzhou.cr.aliyuncs.com/docker/protoc-all:1.51_1"}
BUILD_IMAGE=eci-nydus-registry.cn-hangzhou.cr.aliyuncs.com/docker/golang:1.22.4
LANGUAGE=go

echo "generate grpc protos..."
GRPC_PATH=pkg/rpc
grpc_modules="cachefs cacheroot meta"
for module in ${grpc_modules}; do
    if docker run --rm -v $PWD:/defs ${PROTOC_ALL_IMAGE} \
    -d ${GRPC_PATH}/$module -i . \
    -l ${LANGUAGE} -o . \
    --go-source-relative \
    --with-validator \
    --validator-source-relative; then
        echo "generate grpc protos ${module} successfully"
    else
        echo "generate grpc protos ${module} failed"
    fi
done

echo "generating ttrpc protos..."
TTRPC_PATH=pkg/ttrpc
ttrpc_modules="cachefs"
for module in ${ttrpc_modules}; do
    if docker run --rm -v $PWD:/defs ${PROTOC_ALL_IMAGE} \
    -d ${TTRPC_PATH}/$module -i . \
    -l ${LANGUAGE} -o . \
    --go-source-relative \
    --with-validator \
    --validator-source-relative \
    --ttrpc; then
        echo "generate ttrpc protos ${module} successfully"
    else
        echo "generate ttrpc protos ${module} failed"
    fi
done

docker run --rm -v $PWD:/work -e GOPROXY="https://goproxy.cn,direct" -w /work $BUILD_IMAGE \
bash -c "go install golang.org/x/tools/cmd/goimports@v0.15.0 && goimports -w pkg/rpc && goimports -w pkg/ttrpc"
