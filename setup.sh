#!/bin/sh

apt-get update
apt-get -y install \
    wget build-essential swig cmake git libnuma-dev python3-distutils gcc g++ \
    libboost-all-dev openmpi-bin openmpi-common openmpi-doc libopenmpi-dev libjemalloc-dev \
    libssl-dev libgflags-dev libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev libsnappy-dev

# make sure the thirdparty repos are cloned
git submodule update --init --recursive

#install brpc
VERSION="release-1.14"
STARTING_DIR=$(pwd)
THIRD_PARTY_DIR="${STARTING_DIR}/ThirdParty"
BRPC_SRC_DIR="${THIRD_PARTY_DIR}/brpc"
BRPC_INSTALL_DIR="${THIRD_PARTY_DIR}/brpc_install"
if [ ! -d "$BRPC_SRC_DIR" ]; then
    git clone --branch $VERSION --depth 1 https://github.com/apache/brpc.git ${BRPC_SRC_DIR}
fi
cd ${BRPC_SRC_DIR}
cmake . -B build \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$BRPC_INSTALL_DIR
cmake --build build --target install --parallel $(nproc)
cd $STARTING_DIR

ONNX_SRC_DIR="${THIRD_PARTY_DIR}/onnxruntime-linux-x64-1.22.0"

if [ ! -d "$ONNX_SRC_DIR" ]; then
    wget -P $THIRD_PARTY_DIR https://github.com/microsoft/onnxruntime/releases/download/v1.22.0/onnxruntime-linux-x64-1.22.0.tgz
    tar -xzvf $THIRD_PARTY_DIR/onnxruntime-linux-x64-1.22.0.tgz -C $THIRD_PARTY_DIR
    rm $THIRD_PARTY_DIR/onnxruntime-linux-x64-1.22.0.tgz
fi

#install abseil
VERSION="lts_2025_08_14"
ABSL_SRC_DIR="${THIRD_PARTY_DIR}/abseil-cpp"
ABSL_INSTALL_DIR="${THIRD_PARTY_DIR}/abseil-install"
if [ ! -d "$ABSL_SRC_DIR" ]; then
    git clone --branch $VERSION --depth 1 https://github.com/abseil/abseil-cpp.git ${ABSL_SRC_DIR}
fi
cd ${ABSL_SRC_DIR}
cmake . -B build \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$ABSL_INSTALL_DIR
cmake --build build --target install --parallel $(nproc)
cd $STARTING_DIR

pip install numpy pre-commit
pre-commit autoupdate && pre-commit install && pre-commit run --all-files
