#!/usr/bin/env bash
set -e
set -x

ROOT_DIR=tmp/build_cmake
CMAKE_SOURCE_DIR=$ROOT_DIR/cmake_source
CMAKE_BINARY_DIR=$ROOT_DIR/cmake_binary

# checkout
if [ ! -d $ROOT_DIR ]; then
  git clone https://gitlab.kitware.com/cmake/cmake.git -b release --single-branch $CMAKE_SOURCE_DIR
fi

# configure simple version of cmake
rm -rf $CMAKE_BINARY_DIR

#mkdir -p $CMAKE_BINARY_DIR/.cmake/api/v1/query
#touch $CMAKE_BINARY_DIR/.cmake/api/v1/query/codemodel-v2
#touch $CMAKE_BINARY_DIR/.cmake/api/v1/query/cache-v2
#touch $CMAKE_BINARY_DIR/.cmake/api/v1/query/cmakeFiles-v1
#touch $CMAKE_BINARY_DIR/.cmake/api/v1/query/toolchains-v1

# TODO simplify linker script?
# CMAKE_C_ARCHIVE_CREATE "<CMAKE_AR> qcs <TARGET> <LINK_FLAGS> <OBJECTS>"
# CMAKE_C_ARCHIVE_FINISH ""

cmake -S $CMAKE_SOURCE_DIR -B $CMAKE_BINARY_DIR -D BUILD_CursesDialog=OFF -D BUILD_QtDialog=OFF -D BUILD_TESTING=OFF -D CMAKE_BUILD_TYPE=Debug -G Ninja

ninja -C $CMAKE_BINARY_DIR -t commands > $CMAKE_BINARY_DIR/commands.sh
