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

rm -rf $CMAKE_BINARY_DIR

# configure simple version of cmake
# patch CMAKE_C_ARCHIVE_* to simplify linker script (get rid of ranlib)
cmake -S $CMAKE_SOURCE_DIR -B $CMAKE_BINARY_DIR -D BUILD_CursesDialog=OFF -D BUILD_QtDialog=OFF -D BUILD_TESTING=OFF -D CMAKE_BUILD_TYPE=Debug \
  -D "CMAKE_C_ARCHIVE_CREATE=<CMAKE_AR> qcs <TARGET> <LINK_FLAGS> <OBJECTS>" -D "CMAKE_C_ARCHIVE_FINISH=" \
  -G Ninja

# build commands used by ninja with razel
ninja -C $CMAKE_BINARY_DIR -t commands > $CMAKE_BINARY_DIR/commands.sh
cargo run -- batch $CMAKE_BINARY_DIR/commands.sh

# build with ninja to get a reference executable
ninja -C $CMAKE_BINARY_DIR

diff $CMAKE_BINARY_DIR/bin/cmake razel-out/bin/cmake
