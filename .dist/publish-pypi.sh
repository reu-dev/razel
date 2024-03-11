#!/usr/bin/env bash
set -e
set -x

VERSION=$1
SCRIPT_DIR=$(dirname -- "$0";)
ROOT_DIR=$(realpath $SCRIPT_DIR/../)
PACKAGE_DIR=$(realpath $SCRIPT_DIR/pypi_package)

rm -rf $ROOT_DIR/apis/python/__pycache__

rm -rf $PACKAGE_DIR
mkdir -p $PACKAGE_DIR/src/razel
echo "VERSION=\"$VERSION\"" > $PACKAGE_DIR/version.py
cp $ROOT_DIR/apis/python/* $PACKAGE_DIR/src/razel
cp $ROOT_DIR/LICENSE.md $PACKAGE_DIR
cp $ROOT_DIR/README.md $PACKAGE_DIR
cp $SCRIPT_DIR/pyproject.toml $PACKAGE_DIR

python3 -m pip install --upgrade build twine

pushd $PACKAGE_DIR
python3 -m build
python3 -m twine upload dist/*
popd
