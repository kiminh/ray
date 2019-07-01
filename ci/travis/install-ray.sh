#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [[ "$TRAVIS" == "TRAVIS" ]]; then
  local cache_dir="$HOME/ray-bazel-cache"
  echo "Enabling bazel disk cache at dir $cache_dir."
  mkdir -p $cache_dir
  echo "build --disk_cache=$cache_dir" >> .bazelrc
fi

echo "PYTHON is $PYTHON"

if [[ "$PYTHON" == "2.7" ]]; then

  pushd "$ROOT_DIR/../../python"
    python setup.py install --user
  popd

elif [[ "$PYTHON" == "3.5" ]]; then
  export PATH="$HOME/miniconda/bin:$PATH"

  pushd "$ROOT_DIR/../../python"
    python setup.py install --user
  popd

elif [[ "$LINT" == "1" ]]; then
  export PATH="$HOME/miniconda/bin:$PATH"

  pushd "$ROOT_DIR/../../python"
    python setup.py install --user
  popd

else
  echo "Unrecognized Python version."
  exit 1
fi
