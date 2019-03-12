#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

# run this file before compile the targets
sh $ROOT_DIR/generate_deps.sh

pushd $ROOT_DIR/..
# bazel checkstyle for java
bazel test //java:all -c opt
# compile all the targets
bazel build //java:all -c opt --verbose_failures
popd

# The following are soft links
# TODO: remove this once cmake is removed
mkdir -p $ROOT_DIR/../build/java/
ln -sf $ROOT_DIR/../bazel-bin/java/* $ROOT_DIR/../build/java/
mkdir -p $ROOT_DIR/tutorial/target/
ln -sf $ROOT_DIR/../bazel-bin/java/org_ray_ray_tutorial_deploy.jar $ROOT_DIR/tutorial/target/ray-tutorial-0.1-SNAPSHOT.jar

pushd $ROOT_DIR/test
echo "Running tests under cluster mode."
ENABLE_MULTI_LANGUAGE_TESTS=1 java -jar -Dray.home=$ROOT_DIR/../ $ROOT_DIR/../bazel-bin/java/all_tests_deploy.jar $ROOT_DIR/../java/testng.xml

echo "Running tests under single-process mode."
java -jar -Dray.home=$ROOT_DIR/../ -Dray.run-mode=SINGLE_PROCESS $ROOT_DIR/../bazel-bin/java/all_tests_deploy.jar $ROOT_DIR/../java/testng.xml

popd
