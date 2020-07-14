cmake_minimum_required(VERSION 3.15)
project(test_so)

set(CMAKE_CXX_STANDARD 17)

include_directories(
        /System/Volumes/Data/private/var/tmp/_bazel_yu/e487562a970bc4620ebe32c522116778/external/boost
        /System/Volumes/Data/private/var/tmp/_bazel_yu/e487562a970bc4620ebe32c522116778/external/msgpack/include
)
link_directories(/System/Volumes/Data/private/var/tmp/_bazel_yu/e487562a970bc4620ebe32c522116778/external/boost/stage/lib)

add_executable(test_so main.cpp)

target_link_libraries(test_so  -lboost_system -lboost_filesystem)