//
// Created by ashione on 2019/4/1.
//

#ifndef RAY_STREAMING_STREAMING_CONSTANT_H
#define RAY_STREAMING_STREAMING_CONSTANT_H
#include <cstdlib>
namespace ray {
namespace streaming {
enum class StreamingStatus : uint32_t { OK = 0, MIN = OK, MAX = OK };
}
}  // namespace ray

#endif  // RAY_STREAMING_STREAMING_CONSTANT_H
