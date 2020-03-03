
#pragma once

#include <functional>
#include <memory>
#include <cstdint>

namespace ray {

template <typename T>
using del_unique_ptr = std::unique_ptr<T, std::function<void(T *)> >;

struct member_function_ptr_holder {
  uintptr_t value[2];
};

struct remote_function_ptr_holder {
  uintptr_t value[2];
};

}  // namespace ray
