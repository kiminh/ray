
#pragma once


// #include <memory>

#include "api/blob.h"
#include "api/impl/function_argument.h"
// #include "api/ray_api.h"
#include "ray/util/type_util.h"
// #include "api/ray_object.h"
// #include "api/ray_actor.h"
// #include "api/ray_function.h"
#include "api/wait_result.h"

/**
 * ray api definition
 *
 */
namespace ray {

template <typename T>
class RayObject;
template <typename T>
class RayActor;
template <typename F>
class RayFunction;

class RayRuntime;

class Ray {
  template <typename T>
  friend class RayObject;

 private:
  //static RayApi *_impl;
  static RayRuntime *_impl;

  template <typename T>
  static std::shared_ptr<T> get(const RayObject<T> &object);

 public:
  static void init();

// static bool init(const RayConfig& rayConfig);

  template <typename T>
  static RayObject<T> put(const T &obj);
  //static Status put(const T &obj, RayObject<T> *rayObject);

  template <typename T>
  static std::vector<std::shared_ptr<T>> get(const std::vector<RayObject<T>> &objects);

  template <typename T>
  static WaitResult<T> wait(const std::vector<RayObject<T>> &objects, int num_objects, int64_t timeout_ms);

#include "api/impl/call_funcs.generated.h"

#include "api/impl/create_actors.generated.h"

#include "api/impl/call_actors.generated.h"
};

}  // namespace ray

// --------- inline implementation ------------
#include "api/impl/arguments.h"
#include "api/execute.h"
#include "api/ray_actor.h"
#include "api/ray_function.h"
#include "api/ray_object.h"
#include <ray/runtime/ray_runtime.h>

namespace ray {
class Arguments;

template <typename T>
inline RayObject<T> Ray::put(const T &obj) {
  return _impl->put(obj);
}

template <typename T>
inline std::vector<std::shared_ptr<T>> Ray::get(const std::vector<RayObject<T>> &objects) {
  return _impl->get(objects);
}

template <typename T>
inline std::shared_ptr<T> Ray::get(const RayObject<T> &object) {
  return _impl->get(object);
}

template <typename T>
inline WaitResult<T> Ray::wait(const std::vector<RayObject<T>> &objects, int num_objects, int64_t timeout_ms) {
  return _impl->wait(objects, num_objects, timeout_ms);
}

#include "api/impl/call_funcs_impl.generated.h"

#include "api/impl/create_actors_impl.generated.h"

#include "api/impl/call_actors_impl.generated.h"

}  // namespace ray
