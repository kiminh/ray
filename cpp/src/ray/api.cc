
#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/runtime/ray_runtime.h>
#include <ray/api/execute.h>
#include <ray/api/impl/arguments.h>
#include <ray/api/ray_actor.h>
#include <ray/api/ray_function.h>
#include <ray/api/ray_object.h>
#include <ray/api/wait_result.h>
// #include "runtime/ray_runtime.h"


namespace ray {

RayRuntime *Ray::_impl = nullptr;

void Ray::init() { _impl = &RayRuntime::doInit(std::make_shared<RayConfig>()); }

}  // namespace ray