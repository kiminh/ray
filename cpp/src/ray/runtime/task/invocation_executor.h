#pragma once

#include <ray/runtime/task_spec.h>

namespace ray {

class InvocationExecutor {
 public:
  static void execute(const TaskSpec &taskSpec, ::ray::blob *actor_blob);
};
}  // namespace ray