
#include <memory>

#include <ray/runtime/task_executer.h>

namespace ray {

std::unique_ptr<UniqueId> TaskExcuter::execute(const InvocationSpec &invocation) {
  std::unique_ptr<UniqueId> dummy(new UniqueId());
  return dummy;
};
}  // namespace ray