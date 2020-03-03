#pragma once
#include <dlfcn.h>
#include <stdint.h>

namespace ray {

/* Use for locate remote function address in single process mode */
extern uintptr_t dylib_base_addr;

extern "C" void Ray_agent_init();
}  // namespace ray