
#pragma once

#include <unordered_map>

#include <ray/api/uniqueId.h>
#include <ray/api/wait_result.h>
#include <ray/core.h>
#include <ray/util/type_util.h>

#include <ray/runtime/object_store.h>

namespace ray {

class LocalModeObjectStore : public ObjectStore {
 private:
  std::unordered_map<UniqueId, ::ray::blob> _data;

  std::mutex _dataMutex;

  void waitInternal(const UniqueId *ids, int count, int minNumReturns, int timeoutMs);

 public:
  void putRaw(const UniqueId &objectId, std::vector< ::ray::blob> &&data);

  void del(const UniqueId &objectId);

  del_unique_ptr< ::ray::blob> getRaw(const UniqueId &objectId, int timeoutMs);

  WaitResultInternal wait(const std::vector<UniqueId> &objects, int num_objects, int64_t timeout_ms);
};

}  // namespace ray