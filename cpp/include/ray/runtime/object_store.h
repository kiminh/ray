
#pragma once

#include <memory>
#include <vector> 

#include <ray/api/blob.h>
#include <ray/api/uniqueId.h>
#include <ray/api/wait_result.h>
#include <ray/util/type_util.h>
#include <ray/api/wait_result.h>

namespace ray {

  template <typename T>
  class RayObject;

extern const int fetchSize;
extern const int getTimeoutMs;

class ObjectStore {
 private:
 public:
  void put(const UniqueId &objectId, std::vector< ::ray::blob> &&data);

  del_unique_ptr< ::ray::blob> get(const UniqueId &objectId,
                                   int timeoutMs = getTimeoutMs);
  template <typename T>
  RayObject<T> put(const T &obj);

  template <typename T>
  std::shared_ptr<T> get(const RayObject<T> &object);

  template <typename T>
  std::vector<std::shared_ptr<T>> get(const std::vector<RayObject<T>> &objects);

  template <typename T>
  WaitResult<T> wait(const std::vector<RayObject<T>> &objects, int num_objects, int64_t timeout_ms);

  virtual void putRaw(const UniqueId &objectId, std::vector< ::ray::blob> &&data) = 0;

  virtual void del(const UniqueId &objectId) = 0;

  virtual del_unique_ptr< ::ray::blob> getRaw(const UniqueId &objectId,
                                              int timeoutMs) = 0;

  virtual WaitResultInternal wait(const std::vector<UniqueId> &objects, int num_objects, int64_t timeout_ms) = 0;
  virtual ~ObjectStore(){};
};

} // namespace ray

// --------- inline implementation ------------

#include <ray/api/ray_object.h>
#include <ray/api/impl/arguments.h>

namespace ray {
  template <typename T>
  RayObject<T> ObjectStore::put(const T &obj) {
    // ::ray::binary_writer writer;
    // Arguments::wrap(writer, obj);
    // std::vector<::ray::blob> data;
    // writer.get_buffers(data);
    // auto id = put(std::move(data));
    // return RayObject<T>(id);
    RayObject<T> object;
    return object;
  }

  template <typename T>
  std::shared_ptr<T> ObjectStore::get(const RayObject<T> &object) {
    std::shared_ptr<T> result;
    return result;
  }

  template <typename T>
  std::vector<std::shared_ptr<T>> ObjectStore::get(const std::vector<RayObject<T>> &objects) {
    // auto data = _objectStore->get(RayObject::);
    // ::ray::binary_reader reader(*data.get());
    // Arguments::unwrap(reader, obj);
    std::vector<std::shared_ptr<T>> vector;
    return vector;
  }

  template <typename T>
  WaitResult<T> ObjectStore::wait(const std::vector<RayObject<T>> &objects, int num_objects, int64_t timeout_ms) {
    WaitResult<T> result;
    return result;
  }

}  // namespace ray