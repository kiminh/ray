
#pragma once

#include <mutex>

#include <ray/api/blob.h>
// #include <ray/api/ray_api.h>
#include <ray/api/ray_config.h>
#include <ray/api/ray_config.h>
#include <ray/api/wait_result.h>
#include <ray/util/type_util.h>

namespace ray {

class Worker;
class TaskSubmitter;
class TaskExcuter;
class TaskSpec;
class ObjectStore;

class RayRuntime {
  friend class Ray;

 private:
 protected:
  static std::unique_ptr<RayRuntime> _ins;
  static std::once_flag isInited;

  std::shared_ptr<RayConfig> _config;
  std::unique_ptr<Worker> _worker;
  std::unique_ptr<TaskSubmitter> _taskSubmitter;
  std::unique_ptr<TaskExcuter> _taskExcuter;
  std::unique_ptr<ObjectStore> _objectStore;

 public:
  static RayRuntime &init(std::shared_ptr<RayConfig> config);

  static RayRuntime &getInstance();

  void put(std::vector< ::ray::blob> &&data, const UniqueId &objectId,
           const UniqueId &taskId);

  std::unique_ptr<UniqueId> put(std::vector< ::ray::blob> &&data);

  del_unique_ptr< ::ray::blob> get(const UniqueId &objectId);

  template <typename T>
  static RayObject<T> put(const T &obj);

  template <typename T>
  static std::shared_ptr<T> get(const RayObject<T> &object);

  template <typename T>
  static std::vector<std::shared_ptr<T>> get(const std::vector<RayObject<T>> &objects);

  template <typename T>
  WaitResult<T> wait(const std::vector<RayObject<T>> &objects, int num_objects, int64_t timeout_ms);

  std::unique_ptr<UniqueId> call(remote_function_ptr_holder &fptr,
                                 std::vector< ::ray::blob> &&args);

  virtual std::unique_ptr<UniqueId> create(remote_function_ptr_holder &fptr,
                                   std::vector< ::ray::blob> &&args);

  std::unique_ptr<UniqueId> call(const remote_function_ptr_holder &fptr,
                                 const UniqueId &actor, std::vector< ::ray::blob> &&args);

  TaskSpec *getCurrentTask();

  void setCurrentTask(TaskSpec &task);

  int getNextPutIndex();

  const UniqueId &getCurrentTaskId();

  virtual ~RayRuntime(){};

 private:
  static RayRuntime &doInit(std::shared_ptr<RayConfig> config);

  virtual char *get_actor_ptr(const UniqueId &id);

  void execute(const TaskSpec &taskSpec);
};
}

// --------- inline implementation ------------
#include <ray/runtime/object_store.h>
#include <ray/runtime/worker_context.h>
#include <ray/runtime/task_executer.h>
#include <ray/runtime/task_submitter.h>
#include <ray/runtime/task_spec.h>

namespace ray {

template <typename T>
RayObject<T> RayRuntime::put(const T &obj) {
  return _ins->_objectStore->put(obj);
}

template <typename T>
std::shared_ptr<T>  RayRuntime::get(const RayObject<T> &object) {
  return _ins->_objectStore->get(object);
}

template <typename T>
std::vector<std::shared_ptr<T>> RayRuntime::get(const std::vector<RayObject<T>> &objects) {
  return _ins->_objectStore->get(objects);
}

template <typename T>
WaitResult<T> RayRuntime::wait(const std::vector<RayObject<T>> &objects, int num_objects, int64_t timeout_ms) {
  return _ins->_objectStore->wait(objects, num_objects, timeout_ms);
}

}  // namespace ray