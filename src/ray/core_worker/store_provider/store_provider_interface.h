#ifndef RAY_CORE_WORKER_STORE_PROVIDER_INTERFACE_H
#define RAY_CORE_WORKER_STORE_PROVIDER_INTERFACE_H

#include "ray/core_worker/common.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"

namespace ray {


class CoreWorkerStoreProviderInterface {
 public:
  CoreWorkerStoreProviderInterface() {}

  virtual ~CoreWorkerStoreProviderInterface() {}

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] buffer Data buffer of the object.
  /// \param[in] object_id Object ID specified by user.
  /// \return Status.
  virtual Status Put(const Buffer &buffer, const ObjectID &object_id) = 0;

  /// Get a list of objects from the object store.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] task_id ID for the current task.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  virtual Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms,
                     const TaskID &task_id, std::vector<std::shared_ptr<Buffer>> *results) = 0;

  /// Wait for a list of objects to appear in the object store.
  ///
  /// \param[in] IDs of the objects to wait for.
  /// \param[in] num_returns Number of objects that should appear.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] task_id ID for the current task.  
  /// \param[out] results A bitset that indicates each object has appeared or not.
  /// \return Status.
  virtual Status Wait(const std::vector<ObjectID> &object_ids, int num_objects,
                      int64_t timeout_ms, const TaskID &task_id, std::vector<bool> *results) = 0;

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \param[in] delete_creating_tasks Whether also delete the tasks that
  /// created these objects. \return Status.
  virtual Status Delete(const std::vector<ObjectID> &object_ids, bool local_only,
                        bool delete_creating_tasks) = 0;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_STORE_PROVIDER_INTERFACE_H