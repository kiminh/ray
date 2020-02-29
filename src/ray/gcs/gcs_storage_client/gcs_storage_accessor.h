#ifndef RAY_GCS_GCS_STORAGE_ACCESSOR_H_
#define RAY_GCS_GCS_STORAGE_ACCESSOR_H_

#include "ray/common/id.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"
#include "ray/gcs/subscription_executor.h"

namespace ray {
namespace gcs {

/// \class GcsStorageActorInfoAccessor
/// GcsStorageActorInfoAccessor uses persist storage as the backend storage.
class GcsStorageActorInfoAccessor {
 public:
  explicit GcsStorageActorInfoAccessor(GcsStorageClient &client_impl);

  virtual ~GcsStorageActorInfoAccessor() {}

  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<ActorTableData> &callback);

  Status AsyncRegister(const std::shared_ptr<ActorTableData> &data_ptr,
                       const StatusCallback &callback);

  Status AsyncUpdate(const ActorID &actor_id,
                     const std::shared_ptr<ActorTableData> &data_ptr,
                     const StatusCallback &callback);

  Status AsyncAddCheckpoint(const std::shared_ptr<ActorCheckpointData> &data_ptr,
                            const StatusCallback &callback);

  Status AsyncGetCheckpoint(const ActorCheckpointID &checkpoint_id,
                            const OptionalItemCallback<ActorCheckpointData> &callback);

  Status AsyncGetCheckpointID(
      const ActorID &actor_id,
      const OptionalItemCallback<ActorCheckpointIdData> &callback);

 private:
  /// Add checkpoint id to GCS asynchronously.
  ///
  /// \param actor_id The ID of actor that the checkpoint belongs to.
  /// \param checkpoint_id The ID of checkpoint that will be added to GCS.
  /// \return Status
  Status AsyncAddCheckpointID(const ActorID &actor_id,
                              const ActorCheckpointID &checkpoint_id,
                              const StatusCallback &callback);

 private:
  GcsStorageClient &client_impl_;
};

/// \class GcsStorageJobInfoAccessor
/// GcsStorageJobInfoAccessor uses persist storage as the backend storage.
class GcsStorageJobInfoAccessor {
 public:
  explicit GcsStorageJobInfoAccessor(GcsStorageClient &client_impl);

  virtual ~GcsStorageJobInfoAccessor() {}

  Status AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                  const StatusCallback &callback);

  Status AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback);

 private:
  GcsStorageClient &client_impl_;
};

/// \class GcsStorageTaskInfoAccessor
/// GcsStorageTaskInfoAccessor uses persist storage as the backend storage.
class GcsStorageTaskInfoAccessor {
 public:
  explicit GcsStorageTaskInfoAccessor(GcsStorageClient &client_impl);

  virtual ~GcsStorageTaskInfoAccessor() {}

  Status AsyncAdd(const std::shared_ptr<TaskTableData> &data_ptr,
                  const StatusCallback &callback);

  Status AsyncGet(const TaskID &task_id,
                  const OptionalItemCallback<TaskTableData> &callback);

  Status AsyncDelete(const std::vector<TaskID> &task_ids, const StatusCallback &callback);

  Status AsyncAddTaskLease(const std::shared_ptr<TaskLeaseData> &data_ptr,
                           const StatusCallback &callback);

  Status AttemptTaskReconstruction(
      const std::shared_ptr<TaskReconstructionData> &data_ptr,
      const StatusCallback &callback);

 private:
  GcsStorageClient &client_impl_;
};

/// \class GcsStorageObjectInfoAccessor
/// GcsStorageObjectInfoAccessor uses persist storage as the backend storage.
class GcsStorageObjectInfoAccessor {
 public:
  explicit GcsStorageObjectInfoAccessor(GcsStorageClient &client_impl);

  virtual ~GcsStorageObjectInfoAccessor() {}

  Status AsyncGetLocations(const ObjectID &object_id,
                           const MultiItemCallback<ObjectTableData> &callback);

  Status AsyncAddLocation(const ObjectID &object_id, const ClientID &node_id,
                          const StatusCallback &callback);

  Status AsyncRemoveLocation(const ObjectID &object_id, const ClientID &node_id,
                             const StatusCallback &callback);

 private:
  GcsStorageClient &client_impl_;
};

/// \class GcsStorageNodeInfoAccessor
/// GcsStorageNodeInfoAccessor uses persist storage as the backend storage.
class GcsStorageNodeInfoAccessor {
 public:
  explicit GcsStorageNodeInfoAccessor(GcsStorageClient &client_impl);

  virtual ~GcsStorageNodeInfoAccessor() {}

  Status AsyncRegister(const GcsNodeInfo &node_info, const StatusCallback &callback);

  Status AsyncUnregister(const ClientID &node_id, const StatusCallback &callback);

  Status AsyncGetAll(const MultiItemCallback<GcsNodeInfo> &callback);

  Status AsyncGetResources(
      const ClientID &node_id,
      const OptionalItemCallback<NodeInfoAccessor::ResourceMap> &callback);

  Status AsyncUpdateResources(const ClientID &node_id,
                              const NodeInfoAccessor::ResourceMap &resources,
                              const StatusCallback &callback);

  Status AsyncDeleteResources(const ClientID &node_id,
                              const std::vector<std::string> &resource_names,
                              const StatusCallback &callback);

  Status AsyncReportHeartbeat(const std::shared_ptr<HeartbeatTableData> &data_ptr,
                              const StatusCallback &callback);

  Status AsyncReportBatchHeartbeat(
      const std::shared_ptr<HeartbeatBatchTableData> &data_ptr,
      const StatusCallback &callback);

 private:
  GcsStorageClient &client_impl_;
};

/// \class GcsStorageErrorInfoAccessor
/// GcsStorageErrorInfoAccessor uses persist storage as the backend storage.
class GcsStorageErrorInfoAccessor {
 public:
  explicit GcsStorageErrorInfoAccessor(GcsStorageClient &client_impl);

  virtual ~GcsStorageErrorInfoAccessor() = default;

  Status AsyncReportJobError(const std::shared_ptr<ErrorTableData> &data_ptr,
                             const StatusCallback &callback);

 private:
  GcsStorageClient &client_impl_;
};

/// \class GcsStorageStatsInfoAccessor
/// GcsStorageStatsInfoAccessor uses persist storage as the backend storage.
class GcsStorageStatsInfoAccessor {
 public:
  explicit GcsStorageStatsInfoAccessor(GcsStorageClient &client_impl);

  virtual ~GcsStorageStatsInfoAccessor() = default;

  Status AsyncAddProfileData(const std::shared_ptr<ProfileTableData> &data_ptr,
                             const StatusCallback &callback);

 private:
  GcsStorageClient &client_impl_;
};

/// \class GcsStorageWorkerInfoAccessor
/// GcsStorageWorkerInfoAccessor uses persist storage as the backend storage.
class GcsStorageWorkerInfoAccessor {
 public:
  explicit GcsStorageWorkerInfoAccessor(GcsStorageClient &client_impl);

  virtual ~GcsStorageWorkerInfoAccessor() = default;

  Status AsyncReportWorkerFailure(const std::shared_ptr<WorkerFailureData> &data_ptr,
                                  const StatusCallback &callback);

 private:
  GcsStorageClient &client_impl_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_STORAGE_ACCESSOR_H_
