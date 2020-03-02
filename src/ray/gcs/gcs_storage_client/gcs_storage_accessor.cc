#include "ray/gcs/gcs_storage_client/gcs_storage_accessor.h"
#include <boost/none.hpp>
#include "gcs_storage_accessor.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"
#include "ray/gcs/gcs_storage_client/gcs_storage_redis_client.h"
#include "ray/gcs/pb_util.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

static const std::string kActorPrefix = "ACTOR_";
static const std::string kActorCheckpointPrefix = "ACTOR_CHECKPOINT_";
static const std::string kActorCheckpointIdPrefix = "ACTOR_CHECKPOINT_ID_";
static const std::string kJobPrefix = "JOB_";
static const std::string kTaskPrefix = "TASK_";
static const std::string kTaskLeasePrefix = "TASK_LEASE_";
static const std::string kTaskReconstructionPrefix = "TASK_RECONSTRUCTION_";
static const std::string kObjectPrefix = "OBJECT_";
static const std::string kNodePrefix = "NODE_";
static const std::string kHeartbeatPrefix = "HEARTBEAT_";
static const std::string kHeartbeatBatchPrefix = "HEARTBEAT_BATCH_";
static const std::string kNodeResourcePrefix = "NODE_RESOURCE_";
static const std::string kErrorInfoPrefix = "ERROR_INFO_";
static const std::string kProfilePrefix = "PROFILE_";
static const std::string kWorkerFailurePrefix = "WORKER_FAILURE_";

template <typename Data>
std::string Serialize(const std::shared_ptr<Data> &data_ptr) {
  std::string value;
  data_ptr->SerializeToString(&value);
  return value;
}

template <typename Data>
void Deserialize(const boost::optional<std::string> &data,
                 boost::optional<Data> &result) {
  if (data) {
    Data pb_data;
    pb_data.ParseFromString(*data);
    result = pb_data;
  }
}

SetCallback CreateCallbackOfSet(const StatusCallback &callback,
                                const std::string error_info) {
  auto on_done = [callback, error_info](const Status &status) {
    if (callback != nullptr) {
      Status result = status.ok() ? status : Status::Invalid(error_info);
      callback(result);
    }
  };
  return on_done;
}

DeleteCallback CreateCallbackOfDelete(const StatusCallback &callback,
                                      const std::string error_info) {
  auto on_done = [callback, error_info](const Status &status) {
    if (callback != nullptr) {
      Status result = status.ok() ? status : Status::Invalid(error_info);
      callback(result);
    }
  };
  return on_done;
}

template <typename Data>
GetCallback CreateCallbackOfGet(const OptionalItemCallback<Data> &callback,
                                const std::string error_info) {
  auto on_done = [callback, error_info](const Status &status,
                                        const boost::optional<std::string> &data) {
    if (status.ok()) {
      boost::optional<Data> result;
      Deserialize(data, result);
      callback(Status::OK(), result);
    } else {
      callback(Status::Invalid(error_info), boost::none);
    }
  };
  return on_done;
}

Status GcsStorageActorInfoAccessor::AsyncGet(
    const ActorID &actor_id, const OptionalItemCallback<ActorTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = CreateCallbackOfGet(callback, "Getting actor failed.");
  return client_impl_.Get(kActorPrefix + actor_id.Binary(), on_done);
}

Status GcsStorageActorInfoAccessor::AsyncRegister(
    const std::shared_ptr<ActorTableData> &data_ptr, const StatusCallback &callback) {
  auto on_done = CreateCallbackOfSet(callback, "Adding actor failed.");
  return client_impl_.Set(kActorPrefix + data_ptr->actor_id(), Serialize(data_ptr),
                          on_done);
}

Status GcsStorageActorInfoAccessor::AsyncUpdate(
    const ActorID &actor_id, const std::shared_ptr<ActorTableData> &data_ptr,
    const StatusCallback &callback) {
  auto on_done = [callback](const Status &status) {
    // TODO: ActorTableData::DEAD or ActorTableData::RECONSTRUCTING
    if (callback != nullptr) {
      Status result = status.ok() ? status : Status::Invalid("Updating actor failed.");
      callback(result);
    }
  };

  return client_impl_.Set(kActorPrefix + data_ptr->actor_id(), Serialize(data_ptr),
                          on_done);
}

Status GcsStorageActorInfoAccessor::AsyncAddCheckpoint(
    const std::shared_ptr<ActorCheckpointData> &data_ptr,
    const StatusCallback &callback) {
  ActorCheckpointID checkpoint_id =
      ActorCheckpointID::FromBinary(data_ptr->checkpoint_id());
  auto checkpoint_set_done = [callback, checkpoint_id, data_ptr,
                              this](const Status &status) {
    if (status.ok()) {
      ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
      auto checkpoint_id_set_done =
          CreateCallbackOfSet(callback, "Adding checkpoint id failed.");
      Status checkpoint_id_set_status =
          client_impl_.Set(kActorCheckpointIdPrefix + actor_id.Binary(),
                           checkpoint_id.Binary(), checkpoint_id_set_done);
      if (!checkpoint_id_set_status.ok()) {
        callback(checkpoint_id_set_status);
      }
    } else {
      callback(Status::Invalid("Adding checkpoint failed."));
    }
  };

  return client_impl_.Set(kActorCheckpointPrefix + data_ptr->actor_id(),
                          Serialize(data_ptr), checkpoint_set_done);
}

Status GcsStorageActorInfoAccessor::AsyncGetCheckpoint(
    const ActorCheckpointID &checkpoint_id,
    const OptionalItemCallback<ActorCheckpointData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = CreateCallbackOfGet(callback, "Invalid checkpoint id.");
  return client_impl_.Get(kActorCheckpointPrefix + checkpoint_id.Binary(), on_done);
}

Status GcsStorageActorInfoAccessor::AsyncGetCheckpointID(
    const ActorID &actor_id,
    const OptionalItemCallback<ActorCheckpointIdData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = CreateCallbackOfGet(callback, "Checkpoint not found.");
  return client_impl_.Get(kActorCheckpointIdPrefix + actor_id.Binary(), on_done);
}

Status GcsStorageJobInfoAccessor::AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                                           const StatusCallback &callback) {
  auto on_done = CreateCallbackOfSet(callback, "Adding job failed.");
  return client_impl_.Set(kJobPrefix + data_ptr->job_id(), Serialize(data_ptr), on_done);
}

Status GcsStorageJobInfoAccessor::AsyncMarkFinished(const JobID &job_id,
                                                    const StatusCallback &callback) {
  std::shared_ptr<JobTableData> data_ptr =
      CreateJobTableData(job_id, /*is_dead*/ true, /*time_stamp*/ std::time(nullptr),
                         /*node_manager_address*/ "", /*driver_pid*/ -1);
  auto on_done = CreateCallbackOfSet(callback, "Marking job finished failed.");
  return client_impl_.Set(kJobPrefix + data_ptr->job_id(), Serialize(data_ptr), on_done);
}

Status GcsStorageTaskInfoAccessor::AsyncAdd(
    const std::shared_ptr<TaskTableData> &data_ptr, const StatusCallback &callback) {
  auto on_done = CreateCallbackOfSet(callback, "Adding task failed.");
  return client_impl_.Set(kJobPrefix + data_ptr->task().task_spec().task_id(),
                          Serialize(data_ptr), on_done);
}

Status GcsStorageTaskInfoAccessor::AsyncGet(
    const TaskID &task_id, const OptionalItemCallback<TaskTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = CreateCallbackOfGet(callback, "Task not exist.");
  return client_impl_.Get(kTaskPrefix + task_id.Binary(), on_done);
}

Status GcsStorageTaskInfoAccessor::AsyncDelete(const std::vector<TaskID> &task_ids,
                                               const StatusCallback &callback) {
  std::vector<std::string> keys;
  for (TaskID task_id : task_ids) {
    keys.push_back(kTaskPrefix + task_id.Binary());
  }
  auto on_done = CreateCallbackOfDelete(callback, "Deleting task failed.");
  return client_impl_.Delete(keys, on_done);
}

Status GcsStorageTaskInfoAccessor::AsyncAddTaskLease(
    const std::shared_ptr<TaskLeaseData> &data_ptr, const StatusCallback &callback) {
  auto on_done = CreateCallbackOfSet(callback, "Adding task lease failed.");
  return client_impl_.Set(kTaskLeasePrefix + data_ptr->task_id(), Serialize(data_ptr),
                          on_done);
}

Status GcsStorageTaskInfoAccessor::AttemptTaskReconstruction(
    const std::shared_ptr<TaskReconstructionData> &data_ptr,
    const StatusCallback &callback) {
  auto on_done = CreateCallbackOfSet(callback, "Updating task reconstruction failed.");
  return client_impl_.Set(kTaskReconstructionPrefix + data_ptr->task_id(),
                          Serialize(data_ptr), on_done);
}

Status GcsStorageObjectInfoAccessor::AsyncGetLocations(
    const ObjectID &object_id, const MultiItemCallback<ObjectTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](const Status &status,
                            const boost::optional<std::string> &data) {
    std::vector<ObjectTableData> result;
    if (status.ok() && data) {
      rpc::ObjectTableDataVector pb_data;
      pb_data.ParseFromString(*data);
      for (int index = 0; index < pb_data.items_size(); ++index) {
        auto item = pb_data.items(index);
        result.push_back(item);
      }
    }
    Status statusRet =
        status.ok() ? status : Status::Invalid("Getting object location failed.");
    callback(statusRet, result);
  };

  return client_impl_.Get(kObjectPrefix + object_id.Binary(), on_done);
}

Status GcsStorageObjectInfoAccessor::AsyncAddLocation(const ObjectID &object_id,
                                                      const ClientID &node_id,
                                                      const StatusCallback &callback) {
  auto get_done = [this, object_id, node_id, callback](
                      Status status, const std::vector<ObjectTableData> &result) {
    if (status.ok()) {
      std::vector<ObjectTableData> data_result(result);
      ObjectTableData object_table_data;
      object_table_data.set_manager(node_id.Binary());
      data_result.push_back(object_table_data);

      rpc::ObjectTableDataVector pb_data;
      for (ObjectTableData data : data_result) {
        pb_data.add_items()->CopyFrom(data);
      }

      std::string value;
      pb_data.SerializeToString(&value);
      auto set_done = CreateCallbackOfSet(callback, "Adding object location failed.");
      Status set_status =
          client_impl_.Set(kObjectPrefix + object_id.Binary(), value, set_done);
      if (!set_status.ok()) {
        callback(set_status);
      }
    } else {
      callback(status);
    }
  };
  return AsyncGetLocations(object_id, get_done);
}

Status GcsStorageObjectInfoAccessor::AsyncRemoveLocation(const ObjectID &object_id,
                                                         const ClientID &node_id,
                                                         const StatusCallback &callback) {
  auto get_done = [this, object_id, node_id, callback](
                      Status status, const std::vector<ObjectTableData> &result) {
    if (status.ok()) {
      std::vector<ObjectTableData> data_result(result);
      data_result.erase(remove_if(data_result.begin(), data_result.end(),
                                  [node_id](ObjectTableData data) {
                                    return data.manager() == node_id.Binary();
                                  }),
                        data_result.end());

      rpc::ObjectTableDataVector pb_data;
      for (ObjectTableData data : data_result) {
        pb_data.add_items()->CopyFrom(data);
      }

      std::string value;
      pb_data.SerializeToString(&value);
      auto set_done = CreateCallbackOfSet(callback, "Adding object location failed.");
      (void)client_impl_.Set(kObjectPrefix + object_id.Binary(), value, set_done);
    } else {
      callback(status);
    }
  };
  return AsyncGetLocations(object_id, get_done);
}

Status GcsStorageNodeInfoAccessor::AsyncRegister(const GcsNodeInfo &node_info,
                                                 const StatusCallback &callback) {
  auto on_done = CreateCallbackOfSet(callback, "Registering node failed.");
  std::string value;
  node_info.SerializeToString(&value);
  return client_impl_.Set(kNodePrefix, node_info.node_id(), value, on_done);
}

Status GcsStorageNodeInfoAccessor::AsyncUnregister(const ClientID &node_id,
                                                   const StatusCallback &callback) {
  auto on_done = CreateCallbackOfDelete(callback, "Unregistering node failed.");
  return client_impl_.Delete(kNodePrefix, node_id.Binary(), on_done);
}

Status GcsStorageNodeInfoAccessor::AsyncGetAll(
    const MultiItemCallback<GcsNodeInfo> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](const Status &status,
                            const std::vector<std::string> &data) {
    std::vector<GcsNodeInfo> result;
    if (status.ok()) {
      for (std::string item : data) {
        GcsNodeInfo node_info;
        node_info.ParseFromString(item);
        result.push_back(node_info);
      }
    }

    Status statusRet =
        status.ok() ? status : Status::Invalid("Getting all actor failed.");
    callback(statusRet, result);
  };

  return client_impl_.GetAll(kNodePrefix, on_done);
}

Status GcsStorageNodeInfoAccessor::AsyncReportHeartbeat(
    const std::shared_ptr<HeartbeatTableData> &data_ptr, const StatusCallback &callback) {
  auto on_done = CreateCallbackOfSet(callback, "Reporting heartbeat failed.");
  return client_impl_.Set(kHeartbeatPrefix + data_ptr->client_id(), Serialize(data_ptr),
                          on_done);
}

Status GcsStorageNodeInfoAccessor::AsyncReportBatchHeartbeat(
    const std::shared_ptr<HeartbeatBatchTableData> &data_ptr,
    const StatusCallback &callback) {
  auto on_done = CreateCallbackOfSet(callback, "Reporting batch heartbeat failed.");
  return client_impl_.Set(kHeartbeatBatchPrefix, Serialize(data_ptr), on_done);
}

Status GcsStorageNodeInfoAccessor::AsyncGetResources(
    const ClientID &node_id,
    const OptionalItemCallback<NodeInfoAccessor::ResourceMap> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](const Status &status,
                            const boost::optional<std::string> &data) {
    boost::optional<NodeInfoAccessor::ResourceMap> result;
    if (status.ok() && data) {
      rpc::ResourceMap pb_data;
      pb_data.ParseFromString(*data);
      NodeInfoAccessor::ResourceMap resources;
      for (int index = 0; index < pb_data.items_size(); ++index) {
        auto item = pb_data.items(index);
        resources[item.key()] = std::make_shared<rpc::ResourceTableData>(item.value());
      }
      result = resources;
    }
    Status statusRet =
        status.ok() ? status : Status::Invalid("Getting node resources failed.");
    callback(statusRet, result);
  };

  return client_impl_.Get(kNodeResourcePrefix + node_id.Binary(), on_done);
}

Status GcsStorageNodeInfoAccessor::AsyncUpdateResources(
    const ClientID &node_id, const NodeInfoAccessor::ResourceMap &resources,
    const StatusCallback &callback) {
  rpc::ResourceMap pb_data;
  for (auto iter = resources.begin(); iter != resources.end(); ++iter) {
    rpc::ResourceItem item;
    item.set_key(iter->first);
    item.mutable_value()->CopyFrom(*(iter->second));
    pb_data.add_items()->CopyFrom(item);
  }

  std::string value;
  pb_data.SerializeToString(&value);
  auto on_done = CreateCallbackOfSet(callback, "Updating resources failed.");
  return client_impl_.Set(kNodeResourcePrefix + node_id.Binary(), value, on_done);
}

Status GcsStorageNodeInfoAccessor::AsyncDeleteResources(
    const ClientID &node_id, const std::vector<std::string> &resource_names,
    const StatusCallback &callback) {
  auto on_done = CreateCallbackOfDelete(callback, "Deleting node failed.");
  return client_impl_.Delete(kNodePrefix + node_id.Binary(), on_done);
}

Status GcsStorageErrorInfoAccessor::AsyncReportJobError(
    const std::shared_ptr<ErrorTableData> &data_ptr, const StatusCallback &callback) {
  auto on_done = CreateCallbackOfSet(callback, "Reporting job error failed.");
  return client_impl_.Set(kErrorInfoPrefix + data_ptr->job_id(), Serialize(data_ptr),
                          on_done);
}

Status GcsStorageStatsInfoAccessor::AsyncAddProfileData(
    const std::shared_ptr<ProfileTableData> &data_ptr, const StatusCallback &callback) {
  auto on_done = CreateCallbackOfSet(callback, "Adding profile failed.");
  return client_impl_.Set(kProfilePrefix + UniqueID::FromRandom().Binary(),
                          Serialize(data_ptr), on_done);
}

Status GcsStorageWorkerInfoAccessor::AsyncReportWorkerFailure(
    const std::shared_ptr<WorkerFailureData> &data_ptr, const StatusCallback &callback) {
  auto on_done = CreateCallbackOfSet(callback, "Reporting worker failure failed.");
  return client_impl_.Set(kWorkerFailurePrefix + data_ptr->worker_address().worker_id(),
                          Serialize(data_ptr), on_done);
}

}  // namespace gcs
}  // namespace ray
