#ifndef RAY_GCS_GCS_TABLE_STORAGE_H_
#define RAY_GCS_GCS_TABLE_STORAGE_H_

#include "ray/common/id.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"
#include "ray/gcs/subscription_executor.h"

namespace ray {
namespace gcs {

static const std::string kJobTable = "JOB";
static const std::string kActorTable = "ACTOR";
static const std::string kActorCheckpointTable = "ACTOR_CHECKPOINT";
static const std::string kActorCheckpointIdTable = "ACTOR_CHECKPOINT_ID";
static const std::string kTaskTable = "TASK";
static const std::string kTaskLeaseTable = "TASK_LEASE";
static const std::string kTaskReconstructionTable = "TASK_RECONSTRUCTION";
static const std::string kObjectTable = "OBJECT";
static const std::string kNodeTable = "NODE";
static const std::string kHeartbeatTable = "HEARTBEAT";
static const std::string kHeartbeatBatchTable = "HEARTBEAT_BATCH";
static const std::string kNodeResourceTable = "NODE_RESOURCE";
static const std::string kErrorInfoTable = "ERROR_INFO";
static const std::string kProfileTable = "PROFILE";
static const std::string kWorkerFailureTable = "WORKER_FAILURE";

template <typename KEY, typename VALUE>
class GcsTable {
 public:
  GcsTable(GcsStorageClient &client_impl)
      : client_impl_(client_impl) {}

  virtual ~GcsTable() {}

  Status Put(const JobID &job_id, const KEY &key, const std::shared_ptr<VALUE> &value,
             const StatusCallback &callback);

  Status Get(const JobID &job_id, const KEY &key, const OptionalItemCallback<VALUE> &callback);

  Status GetAll(const JobID &job_id, const MultiItemCallback<VALUE> &callback);

  Status Delete(const JobID &job_id, const KEY &key, const StatusCallback &callback);

  Status Delete(const JobID &job_id, const StatusCallback &callback);

 protected:
  std::string name_;

 private:
  GcsStorageClient &client_impl_;
};

class GcsJobTable : public GcsTable<JobID, JobTableData> {
 public:
  GcsJobTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    name_ = kJobTable;
  }

  virtual ~GcsJobTable() {}
};

class GcsActorTable : public GcsTable<ActorID, ActorTableData> {
 public:
  GcsActorTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    name_ = kActorTable;
  }

  virtual ~GcsActorTable() {}
};

class GcsActorCheckpointTable : public GcsTable<ActorID, ActorCheckpointData> {
 public:
  GcsActorCheckpointTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    name_ = kActorCheckpointTable;
  }

  virtual ~GcsActorCheckpointTable() {}
};

class GcsActorCheckpointIdTable : public GcsTable<ActorCheckpointID, ActorCheckpointIdData> {
 public:
  GcsActorCheckpointIdTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    name_ = kActorCheckpointIdTable;
  }

  virtual ~GcsActorCheckpointIdTable() {}
};

class GcsTaskTable : public GcsTable<TaskID, TaskTableData> {
 public:
  GcsTaskTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    name_ = kTaskTable;
  }

  virtual ~GcsTaskTable() {}
};

class GcsTaskLeaseTable : public GcsTable<TaskID, TaskLeaseData> {
 public:
  GcsTaskLeaseTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    name_ = kTaskLeaseTable;
  }

  virtual ~GcsTaskLeaseTable() {}
};

class GcsTaskReconstructionTable : public GcsTable<TaskID, TaskReconstructionData> {
 public:
  GcsTaskReconstructionTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    name_ = kTaskReconstructionTable;
  }

  virtual ~GcsTaskReconstructionTable() {}
};

class GcsObjectTable : public GcsTable<ObjectID, ObjectTableData> {
 public:
  GcsObjectTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    name_ = kObjectTable;
  }

  virtual ~GcsObjectTable() {}
};

class GcsNodeTable : public GcsTable<ClientID, GcsNodeInfo> {
 public:
  GcsNodeTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    name_ = kObjectTable;
  }

  virtual ~GcsNodeTable() {}
};

class GcsNodeResourceTable : public GcsTable<ClientID, ResourceMap> {
 public:
  GcsNodeResourceTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    name_ = kObjectTable;
  }

  virtual ~GcsNodeResourceTable() {}
};

class GcsNodeTable {
 public:
  explicit GcsNodeTable(GcsStorageClient &client_impl)
      : client_impl_(client_impl) {}

  virtual ~GcsNodeTable() {}

  Status Put(const ClientID &node_id, const GcsNodeInfo &node_info, const StatusCallback &callback);

  Status GetAll(const MultiItemCallback<GcsNodeInfo> &callback);

  Status Delete(const ClientID &node_id, const StatusCallback &callback);

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

class GcsErrorTable {
 public:
  explicit GcsErrorTable(GcsStorageClient &client_impl)
      : client_impl_(client_impl) {}

  virtual ~GcsErrorTable() = default;

  // TODO:
  Status Put(const std::shared_ptr<ErrorTableData> &data_ptr,
             const StatusCallback &callback);

 private:
  GcsStorageClient &client_impl_;
};

class GcsProfileTable {
 public:
  explicit GcsProfileTable(GcsStorageClient &client_impl)
      : client_impl_(client_impl) {}

  virtual ~GcsProfileTable() = default;

  Status Put(const std::shared_ptr<ProfileTableData> &data_ptr,
             const StatusCallback &callback);

 private:
  GcsStorageClient &client_impl_;
};

class GcsWorkerFailureTable {
 public:
  explicit GcsWorkerFailureTable(GcsStorageClient &client_impl)
      : client_impl_(client_impl) {}

  virtual ~GcsWorkerFailureTable() = default;

  Status Put(const std::shared_ptr<WorkerFailureData> &data_ptr,
             const StatusCallback &callback);

 private:
  GcsStorageClient &client_impl_;
};

class GcsTableStorage {

};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_TABLE_STORAGE_H_
