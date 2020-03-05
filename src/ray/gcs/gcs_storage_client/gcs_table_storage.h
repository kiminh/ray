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
  GcsTable(GcsStorageClient &client_impl) : client_impl_(client_impl) {}

  virtual ~GcsTable() {}

  Status Put(const JobID &job_id, const KEY &key, const std::shared_ptr<VALUE> &value,
             const StatusCallback &callback);

  Status Get(const JobID &job_id, const KEY &key,
             const OptionalItemCallback<VALUE> &callback);

  Status GetAll(const JobID &job_id, const MultiItemCallback<VALUE> &callback);

  Status Delete(const JobID &job_id, const KEY &key, const StatusCallback &callback);

  Status Delete(const JobID &job_id, const std::vector<KEY> &keys,
                const StatusCallback &callback);

  Status Delete(const JobID &job_id, const StatusCallback &callback);

 protected:
  std::string table_name_;

 private:
  GcsStorageClient &client_impl_;
};

class GcsJobTable : public GcsTable<JobID, JobTableData> {
 public:
  GcsJobTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kJobTable;
  }

  virtual ~GcsJobTable() {}
};

class GcsActorTable : public GcsTable<ActorID, ActorTableData> {
 public:
  GcsActorTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kActorTable;
  }

  virtual ~GcsActorTable() {}
};

class GcsActorCheckpointTable : public GcsTable<ActorCheckpointID, ActorCheckpointData> {
 public:
  GcsActorCheckpointTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kActorCheckpointTable;
  }

  virtual ~GcsActorCheckpointTable() {}
};

class GcsActorCheckpointIdTable : public GcsTable<ActorID, ActorCheckpointIdData> {
 public:
  GcsActorCheckpointIdTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kActorCheckpointIdTable;
  }

  virtual ~GcsActorCheckpointIdTable() {}
};

class GcsTaskTable : public GcsTable<TaskID, TaskTableData> {
 public:
  GcsTaskTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kTaskTable;
  }

  virtual ~GcsTaskTable() {}
};

class GcsTaskLeaseTable : public GcsTable<TaskID, TaskLeaseData> {
 public:
  GcsTaskLeaseTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kTaskLeaseTable;
  }

  virtual ~GcsTaskLeaseTable() {}
};

class GcsTaskReconstructionTable : public GcsTable<TaskID, TaskReconstructionData> {
 public:
  GcsTaskReconstructionTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kTaskReconstructionTable;
  }

  virtual ~GcsTaskReconstructionTable() {}
};

class GcsObjectTable : public GcsTable<ObjectID, rpc::ObjectTableDataList> {
 public:
  GcsObjectTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kObjectTable;
  }

  virtual ~GcsObjectTable() {}
};

class GcsNodeTable : public GcsTable<ClientID, GcsNodeInfo> {
 public:
  GcsNodeTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kNodeTable;
  }

  virtual ~GcsNodeTable() {}
};

class GcsNodeResourceTable : public GcsTable<ClientID, rpc::ResourceMap> {
 public:
  GcsNodeResourceTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kNodeResourceTable;
  }

  virtual ~GcsNodeResourceTable() {}
};

class GcsHeartbeatTable : public GcsTable<ClientID, HeartbeatTableData> {
 public:
  GcsHeartbeatTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kHeartbeatTable;
  }

  virtual ~GcsHeartbeatTable() {}
};

class GcsHeartbeatBatchTable : public GcsTable<ClientID, HeartbeatBatchTableData> {
 public:
  GcsHeartbeatBatchTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kHeartbeatBatchTable;
  }

  virtual ~GcsHeartbeatBatchTable() {}
};

class GcsErrorInfoTable : public GcsTable<JobID, ErrorTableData> {
 public:
  GcsErrorInfoTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kErrorInfoTable;
  }

  virtual ~GcsErrorInfoTable() {}
};

class GcsProfileTable : public GcsTable<UniqueID, ProfileTableData> {
 public:
  GcsProfileTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kProfileTable;
  }

  virtual ~GcsProfileTable() {}
};

class GcsWorkerFailureTable : public GcsTable<WorkerID, WorkerFailureData> {
 public:
  GcsWorkerFailureTable(GcsStorageClient &client_impl) : GcsTable(client_impl) {
    table_name_ = kWorkerFailureTable;
  }

  virtual ~GcsWorkerFailureTable() {}
};

class GcsTableStorage {
 public:
  GcsTableStorage(const std::shared_ptr<gcs::GcsStorageClient> &gcs_storage_client)
      : gcs_storage_client_(gcs_storage_client) {}

  GcsJobTable &JobTable() {
    RAY_CHECK(job_table_ != nullptr);
    return *job_table_;
  }

  GcsActorTable &ActorTable() {
    RAY_CHECK(actor_table_ != nullptr);
    return *actor_table_;
  }

  GcsActorCheckpointTable &ActorCheckpointTable() {
    RAY_CHECK(actor_checkpoint_table_ != nullptr);
    return *actor_checkpoint_table_;
  }

  GcsActorCheckpointIdTable &ActorCheckpointIdTable() {
    RAY_CHECK(actor_checkpoint_id_table_ != nullptr);
    return *actor_checkpoint_id_table_;
  }

  GcsTaskTable &TaskTable() {
    RAY_CHECK(task_table_ != nullptr);
    return *task_table_;
  }

  GcsTaskLeaseTable &TaskLeaseTable() {
    RAY_CHECK(task_lease_table_ != nullptr);
    return *task_lease_table_;
  }

  GcsTaskReconstructionTable &TaskReconstructionTable() {
    RAY_CHECK(task_reconstruction_table_ != nullptr);
    return *task_reconstruction_table_;
  }

  GcsObjectTable &ObjectTable() {
    RAY_CHECK(object_table_ != nullptr);
    return *object_table_;
  }

  GcsNodeTable &NodeTable() {
    RAY_CHECK(node_table_ != nullptr);
    return *node_table_;
  }

  GcsNodeResourceTable &NodeResourceTable() {
    RAY_CHECK(node_resource_table_ != nullptr);
    return *node_resource_table_;
  }

  GcsHeartbeatTable &HeartbeatTable() {
    RAY_CHECK(heartbeat_table_ != nullptr);
    return *heartbeat_table_;
  }

  GcsHeartbeatBatchTable &HeartbeatBatchTable() {
    RAY_CHECK(heartbeat_batch_table_ != nullptr);
    return *heartbeat_batch_table_;
  }

  GcsErrorInfoTable &ErrorInfoTable() {
    RAY_CHECK(error_info_table_ != nullptr);
    return *error_info_table_;
  }

  GcsProfileTable &ProfileTable() {
    RAY_CHECK(profile_table_ != nullptr);
    return *profile_table_;
  }

  GcsWorkerFailureTable &WorkerFailureTable() {
    RAY_CHECK(worker_failure_table_ != nullptr);
    return *worker_failure_table_;
  }

 private:
  std::shared_ptr<GcsStorageClient> gcs_storage_client_;

  std::unique_ptr<GcsJobTable> job_table_;
  std::unique_ptr<GcsActorTable> actor_table_;
  std::unique_ptr<GcsActorCheckpointTable> actor_checkpoint_table_;
  std::unique_ptr<GcsActorCheckpointIdTable> actor_checkpoint_id_table_;
  std::unique_ptr<GcsTaskTable> task_table_;
  std::unique_ptr<GcsTaskLeaseTable> task_lease_table_;
  std::unique_ptr<GcsTaskReconstructionTable> task_reconstruction_table_;
  std::unique_ptr<GcsObjectTable> object_table_;
  std::unique_ptr<GcsNodeTable> node_table_;
  std::unique_ptr<GcsNodeResourceTable> node_resource_table_;
  std::unique_ptr<GcsHeartbeatTable> heartbeat_table_;
  std::unique_ptr<GcsHeartbeatBatchTable> heartbeat_batch_table_;
  std::unique_ptr<GcsErrorInfoTable> error_info_table_;
  std::unique_ptr<GcsProfileTable> profile_table_;
  std::unique_ptr<GcsWorkerFailureTable> worker_failure_table_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_TABLE_STORAGE_H_
