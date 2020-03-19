#ifndef RAY_GCS_GCS_TABLE_STORAGE_H_
#define RAY_GCS_GCS_TABLE_STORAGE_H_

#include "ray/common/id.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/pb_util.h"
#include "ray/gcs/store_client/store_client.h"

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
  GcsTable(StoreClient &store_client) : store_client_(store_client) {}

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
  StoreClient &store_client_;
};

 class GcsJobTable : public GcsTable<JobID, rpc::JobTableData> {
 public:
  GcsJobTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kJobTable;
  }

  virtual ~GcsJobTable() {}
};

class GcsActorTable : public GcsTable<ActorID, rpc::ActorTableData> {
 public:
  GcsActorTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kActorTable;
  }

  virtual ~GcsActorTable() {}
};

 class GcsActorCheckpointTable : public GcsTable<ActorCheckpointID, rpc::ActorCheckpointData> {
 public:
  GcsActorCheckpointTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kActorCheckpointTable;
  }

  virtual ~GcsActorCheckpointTable() {}
};

class GcsActorCheckpointIdTable : public GcsTable<ActorID, rpc::ActorCheckpointIdData> {
 public:
  GcsActorCheckpointIdTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kActorCheckpointIdTable;
  }

  virtual ~GcsActorCheckpointIdTable() {}
};

class GcsTaskTable : public GcsTable<TaskID, rpc::TaskTableData> {
 public:
  GcsTaskTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kTaskTable;
  }

  virtual ~GcsTaskTable() {}
};

class GcsTaskLeaseTable : public GcsTable<TaskID, rpc::TaskLeaseData> {
 public:
  GcsTaskLeaseTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kTaskLeaseTable;
  }

  virtual ~GcsTaskLeaseTable() {}
};

class GcsTaskReconstructionTable : public GcsTable<TaskID, rpc::TaskReconstructionData> {
 public:
  GcsTaskReconstructionTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kTaskReconstructionTable;
  }

  virtual ~GcsTaskReconstructionTable() {}
};

class GcsObjectTable : public GcsTable<ObjectID, rpc::ObjectTableDataList> {
 public:
  GcsObjectTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kObjectTable;
  }

  virtual ~GcsObjectTable() {}
};

 class GcsNodeTable : public GcsTable<ClientID, rpc::GcsNodeInfo> {
 public:
  GcsNodeTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kNodeTable;
  }

  virtual ~GcsNodeTable() {}
};

class GcsNodeResourceTable : public GcsTable<ClientID, rpc::ResourceMap> {
 public:
  GcsNodeResourceTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kNodeResourceTable;
  }

  virtual ~GcsNodeResourceTable() {}
};

 class GcsHeartbeatTable : public GcsTable<ClientID, rpc::HeartbeatTableData> {
 public:
  GcsHeartbeatTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kHeartbeatTable;
  }

  virtual ~GcsHeartbeatTable() {}
};

 class GcsHeartbeatBatchTable : public GcsTable<ClientID, rpc::HeartbeatBatchTableData> {
 public:
  GcsHeartbeatBatchTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kHeartbeatBatchTable;
  }

  virtual ~GcsHeartbeatBatchTable() {}
};

 class GcsErrorInfoTable : public GcsTable<JobID, rpc::ErrorTableData> {
 public:
  GcsErrorInfoTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kErrorInfoTable;
  }

  virtual ~GcsErrorInfoTable() {}
};

class GcsProfileTable : public GcsTable<UniqueID, rpc::ProfileTableData> {
 public:
  GcsProfileTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kProfileTable;
  }

  virtual ~GcsProfileTable() {}
};

class GcsWorkerFailureTable : public GcsTable<WorkerID, rpc::WorkerFailureData> {
 public:
  GcsWorkerFailureTable(StoreClient &store_client) : GcsTable(store_client) {
    table_name_ = kWorkerFailureTable;
  }

  virtual ~GcsWorkerFailureTable() {}
};

class GcsTableStorage {
 public:
  explicit GcsTableStorage(const std::shared_ptr<gcs::StoreClient> &store_client)
      : store_client_(store_client) {
    job_table_.reset(new GcsJobTable(*store_client));
    actor_table_.reset(new GcsActorTable(*store_client));
    actor_checkpoint_table_.reset(new GcsActorCheckpointTable(*store_client));
    actor_checkpoint_id_table_.reset(new GcsActorCheckpointIdTable(*store_client));
    task_table_.reset(new GcsTaskTable(*store_client));
    task_lease_table_.reset(new GcsTaskLeaseTable(*store_client));
    task_reconstruction_table_.reset(new GcsTaskReconstructionTable(*store_client));
    object_table_.reset(new GcsObjectTable(*store_client));
    node_table_.reset(new GcsNodeTable(*store_client));
    node_resource_table_.reset(new GcsNodeResourceTable(*store_client));
    heartbeat_table_.reset(new GcsHeartbeatTable(*store_client));
    heartbeat_batch_table_.reset(new GcsHeartbeatBatchTable(*store_client));
    error_info_table_.reset(new GcsErrorInfoTable(*store_client));
    profile_table_.reset(new GcsProfileTable(*store_client));
    worker_failure_table_.reset(new GcsWorkerFailureTable(*store_client));
  }

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
  std::shared_ptr<StoreClient> store_client_;

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
