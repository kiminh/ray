#ifndef RAY_GCS_GCS_TABLE_STORAGE_H_
#define RAY_GCS_GCS_TABLE_STORAGE_H_

#include "ray/common/id.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/pb_util.h"
#include "ray/gcs/store_client/store_client.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

using rpc::ActorCheckpointData;
using rpc::ActorCheckpointIdData;
using rpc::ActorTableData;
using rpc::ErrorTableData;
using rpc::GcsNodeInfo;
using rpc::HeartbeatBatchTableData;
using rpc::HeartbeatTableData;
using rpc::JobTableData;
using rpc::ObjectTableData;
using rpc::ObjectTableDataList;
using rpc::ProfileTableData;
using rpc::ResourceMap;
using rpc::ResourceTableData;
using rpc::TaskLeaseData;
using rpc::TaskReconstructionData;
using rpc::TaskTableData;
using rpc::WorkerFailureData;

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
  GcsTable(StoreClient *store_client) : store_client_(store_client) {}

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
  StoreClient *store_client_;
};

class GcsJobTable : public GcsTable<JobID, JobTableData> {
 public:
  explicit GcsJobTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kJobTable;
  }
};

class GcsActorTable : public GcsTable<ActorID, ActorTableData> {
 public:
  explicit GcsActorTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kActorTable;
  }
};

class GcsActorCheckpointTable : public GcsTable<ActorCheckpointID, ActorCheckpointData> {
 public:
  explicit GcsActorCheckpointTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kActorCheckpointTable;
  }
};

class GcsActorCheckpointIdTable : public GcsTable<ActorID, ActorCheckpointIdData> {
 public:
  explicit GcsActorCheckpointIdTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kActorCheckpointIdTable;
  }
};

class GcsTaskTable : public GcsTable<TaskID, TaskTableData> {
 public:
  explicit GcsTaskTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kTaskTable;
  }
};

class GcsTaskLeaseTable : public GcsTable<TaskID, TaskLeaseData> {
 public:
  explicit GcsTaskLeaseTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kTaskLeaseTable;
  }
};

class GcsTaskReconstructionTable : public GcsTable<TaskID, TaskReconstructionData> {
 public:
  explicit GcsTaskReconstructionTable(StoreClient *store_client)
      : GcsTable(store_client) {
    table_name_ = kTaskReconstructionTable;
  }
};

class GcsObjectTable : public GcsTable<ObjectID, ObjectTableDataList> {
 public:
  explicit GcsObjectTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kObjectTable;
  }
};

class GcsNodeTable : public GcsTable<ClientID, GcsNodeInfo> {
 public:
  explicit GcsNodeTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kNodeTable;
  }
};

class GcsNodeResourceTable : public GcsTable<ClientID, ResourceMap> {
 public:
  explicit GcsNodeResourceTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kNodeResourceTable;
  }
};

class GcsHeartbeatTable : public GcsTable<ClientID, HeartbeatTableData> {
 public:
  explicit GcsHeartbeatTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kHeartbeatTable;
  }
};

class GcsHeartbeatBatchTable : public GcsTable<ClientID, HeartbeatBatchTableData> {
 public:
  explicit GcsHeartbeatBatchTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kHeartbeatBatchTable;
  }
};

class GcsErrorInfoTable : public GcsTable<JobID, ErrorTableData> {
 public:
  explicit GcsErrorInfoTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kErrorInfoTable;
  }
};

class GcsProfileTable : public GcsTable<UniqueID, ProfileTableData> {
 public:
  explicit GcsProfileTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kProfileTable;
  }
};

class GcsWorkerFailureTable : public GcsTable<WorkerID, WorkerFailureData> {
 public:
  explicit GcsWorkerFailureTable(StoreClient *store_client) : GcsTable(store_client) {
    table_name_ = kWorkerFailureTable;
  }
};

class GcsTableStorage {
 public:
  explicit GcsTableStorage(const std::shared_ptr<gcs::StoreClient> &store_client)
      : store_client_(store_client) {
    job_table_.reset(new GcsJobTable(store_client.get()));
    actor_table_.reset(new GcsActorTable(store_client.get()));
    actor_checkpoint_table_.reset(new GcsActorCheckpointTable(store_client.get()));
    actor_checkpoint_id_table_.reset(new GcsActorCheckpointIdTable(store_client.get()));
    task_table_.reset(new GcsTaskTable(store_client.get()));
    task_lease_table_.reset(new GcsTaskLeaseTable(store_client.get()));
    task_reconstruction_table_.reset(new GcsTaskReconstructionTable(store_client.get()));
    object_table_.reset(new GcsObjectTable(store_client.get()));
    node_table_.reset(new GcsNodeTable(store_client.get()));
    node_resource_table_.reset(new GcsNodeResourceTable(store_client.get()));
    heartbeat_table_.reset(new GcsHeartbeatTable(store_client.get()));
    heartbeat_batch_table_.reset(new GcsHeartbeatBatchTable(store_client.get()));
    error_info_table_.reset(new GcsErrorInfoTable(store_client.get()));
    profile_table_.reset(new GcsProfileTable(store_client.get()));
    worker_failure_table_.reset(new GcsWorkerFailureTable(store_client.get()));
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
