#ifndef RAY_GCS_GCS_TABLE_SUB_H_
#define RAY_GCS_GCS_TABLE_SUB_H_

#include <ray/protobuf/gcs.pb.h>
#include "ray/common/id.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/pb_util.h"
#include "ray/gcs/redis_client.h"
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
using rpc::TablePubsub;


template <typename ID, typename Data>
class GcsTableSub {
 public:
  GcsTableSub(std::shared_ptr<RedisClient> redis_client) : redis_client_(redis_client) {}

  virtual ~GcsTableSub() {
  }

  Status Subscribe(const JobID &job_id, const ClientID &client_id,
                   const SubscribeCallback<ID, Data> &subscribe,
                   const StatusCallback &done);

  Status Unsubscribe(const JobID &job_id, const ClientID &client_id,
                     const StatusCallback &done);

 protected:
  TablePubsub pubsub_channel_;

 private:
  std::shared_ptr<RedisClient> redis_client_;
};

class GcsJobTableSub : public GcsTableSub<JobID, JobTableData> {
 public:
  explicit GcsJobTableSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTableSub(redis_client) {
    pubsub_channel_ = TablePubsub::JOB_PUBSUB;
  }
};

class GcsActorTableSub : public GcsTableSub<ActorID, ActorTableData> {
 public:
  explicit GcsActorTableSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTableSub(redis_client) {
    pubsub_channel_ = TablePubsub::ACTOR_PUBSUB;
  }
};

class GcsActorCheckpointTableSub : public GcsTableSub<ActorCheckpointID, ActorCheckpointData> {
 public:
  explicit GcsActorCheckpointTableSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTableSub(redis_client) {
  }
};

class GcsActorCheckpointIdTableSub : public GcsTableSub<ActorID, ActorCheckpointIdData> {
 public:
  explicit GcsActorCheckpointIdTableSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTableSub(redis_client) {
  }
};

class GcsTaskTableSub : public GcsTableSub<TaskID, TaskTableData> {
 public:
  explicit GcsTaskTableSub(std::shared_ptr<RedisClient> redis_client) : GcsTableSub(redis_client) {
  }
};

class GcsTaskLeaseTableSub : public GcsTableSub<TaskID, TaskLeaseData> {
 public:
  explicit GcsTaskLeaseTableSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTableSub(redis_client) {
  }
};

class GcsTaskReconstructionTableSub : public GcsTableSub<TaskID, TaskReconstructionData> {
 public:
  explicit GcsTaskReconstructionTableSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTableSub(redis_client) {
  }
};

class GcsObjectTableSub : public GcsTableSub<ObjectID, ObjectTableDataList> {
 public:
  explicit GcsObjectTableSub(std::shared_ptr<RedisClient> redis_client) : GcsTableSub(redis_client) {
  }
};

class GcsNodeTableSub : public GcsTableSub<ClientID, GcsNodeInfo> {
 public:
  explicit GcsNodeTableSub(std::shared_ptr<RedisClient> redis_client) : GcsTableSub(redis_client) {
  }
};

class GcsNodeResourceTableSub : public GcsTableSub<ClientID, ResourceMap> {
 public:
  explicit GcsNodeResourceTableSub(std::shared_ptr<RedisClient> redis_client) : GcsTableSub(redis_client) {
  }
};

class GcsHeartbeatTableSub : public GcsTableSub<ClientID, HeartbeatTableData> {
 public:
  explicit GcsHeartbeatTableSub(std::shared_ptr<RedisClient> redis_client) : GcsTableSub(redis_client) {
  }
};

class GcsHeartbeatBatchTableSub : public GcsTableSub<ClientID, HeartbeatBatchTableData> {
 public:
  explicit GcsHeartbeatBatchTableSub(std::shared_ptr<RedisClient> redis_client) : GcsTableSub(redis_client) {
  }
};

class GcsWorkerFailureTableSub : public GcsTableSub<WorkerID, WorkerFailureData> {
 public:
  explicit GcsWorkerFailureTableSub(std::shared_ptr<RedisClient> redis_client) : GcsTableSub(redis_client) {
  }
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_TABLE_SUB_H_
