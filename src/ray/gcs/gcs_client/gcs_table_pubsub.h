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
using rpc::GcsChangeMode;
using rpc::GcsNodeInfo;
using rpc::HeartbeatBatchTableData;
using rpc::HeartbeatTableData;
using rpc::JobTableData;
using rpc::ObjectTableData;
using rpc::ObjectTableDataList;
using rpc::ProfileTableData;
using rpc::ResourceMap;
using rpc::ResourceTableData;
using rpc::TablePubsub;
using rpc::TaskLeaseData;
using rpc::TaskReconstructionData;
using rpc::TaskTableData;
using rpc::WorkerFailureData;

template <typename ID, typename Data>
class GcsTablePubSub {
 public:
  using Callback = std::function<void(const ID &id, const rpc::GcsChangeMode &change_mode,
                                      const std::vector<Data> &data)>;

  GcsTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : redis_client_(redis_client) {}

  virtual ~GcsTablePubSub() = default;

  Status Publish(const ID &id, const Data &data, const GcsChangeMode &change_mode,
                 const StatusCallback &done);

  Status Subscribe(const ID &id, const Callback &subscribe, const StatusCallback &done);

  Status SubscribeAll(const Callback &subscribe, const StatusCallback &done);

  Status Unsubscribe(const ID &id, const StatusCallback &done);

 protected:
  TablePubsub pubsub_channel_;

 private:
  Status Subscribe(const boost::optional<ID> &id, const Callback &subscribe,
                   const StatusCallback &done);

  std::string GenChannelPattern(const boost::optional<ID> &id = boost::none);

  std::shared_ptr<RedisClient> redis_client_;
  std::unordered_map<ID, int64_t> subscribe_callback_index;
  std::unordered_map<ID, StatusCallback> unsubscribe_callbacks;
};

class GcsJobTablePubSub : public GcsTablePubSub<JobID, JobTableData> {
 public:
  explicit GcsJobTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pubsub_channel_ = TablePubsub::JOB_PUBSUB;
  }
};

class GcsActorTablePubSub : public GcsTablePubSub<ActorID, ActorTableData> {
 public:
  explicit GcsActorTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pubsub_channel_ = TablePubsub::ACTOR_PUBSUB;
  }
};

class GcsTaskTablePubSub : public GcsTablePubSub<TaskID, TaskTableData> {
 public:
  explicit GcsTaskTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pubsub_channel_ = TablePubsub::TASK_PUBSUB;
  }
};

class GcsTaskLeaseTablePubSub : public GcsTablePubSub<TaskID, TaskLeaseData> {
 public:
  explicit GcsTaskLeaseTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pubsub_channel_ = TablePubsub::TASK_LEASE_PUBSUB;
  }
};

class GcsObjectTablePubSub : public GcsTablePubSub<ObjectID, ObjectTableData> {
 public:
  explicit GcsObjectTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pubsub_channel_ = TablePubsub::OBJECT_PUBSUB;
  }
};

class GcsNodeTablePubSub : public GcsTablePubSub<ClientID, GcsNodeInfo> {
 public:
  explicit GcsNodeTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pubsub_channel_ = TablePubsub::CLIENT_PUBSUB;
  }
};

class GcsNodeResourceTablePubSub : public GcsTablePubSub<ClientID, ResourceMap> {
 public:
  explicit GcsNodeResourceTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pubsub_channel_ = TablePubsub::NODE_RESOURCE_PUBSUB;
  }
};

class GcsHeartbeatTablePubSub : public GcsTablePubSub<ClientID, HeartbeatTableData> {
 public:
  explicit GcsHeartbeatTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pubsub_channel_ = TablePubsub::HEARTBEAT_PUBSUB;
  }
};

class GcsHeartbeatBatchTablePubSub
    : public GcsTablePubSub<ClientID, HeartbeatBatchTableData> {
 public:
  explicit GcsHeartbeatBatchTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pubsub_channel_ = TablePubsub::HEARTBEAT_BATCH_PUBSUB;
  }
};

class GcsWorkerFailureTablePubSub : public GcsTablePubSub<WorkerID, WorkerFailureData> {
 public:
  explicit GcsWorkerFailureTablePubSub(std::shared_ptr<RedisClient> redis_client)
      : GcsTablePubSub(redis_client) {
    pubsub_channel_ = TablePubsub::WORKER_FAILURE_PUBSUB;
  }
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_TABLE_SUB_H_
