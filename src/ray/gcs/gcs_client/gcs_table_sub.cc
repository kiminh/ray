#include "gcs_table_sub.h"
#include "ray/common/common_protocol.h"
#include "ray/common/constants.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/redis_context.h"
#include "ray/protobuf/gcs.pb.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

template <typename ID, typename Data>
Status GcsTableSub<ID, Data>::Subscribe(const JobID &job_id, const ClientID &client_id,
                 const SubscribeCallback<ID, Data> &subscribe,
                 const StatusCallback &done) {
  RAY_LOG(INFO) << "Subscribing...........";
  auto context = redis_client_->GetPrimaryContext();

  RedisCallback redis_callback = [subscribe](std::shared_ptr<CallbackReply> reply) {
    RAY_LOG(INFO) << "hello world................";
    if (!reply->IsNil()) {
      RAY_LOG(INFO) << "!reply->IsNil()................";
      const auto data = reply->ReadAsPubsubData();

      if (data.empty()) {
        RAY_LOG(INFO) << "data is empty.........";
      } else {
        RAY_LOG(INFO) << "data is = " << data;

        if (subscribe != nullptr) {
          // Parse the notification.
          rpc::GcsEntry gcs_entry;
          gcs_entry.ParseFromString(data);
          ID id = ID::FromBinary(gcs_entry.id());
          std::unordered_map<std::string, std::shared_ptr<Data>> data_map;
          if (gcs_entry.change_mode() == rpc::GcsChangeMode::REMOVE) {
            for (const auto &key : gcs_entry.entries()) {
              data_map.emplace(key, std::shared_ptr<Data>());
            }
          } else {
            RAY_CHECK(gcs_entry.entries_size() % 2 == 0);
            for (int i = 0; i < gcs_entry.entries_size(); i += 2) {
              const auto &key = gcs_entry.entries(i);
              const auto value = std::make_shared<Data>();
              value->ParseFromString(gcs_entry.entries(i + 1));
              data_map.emplace(key, std::move(value));
            }
          }
          MapNotification<std::string, Data> notification(gcs_entry.change_mode(),
                                                          data_map);
          std::vector<MapNotification<std::string, Data>> notification_vec;
          notification_vec.emplace_back(std::move(notification));
          subscribe(client_, id, notification_vec);
        }
      }
    }
  };
  int64_t index;
  RAY_CHECK_OK(context->SubscribeAsync(client_id, pubsub_channel_, redis_callback, &index));
  return Status::OK();
}

template <typename ID, typename Data>
Status GcsTableSub<ID, Data>::Unsubscribe(const JobID &job_id, const ClientID &client_id,
                   const StatusCallback &done) {
  return Status::OK();
}

template class GcsTableSub<JobID, JobTableData>;
template class GcsTableSub<ActorID, ActorTableData>;
template class GcsTableSub<TaskID, TaskTableData>;
template class GcsTableSub<TaskID, boost::optional<TaskLeaseData>>;
template class GcsTableSub<ObjectID, ObjectTableDataList>;
template class GcsTableSub<ClientID, GcsNodeInfo>;
template class GcsTableSub<ClientID, ResourceMap>;
template class GcsTableSub<ClientID, HeartbeatTableData>;
template class GcsTableSub<ClientID, HeartbeatBatchTableData>;
template class GcsTableSub<WorkerID, WorkerFailureData>;

}  // namespace gcs
}  // namespace ray
