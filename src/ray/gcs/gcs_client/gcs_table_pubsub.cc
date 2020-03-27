#include "gcs_table_pubsub.h"
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
Status GcsTablePubSub<ID, Data>::Publish(const JobID &job_id, const ClientID &client_id,
               const Data &data,
               const StatusCallback &done) {
  RAY_LOG(INFO) << "Publishing...........";
  std::vector<std::string> args;
  args.push_back("PUBLISH");
  args.push_back(std::to_string(pubsub_channel_));

  std::string data_str;
  data.SerializeToString(&data_str);
  rpc::GcsEntry gcs_entry;
  gcs_entry.set_id(job_id.Binary());
  gcs_entry.set_change_mode(rpc::GcsChangeMode::APPEND_OR_ADD);
  gcs_entry.add_entries(data_str);
  std::string gcs_entry_str = gcs_entry.SerializeAsString();
  args.push_back(gcs_entry_str);

  auto context = redis_client_->GetPrimaryContext();
  RAY_CHECK_OK(context->RunArgvAsync(args));
  return Status::OK();
}

template <typename ID, typename Data>
Status GcsTablePubSub<ID, Data>::Subscribe(const JobID &job_id, const ClientID &client_id,
                 const Callback &subscribe,
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
        // Data is provided. This is the callback for a message.
        if (subscribe != nullptr) {
          rpc::GcsEntry gcs_entry;
          gcs_entry.ParseFromString(data);
          ID id = ID::FromBinary(gcs_entry.id());
          std::vector<Data> results;
          RAY_LOG(INFO) << "gcs_entry.entries_size() is = " << gcs_entry.entries_size();
          for (int64_t i = 0; i < gcs_entry.entries_size(); i++) {
            Data result;
            result.ParseFromString(gcs_entry.entries(i));
            results.emplace_back(std::move(result));
          }
          subscribe(id, results);
        }
      }
    }
  };
  int64_t index;
  RAY_CHECK_OK(context->SubscribeAsync(client_id, pubsub_channel_, redis_callback, &index));
  return Status::OK();
}

template <typename ID, typename Data>
Status GcsTablePubSub<ID, Data>::Unsubscribe(const JobID &job_id, const ClientID &client_id,
                   const StatusCallback &done) {
  return Status::OK();
}

template class GcsTablePubSub<JobID, JobTableData>;
template class GcsTablePubSub<ActorID, ActorTableData>;
template class GcsTablePubSub<TaskID, TaskTableData>;
template class GcsTablePubSub<TaskID, TaskLeaseData>;
template class GcsTablePubSub<ObjectID, ObjectTableDataList>;
template class GcsTablePubSub<ClientID, GcsNodeInfo>;
template class GcsTablePubSub<ClientID, ResourceMap>;
template class GcsTablePubSub<ClientID, HeartbeatTableData>;
template class GcsTablePubSub<ClientID, HeartbeatBatchTableData>;
template class GcsTablePubSub<WorkerID, WorkerFailureData>;

}  // namespace gcs
}  // namespace ray
