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
                                         const ID &id, const Data &data,
                                         const GcsChangeMode &change_mode,
                                         const StatusCallback &done) {
  std::vector<std::string> args;
  args.emplace_back("PUBLISH");
  args.emplace_back(GenChannelPattern(client_id, id));

  rpc::GcsEntry gcs_entry;
  gcs_entry.set_id(id.Binary());
  gcs_entry.set_change_mode(change_mode);
  std::string data_str;
  data.SerializeToString(&data_str);
  gcs_entry.add_entries(data_str);
  args.emplace_back(gcs_entry.SerializeAsString());

  auto on_done = [done](std::shared_ptr<CallbackReply> reply) {
    if (done) {
      done(Status::OK());
    }
  };

  auto status = redis_client_->GetPrimaryContext()->RunArgvAsync(args, on_done);
  return status;
}

template <typename ID, typename Data>
Status GcsTablePubSub<ID, Data>::Subscribe(const JobID &job_id, const ClientID &client_id,
                                           const boost::optional<ID> &id,
                                           const Callback &subscribe,
                                           const StatusCallback &done) {
  auto context = redis_client_->GetPrimaryContext();
  RedisCallback redis_callback = [subscribe](std::shared_ptr<CallbackReply> reply) {
    RAY_LOG(INFO) << "Receiving Subscribe...................";
    if (!reply->IsNil()) {
      // TODO(ffbin): remove redis_callback in RedisCallbackManager
//      RAY_LOG(INFO) << "Receiving Subscribe...................1111111111111111111111";
      const auto data = reply->ReadAsPubsubData();
//      RAY_LOG(INFO) << "Receiving Subscribe...................2222222222222222222222";
      if (!data.empty()) {
//        RAY_LOG(INFO) << "Receiving Subscribe...................3333333333333333333333";
        // Data is provided. This is the callback for a message.
        if (subscribe != nullptr) {
//          RAY_LOG(INFO) << "Receiving Subscribe...................444444444444444444";
          rpc::GcsEntry gcs_entry;
          gcs_entry.ParseFromString(data);
          ID id = ID::FromBinary(gcs_entry.id());
          std::vector<Data> results;
//          RAY_LOG(INFO) << "Receiving Subscribe...................5555555555555555";

          for (int64_t i = 0; i < gcs_entry.entries_size(); i++) {
            Data result;
            result.ParseFromString(gcs_entry.entries(i));
            results.emplace_back(std::move(result));
          }
//          RAY_LOG(INFO) << "Receiving Subscribe...................6666666666666666";
          subscribe(id, gcs_entry.change_mode(), results);
        }
      }
    }
  };

  auto status = context->PSubscribeAsync(GenChannelPattern(client_id, id), redis_callback);
  if (done) {
    done(status);
  }
  return status;
}

template <typename ID, typename Data>
Status GcsTablePubSub<ID, Data>::Unsubscribe(const JobID &job_id,
                                             const ClientID &client_id,
                                             const boost::optional<ID> &id,
                                             const StatusCallback &done) {
  Status status = redis_client_->GetPrimaryContext()->PUnsubscribeAsync(GenChannelPattern(client_id, id));
  return status;
}

template <typename ID, typename Data>
std::string GcsTablePubSub<ID, Data>::GenChannelPattern(const ClientID &client_id,
                                                        const boost::optional<ID> &id) {
  std::stringstream pattern;
  pattern << pubsub_channel_;
  pattern << ":";
  if (!client_id.IsNil()) {
    pattern << client_id.Binary() << ":";
  }
  if (id) {
    pattern << id->Binary();
  } else {
    pattern << "*";
  }
  return pattern.str();
}

template class GcsTablePubSub<JobID, JobTableData>;
template class GcsTablePubSub<ActorID, ActorTableData>;
template class GcsTablePubSub<TaskID, TaskTableData>;
template class GcsTablePubSub<TaskID, TaskLeaseData>;
template class GcsTablePubSub<ObjectID, ObjectTableData>;
template class GcsTablePubSub<ClientID, GcsNodeInfo>;
template class GcsTablePubSub<ClientID, ResourceMap>;
template class GcsTablePubSub<ClientID, HeartbeatTableData>;
template class GcsTablePubSub<ClientID, HeartbeatBatchTableData>;
template class GcsTablePubSub<WorkerID, WorkerFailureData>;

}  // namespace gcs
}  // namespace ray
