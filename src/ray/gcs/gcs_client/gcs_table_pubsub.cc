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
    if (!reply->IsNil()) {
      const auto data = reply->ReadAsPubsubData();
      if (!data.empty()) {
        // Data is provided. This is the callback for a message.
        if (subscribe != nullptr) {
          rpc::GcsEntry gcs_entry;
          gcs_entry.ParseFromString(data);
          ID id = ID::FromBinary(gcs_entry.id());
          std::vector<Data> results;

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

  auto status = context->PSubscribeAsync(GenChannelPattern(client_id, id), redis_callback,
                                  &callback_index_);
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
  int64_t index;
  RedisCallback redis_callback = [done](std::shared_ptr<CallbackReply> reply) {
    RAY_LOG(INFO) << "HELLO WORLD..........";
    if (done) {
      done(Status::OK());
    }
  };
  Status status = redis_client_->GetPrimaryContext()->PUnsubscribeAsync(GenChannelPattern(client_id, id),
      redis_callback, &index);
  return status;
}

template <typename ID, typename Data>
std::string GcsTablePubSub<ID, Data>::GenChannelPattern(const ClientID &client_id,
                                                        const boost::optional<ID> &id) {
  RAY_LOG(INFO) << "client_id = " << client_id << ", id = " << *id << ", pubsub_channel_ = " << pubsub_channel_;
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
  RAY_LOG(INFO) << "pattern size = " << pattern.str().length();
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
