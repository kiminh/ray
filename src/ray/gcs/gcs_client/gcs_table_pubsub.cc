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
Status GcsTablePubSub<ID, Data>::Publish(const ID &id, const Data &data,
                                         const GcsChangeMode &change_mode,
                                         const StatusCallback &done) {
  rpc::GcsEntry gcs_entry;
  gcs_entry.set_id(id.Binary());
  gcs_entry.set_change_mode(change_mode);
  std::string data_str;
  data.SerializeToString(&data_str);
  gcs_entry.add_entries(data_str);
  auto message = gcs_entry.SerializeAsString();

  auto on_done = [done](std::shared_ptr<CallbackReply> reply) {
    if (done) {
      done(Status::OK());
    }
  };

  auto status = redis_client_->GetPrimaryContext()->PublishAsync(GenChannelPattern(id),
                                                                 message, on_done);
  return status;
}

template <typename ID, typename Data>
Status GcsTablePubSub<ID, Data>::Subscribe(const ID &id, const Callback &subscribe,
                                           const StatusCallback &done) {
  return Subscribe(boost::optional<ID>(id), subscribe, done);
}

template <typename ID, typename Data>
Status GcsTablePubSub<ID, Data>::SubscribeAll(const Callback &subscribe,
                                              const StatusCallback &done) {
  return Subscribe(boost::none, subscribe, done);
}

template <typename ID, typename Data>
Status GcsTablePubSub<ID, Data>::Unsubscribe(const ID &id, const StatusCallback &done) {
  if (done) {
    unsubscribe_callbacks_[id] = done;
  }
  return redis_client_->GetPrimaryContext()->PUnsubscribeAsync(GenChannelPattern(id));
}

template <typename ID, typename Data>
Status GcsTablePubSub<ID, Data>::Subscribe(const boost::optional<ID> &id,
                                           const Callback &subscribe,
                                           const StatusCallback &done) {
  auto context = redis_client_->GetPrimaryContext();
  RedisCallback redis_callback = [this, id,
                                  subscribe](std::shared_ptr<CallbackReply> reply) {
    if (!reply->IsNil()) {
      if (reply->GetMessageType() == "punsubscribe") {
        if (id && unsubscribe_callbacks_.count(*id)) {
          unsubscribe_callbacks_[*id](Status::OK());
          unsubscribe_callbacks_.erase(*id);
        }
        ray::gcs::RedisCallbackManager::instance().remove(subscribe_callback_index_[*id]);
      } else {
        // TODO(ffbin): remove redis_callback in RedisCallbackManager
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
            subscribe(id, gcs_entry.change_mode(), results.back());
          }
        }
      }
    }
  };

  int64_t callback_index;
  auto status =
      context->PSubscribeAsync(GenChannelPattern(id), redis_callback, &callback_index);
  if (id) {
    subscribe_callback_index_[*id] = callback_index;
  }

  if (done) {
    done(status);
  }
  return status;
}

template <typename ID, typename Data>
std::string GcsTablePubSub<ID, Data>::GenChannelPattern(const boost::optional<ID> &id) {
  std::stringstream pattern;
  pattern << pubsub_channel_ << ":";
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
