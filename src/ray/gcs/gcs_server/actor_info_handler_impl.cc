#include "actor_info_handler_impl.h"
#include "absl/time/clock.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultActorInfoHandler::HandleGetActorInfo(
    const rpc::GetActorInfoRequest &request, rpc::GetActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Getting actor info, actor id = " << actor_id;

  auto on_done = [actor_id, reply, send_reply_callback](
                     Status status, const boost::optional<ActorTableData> &result) {
    if (status.ok()) {
      if (result) {
        reply->mutable_actor_table_data()->CopyFrom(*result);
      }
      RAY_LOG(DEBUG) << "Finished getting actor info, actor id = " << actor_id;
    } else {
      RAY_LOG(ERROR) << "Failed to get actor info: " << status.ToString()
                     << ", actor id = " << actor_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status =
      gcs_table_storage_->ActorTable().Get(actor_id.JobId(), actor_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void DefaultActorInfoHandler::HandleRegisterActorInfo(
    const rpc::RegisterActorInfoRequest &request, rpc::RegisterActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_table_data().actor_id());
  RAY_LOG(DEBUG) << "Registering actor info, actor id = " << actor_id;
  auto actor_table_data = std::make_shared<ActorTableData>();
  actor_table_data->CopyFrom(request.actor_table_data());
  auto on_done = [this, actor_id, actor_table_data, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to register actor info: " << status.ToString()
                     << ", actor id = " << actor_id;
    } else {
      RAY_LOG(DEBUG) << "Finished registering actor info, actor id = " << actor_id;
      RAY_CHECK_OK(actor_pub_.Publish(actor_id, *actor_table_data,
                                      GcsChangeMode::APPEND_OR_ADD, nullptr));
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_table_storage_->ActorTable().Put(actor_id.JobId(), actor_id,
                                                       actor_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultActorInfoHandler::HandleUpdateActorInfo(
    const rpc::UpdateActorInfoRequest &request, rpc::UpdateActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Updating actor info, actor id = " << actor_id;
  auto actor_table_data = std::make_shared<ActorTableData>();
  actor_table_data->CopyFrom(request.actor_table_data());
  auto on_done = [this, actor_id, actor_table_data, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to update actor info: " << status.ToString()
                     << ", actor id = " << actor_id;
    } else {
      RAY_LOG(DEBUG) << "Finished updating actor info, actor id = " << actor_id;
      RAY_CHECK_OK(actor_pub_.Publish(actor_id, *actor_table_data,
                                      GcsChangeMode::APPEND_OR_ADD, nullptr));
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_table_storage_->ActorTable().Put(actor_id.JobId(), actor_id,
                                                       actor_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultActorInfoHandler::HandleAddActorCheckpoint(
    const AddActorCheckpointRequest &request, AddActorCheckpointReply *reply,
    SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.checkpoint_data().actor_id());
  ActorCheckpointID checkpoint_id =
      ActorCheckpointID::FromBinary(request.checkpoint_data().checkpoint_id());
  RAY_LOG(DEBUG) << "Adding actor checkpoint, actor id = " << actor_id
                 << ", checkpoint id = " << checkpoint_id;
  auto actor_checkpoint_data = std::make_shared<ActorCheckpointData>();
  actor_checkpoint_data->CopyFrom(request.checkpoint_data());
  auto on_done = [this, actor_id, checkpoint_id, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add actor checkpoint: " << status.ToString()
                     << ", actor id = " << actor_id
                     << ", checkpoint id = " << checkpoint_id;
      send_reply_callback(status, nullptr, nullptr);
    } else {
      // Add checkpoint id
      auto on_done = [actor_id, checkpoint_id,
                      send_reply_callback](const Status &status) {
        RAY_LOG(DEBUG) << "Finished adding actor checkpoint, actor id = " << actor_id
                       << ", checkpoint id = " << checkpoint_id;
        send_reply_callback(status, nullptr, nullptr);
      };
      AddCheckpointId(actor_id.JobId(), actor_id, checkpoint_id, on_done);
    }
  };

  Status status = gcs_table_storage_->ActorCheckpointTable().Put(
      JobID::Nil(), checkpoint_id, actor_checkpoint_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultActorInfoHandler::HandleGetActorCheckpoint(
    const GetActorCheckpointRequest &request, GetActorCheckpointReply *reply,
    SendReplyCallback send_reply_callback) {
  ActorCheckpointID checkpoint_id =
      ActorCheckpointID::FromBinary(request.checkpoint_id());
  RAY_LOG(DEBUG) << "Getting actor checkpoint, checkpoint id = " << checkpoint_id;
  auto on_done = [checkpoint_id, reply, send_reply_callback](
                     Status status, const boost::optional<ActorCheckpointData> &result) {
    if (status.ok()) {
      RAY_DCHECK(result);
      reply->mutable_checkpoint_data()->CopyFrom(*result);
    } else {
      RAY_LOG(ERROR) << "Failed to get actor checkpoint: " << status.ToString()
                     << ", checkpoint id = " << checkpoint_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_table_storage_->ActorCheckpointTable().Get(JobID::Nil(),
                                                                 checkpoint_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
  RAY_LOG(DEBUG) << "Finished getting actor checkpoint, checkpoint id = "
                 << checkpoint_id;
}

void DefaultActorInfoHandler::HandleGetActorCheckpointID(
    const GetActorCheckpointIDRequest &request, GetActorCheckpointIDReply *reply,
    SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Getting actor checkpoint id, actor id = " << actor_id;
  auto on_done = [actor_id, reply, send_reply_callback](
                     Status status,
                     const boost::optional<ActorCheckpointIdData> &result) {
    if (status.ok()) {
      RAY_DCHECK(result);
      reply->mutable_checkpoint_id_data()->CopyFrom(*result);
    } else {
      RAY_LOG(ERROR) << "Failed to get actor checkpoint id: " << status.ToString()
                     << ", actor id = " << actor_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status =
      gcs_table_storage_->ActorCheckpointIdTable().Get(JobID::Nil(), actor_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
  RAY_LOG(DEBUG) << "Finished getting actor checkpoint id, actor id = " << actor_id;
}

void DefaultActorInfoHandler::AddCheckpointId(
    const JobID &job_id, const ActorID &actor_id, const ActorCheckpointID &checkpoint_id,
    const std::function<void(const Status &status)> &done) {
  auto on_done = [this, job_id, actor_id, checkpoint_id, done](
                     Status status,
                     const boost::optional<ActorCheckpointIdData> &result) {
    if (status.ok()) {
      if (result) {
        std::shared_ptr<ActorCheckpointIdData> copy =
            std::make_shared<ActorCheckpointIdData>(*result);
        copy->add_timestamps(absl::GetCurrentTimeNanos() / 1000000);
        copy->add_checkpoint_ids(checkpoint_id.Binary());
        auto num_to_keep = RayConfig::instance().num_actor_checkpoints_to_keep();
        while (copy->timestamps().size() > num_to_keep) {
          // Delete the checkpoint from actor checkpoint table.
          const auto &to_delete = ActorCheckpointID::FromBinary(copy->checkpoint_ids(0));
          copy->mutable_checkpoint_ids()->erase(copy->mutable_checkpoint_ids()->begin());
          copy->mutable_timestamps()->erase(copy->mutable_timestamps()->begin());
          RAY_CHECK_OK(gcs_table_storage_->ActorCheckpointTable().Delete(
              job_id, to_delete, nullptr));
        }
        RAY_CHECK_OK(gcs_table_storage_->ActorCheckpointIdTable().Put(job_id, actor_id,
                                                                      copy, done));
      } else {
        std::shared_ptr<ActorCheckpointIdData> data =
            std::make_shared<ActorCheckpointIdData>();
        data->set_actor_id(actor_id.Binary());
        data->add_timestamps(absl::GetCurrentTimeNanos() / 1000000);
        *data->add_checkpoint_ids() = checkpoint_id.Binary();
        RAY_CHECK_OK(gcs_table_storage_->ActorCheckpointIdTable().Put(job_id, actor_id,
                                                                      data, done));
      }
      RAY_LOG(DEBUG) << "Finished adding actor checkpoint, actor id = " << actor_id
                     << ", checkpoint id = " << checkpoint_id;
    } else {
      RAY_LOG(DEBUG) << "Failed to add actor checkpoint, actor id = " << actor_id
                     << ", checkpoint id = " << checkpoint_id;
      done(status);
    }
  };

  Status status =
      gcs_table_storage_->ActorCheckpointIdTable().Get(job_id, actor_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

}  // namespace rpc
}  // namespace ray
