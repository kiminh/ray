#include "actor_info_handler_impl.h"
#include "gcs_actor_manager.h"
#include "gcs_leased_worker.h"
#include "gcs_node_manager.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultActorInfoHandler::HandleCreateActor(
    const ray::rpc::CreateActorRequest &request, ray::rpc::CreateActorReply *reply,
    ray::rpc::SendReplyCallback send_reply_callback) {
  RAY_UNUSED(reply);
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(DEBUG) << "Creating actor, actor id = " << actor_id;
  gcs_actor_manager_->CreateActor(
      request, [send_reply_callback, actor_id](Status status) {
        if (status.ok()) {
          RAY_LOG(INFO) << "Succeed in creating actor, actor id = " << actor_id;
        } else {
          RAY_LOG(ERROR) << "Failed to create actor, actor id = " << actor_id;
        }
        send_reply_callback(status, nullptr, nullptr);
      });
  RAY_LOG(DEBUG) << "Finished Creating actor, actor id = " << actor_id;
}

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
    } else {
      RAY_LOG(ERROR) << "Failed to get actor info: " << status.ToString()
                     << ", actor id = " << actor_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Actors().AsyncGet(actor_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
  RAY_LOG(DEBUG) << "Finished getting actor info, actor id = " << actor_id;
}

void DefaultActorInfoHandler::HandleRegisterActorInfo(
    const rpc::RegisterActorInfoRequest &request, rpc::RegisterActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_table_data().actor_id());
  RAY_LOG(DEBUG) << "Registering actor info, actor id = " << actor_id;
  auto actor_table_data = std::make_shared<ActorTableData>();
  actor_table_data->CopyFrom(request.actor_table_data());
  auto on_done = [actor_id, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to register actor info: " << status.ToString()
                     << ", actor id = " << actor_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Actors().AsyncRegister(actor_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished registering actor info, actor id = " << actor_id;
}

void DefaultActorInfoHandler::HandleUpdateActorInfo(
    const rpc::UpdateActorInfoRequest &request, rpc::UpdateActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Updating actor info, actor id = " << actor_id;

  if (request.actor_table_data().state() == ray::rpc::ActorTableData::RECONSTRUCTING) {
    auto node_id = ClientID::FromBinary(request.actor_table_data().address().raylet_id());
    auto worker_id =
        WorkerID::FromBinary(request.actor_table_data().address().worker_id());
    if (auto node = gcs_node_manager_->GetNode(node_id)) {
      if (auto leased_worker = node->RemoveLeasedWorker(worker_id)) {
        if (auto actor = leased_worker->GetActor()) {
          RAY_CHECK(actor->GetActorID() == ActorID::FromBinary(request.actor_id()));
          gcs_actor_manager_->OnActorFailure(std::move(actor));
        }
      }
    }
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
  RAY_LOG(DEBUG) << "Finished updating actor info, actor id = " << actor_id;
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
  auto on_done = [actor_id, checkpoint_id, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add actor checkpoint: " << status.ToString()
                     << ", actor id = " << actor_id
                     << ", checkpoint id = " << checkpoint_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Actors().AsyncAddCheckpoint(actor_checkpoint_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished adding actor checkpoint, actor id = " << actor_id
                 << ", checkpoint id = " << checkpoint_id;
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

  Status status = gcs_client_.Actors().AsyncGetCheckpoint(checkpoint_id, on_done);
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

  Status status = gcs_client_.Actors().AsyncGetCheckpointID(actor_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
  RAY_LOG(DEBUG) << "Finished getting actor checkpoint id, actor id = " << actor_id;
}

}  // namespace rpc
}  // namespace ray
