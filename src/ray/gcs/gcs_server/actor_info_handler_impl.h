#ifndef RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
#define RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H

#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `ActorInfoHandler`.
class DefaultActorInfoHandler : public rpc::ActorInfoHandler {
 public:
  explicit DefaultActorInfoHandler(
      const std::shared_ptr<gcs::GcsTableStorage> &gcs_table_storage) {
    gcs_table_storage_ = gcs_table_storage;
  }

  void HandleGetActorInfo(const GetActorInfoRequest &request, GetActorInfoReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleRegisterActorInfo(const RegisterActorInfoRequest &request,
                               RegisterActorInfoReply *reply,
                               SendReplyCallback send_reply_callback) override;

  void HandleUpdateActorInfo(const UpdateActorInfoRequest &request,
                             UpdateActorInfoReply *reply,
                             SendReplyCallback send_reply_callback) override;

  void HandleAddActorCheckpoint(const AddActorCheckpointRequest &request,
                                AddActorCheckpointReply *reply,
                                SendReplyCallback send_reply_callback) override;

  void HandleGetActorCheckpoint(const GetActorCheckpointRequest &request,
                                GetActorCheckpointReply *reply,
                                SendReplyCallback send_reply_callback) override;

  void HandleGetActorCheckpointID(const GetActorCheckpointIDRequest &request,
                                  GetActorCheckpointIDReply *reply,
                                  SendReplyCallback send_reply_callback) override;

 private:
  /// Add a checkpoint id to an actor, and remove a previous checkpoint if the
  /// total number of checkpoints in GCS exceeds the max allowed value.
  ///
  /// \param job_id The ID of the job.
  /// \param actor_id ID of the actor.
  /// \param checkpoint_id ID of the checkpoint.
  /// \param done Callback that will be called after checkpoint id has been added.
  /// \return Status.
  void AddCheckpointId(const JobID &job_id, const ActorID &actor_id,
                       const ActorCheckpointID &checkpoint_id,
                       const std::function<void(const Status &status)> &done);

  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
