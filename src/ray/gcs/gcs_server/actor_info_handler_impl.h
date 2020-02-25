#ifndef RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
#define RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H

#include "gcs_actor_manager.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {

namespace gcs {
class GcsActorManager;
class GcsNodeManager;
}  // namespace gcs

namespace rpc {
/// This implementation class of `ActorInfoHandler`.
class DefaultActorInfoHandler : public rpc::ActorInfoHandler {
 public:
  explicit DefaultActorInfoHandler(
      gcs::RedisGcsClient &gcs_client,
      std::shared_ptr<gcs::GcsActorManager> gcs_actor_manager,
      std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager)
      : gcs_client_(gcs_client),
        gcs_actor_manager_(std::move(gcs_actor_manager)),
        gcs_node_manager_(std::move(gcs_node_manager)) {}

  void HandleCreateActor(const CreateActorRequest &request, CreateActorReply *reply,
                         SendReplyCallback send_reply_callback) override;

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
  gcs::RedisGcsClient &gcs_client_;
  std::shared_ptr<gcs::GcsActorManager> gcs_actor_manager_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
