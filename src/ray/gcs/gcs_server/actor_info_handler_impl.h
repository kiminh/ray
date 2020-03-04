#ifndef RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
#define RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H

#include "ray/gcs/gcs_storage_client/gcs_table_storage.h"
#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `ActorInfoHandler`.
class DefaultActorInfoHandler : public rpc::ActorInfoHandler {
 public:
  explicit DefaultActorInfoHandler(gcs::GcsStorageClient &gcs_storage_client) {
    actor_info_accessor_ = std::unique_ptr<gcs::GcsStorageActorInfoAccessor>(
        new gcs::GcsStorageActorInfoAccessor(gcs_storage_client));
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
  std::unique_ptr<gcs::GcsStorageActorInfoAccessor> actor_info_accessor_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
