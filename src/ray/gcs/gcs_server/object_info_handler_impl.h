#ifndef RAY_GCS_OBJECT_INFO_HANDLER_IMPL_H
#define RAY_GCS_OBJECT_INFO_HANDLER_IMPL_H

#include "ray/gcs/gcs_storage_client/gcs_table_storage.h"
#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `ObjectInfoHandler`.
class DefaultObjectInfoHandler : public rpc::ObjectInfoHandler {
 public:
  explicit DefaultObjectInfoHandler(gcs::GcsStorageClient &gcs_storage_client) {
    object_info_accessor_ = std::unique_ptr<gcs::GcsStorageObjectInfoAccessor>(
        new gcs::GcsStorageObjectInfoAccessor(gcs_storage_client));
  }

  void HandleGetObjectLocations(const GetObjectLocationsRequest &request,
                                GetObjectLocationsReply *reply,
                                SendReplyCallback send_reply_callback) override;

  void HandleAddObjectLocation(const AddObjectLocationRequest &request,
                               AddObjectLocationReply *reply,
                               SendReplyCallback send_reply_callback) override;

  void HandleRemoveObjectLocation(const RemoveObjectLocationRequest &request,
                                  RemoveObjectLocationReply *reply,
                                  SendReplyCallback send_reply_callback) override;

 private:
  std::unique_ptr<gcs::GcsStorageObjectInfoAccessor> object_info_accessor_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_OBJECT_INFO_HANDLER_IMPL_H
