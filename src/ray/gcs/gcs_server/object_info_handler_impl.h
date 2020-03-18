#ifndef RAY_GCS_OBJECT_INFO_HANDLER_IMPL_H
#define RAY_GCS_OBJECT_INFO_HANDLER_IMPL_H

#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `ObjectInfoHandler`.
class DefaultObjectInfoHandler : public rpc::ObjectInfoHandler {
 public:
  explicit DefaultObjectInfoHandler(
      const std::shared_ptr<gcs::GcsTableStorage> &gcs_table_storage) {
    gcs_table_storage_ = gcs_table_storage;
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
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_OBJECT_INFO_HANDLER_IMPL_H
