#ifndef RAY_GCS_ERROR_INFO_HANDLER_IMPL_H
#define RAY_GCS_ERROR_INFO_HANDLER_IMPL_H

#include "ray/gcs/gcs_storage_client/gcs_table_storage.h"
#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `ErrorInfoHandler`.
class DefaultErrorInfoHandler : public rpc::ErrorInfoHandler {
 public:
  explicit DefaultErrorInfoHandler(gcs::GcsStorageClient &gcs_storage_client) {
    error_info_accessor_ = std::unique_ptr<gcs::GcsStorageErrorInfoAccessor>(
        new gcs::GcsStorageErrorInfoAccessor(gcs_storage_client));
  }

  void HandleReportJobError(const ReportJobErrorRequest &request,
                            ReportJobErrorReply *reply,
                            SendReplyCallback send_reply_callback) override;

 private:
  std::unique_ptr<gcs::GcsStorageErrorInfoAccessor> error_info_accessor_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_ERROR_INFO_HANDLER_IMPL_H
