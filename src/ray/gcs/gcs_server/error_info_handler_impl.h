#ifndef RAY_GCS_ERROR_INFO_HANDLER_IMPL_H
#define RAY_GCS_ERROR_INFO_HANDLER_IMPL_H

#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `ErrorInfoHandler`.
class DefaultErrorInfoHandler : public rpc::ErrorInfoHandler {
 public:
  explicit DefaultErrorInfoHandler(
      const std::shared_ptr<gcs::GcsTableStorage> &gcs_table_storage) {
    gcs_table_storage_ = gcs_table_storage;
  }

  void HandleReportJobError(const ReportJobErrorRequest &request,
                            ReportJobErrorReply *reply,
                            SendReplyCallback send_reply_callback) override;

 private:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_ERROR_INFO_HANDLER_IMPL_H
