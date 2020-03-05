#ifndef RAY_GCS_WORKER_INFO_HANDLER_IMPL_H
#define RAY_GCS_WORKER_INFO_HANDLER_IMPL_H

#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"
#include "ray/gcs/gcs_storage_client/gcs_table_storage.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `WorkerInfoHandler`.
class DefaultWorkerInfoHandler : public rpc::WorkerInfoHandler {
 public:
  explicit DefaultWorkerInfoHandler(
      const std::shared_ptr<gcs::GcsTableStorage> &gcs_table_storage) {
    gcs_table_storage_ = gcs_table_storage;
  }

  void HandleReportWorkerFailure(const ReportWorkerFailureRequest &request,
                                 ReportWorkerFailureReply *reply,
                                 SendReplyCallback send_reply_callback) override;

 private:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_WORKER_INFO_HANDLER_IMPL_H
