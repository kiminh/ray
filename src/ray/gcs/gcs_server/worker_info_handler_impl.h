#ifndef RAY_GCS_WORKER_INFO_HANDLER_IMPL_H
#define RAY_GCS_WORKER_INFO_HANDLER_IMPL_H

#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/gcs_client/gcs_table_pubsub.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `WorkerInfoHandler`.
class DefaultWorkerInfoHandler : public rpc::WorkerInfoHandler {
 public:
  explicit DefaultWorkerInfoHandler(
      const std::shared_ptr<gcs::GcsTableStorage> &gcs_table_storage,
      const std::shared_ptr<gcs::RedisClient> &redis_client) :
      worker_failure_pub_(redis_client) {
    gcs_table_storage_ = gcs_table_storage;
  }

  void HandleReportWorkerFailure(const ReportWorkerFailureRequest &request,
                                 ReportWorkerFailureReply *reply,
                                 SendReplyCallback send_reply_callback) override;

 private:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  gcs::GcsWorkerFailureTablePubSub worker_failure_pub_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_WORKER_INFO_HANDLER_IMPL_H
