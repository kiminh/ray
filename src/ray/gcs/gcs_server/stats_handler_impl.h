#ifndef RAY_GCS_STATS_HANDLER_IMPL_H
#define RAY_GCS_STATS_HANDLER_IMPL_H

#include "ray/gcs/gcs_storage_client/gcs_storage_accessor.h"
#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `StatsHandler`.
class DefaultStatsHandler : public rpc::StatsHandler {
 public:
  explicit DefaultStatsHandler(gcs::GcsStorageClient &gcs_storage_client) {
    stats_info_accessor_ = std::unique_ptr<gcs::GcsStorageStatsInfoAccessor>(
        new gcs::GcsStorageStatsInfoAccessor(gcs_storage_client));
  }

  void HandleAddProfileData(const AddProfileDataRequest &request,
                            AddProfileDataReply *reply,
                            SendReplyCallback send_reply_callback) override;

 private:
  std::unique_ptr<gcs::GcsStorageStatsInfoAccessor> stats_info_accessor_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_STATS_HANDLER_IMPL_H
