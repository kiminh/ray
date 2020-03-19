#ifndef RAY_GCS_STATS_HANDLER_IMPL_H
#define RAY_GCS_STATS_HANDLER_IMPL_H

#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `StatsHandler`.
class DefaultStatsHandler : public rpc::StatsHandler {
 public:
  explicit DefaultStatsHandler(
      const std::shared_ptr<gcs::GcsTableStorage> &gcs_table_storage) {
    gcs_table_storage_ = gcs_table_storage;
  }

  void HandleAddProfileData(const AddProfileDataRequest &request,
                            AddProfileDataReply *reply,
                            SendReplyCallback send_reply_callback) override;

 private:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_STATS_HANDLER_IMPL_H
