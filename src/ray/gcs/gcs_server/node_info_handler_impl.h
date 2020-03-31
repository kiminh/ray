#ifndef RAY_GCS_NODE_INFO_HANDLER_IMPL_H
#define RAY_GCS_NODE_INFO_HANDLER_IMPL_H

#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/gcs_client/gcs_table_pubsub.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `NodeInfoHandler`.
class DefaultNodeInfoHandler : public rpc::NodeInfoHandler {
 public:
  explicit DefaultNodeInfoHandler(
      const std::shared_ptr<gcs::GcsTableStorage> &gcs_table_storage,
      const std::shared_ptr<gcs::RedisClient> &redis_client) :
      node_pub_(redis_client),
      node_resource_pub_(redis_client),
      heartbeat_pub_(redis_client),
      heartbeat_batch_pub_(redis_client) {
    gcs_table_storage_ = gcs_table_storage;
  }

  void HandleRegisterNode(const RegisterNodeRequest &request, RegisterNodeReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleUnregisterNode(const UnregisterNodeRequest &request,
                            UnregisterNodeReply *reply,
                            SendReplyCallback send_reply_callback) override;

  void HandleGetAllNodeInfo(const GetAllNodeInfoRequest &request,
                            GetAllNodeInfoReply *reply,
                            SendReplyCallback send_reply_callback) override;

  void HandleReportHeartbeat(const ReportHeartbeatRequest &request,
                             ReportHeartbeatReply *reply,
                             SendReplyCallback send_reply_callback) override;

  void HandleReportBatchHeartbeat(const ReportBatchHeartbeatRequest &request,
                                  ReportBatchHeartbeatReply *reply,
                                  SendReplyCallback send_reply_callback) override;

  void HandleGetResources(const GetResourcesRequest &request, GetResourcesReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleUpdateResources(const UpdateResourcesRequest &request,
                             UpdateResourcesReply *reply,
                             SendReplyCallback send_reply_callback) override;

  void HandleDeleteResources(const DeleteResourcesRequest &request,
                             DeleteResourcesReply *reply,
                             SendReplyCallback send_reply_callback) override;

 private:
  void UpdateResource(
      const ClientID &node_id,
      const std::unordered_map<std::string, rpc::ResourceTableData> &resource_map,
      SendReplyCallback send_reply_callback,
      std::function<void()> callback);

  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  gcs::GcsNodeTablePubSub node_pub_;
  gcs::GcsNodeResourceTablePubSub node_resource_pub_;
  gcs::GcsHeartbeatTablePubSub heartbeat_pub_;
  gcs::GcsHeartbeatBatchTablePubSub heartbeat_batch_pub_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_NODE_INFO_HANDLER_IMPL_H
