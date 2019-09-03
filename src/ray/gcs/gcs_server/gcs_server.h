#pragma once

#include "failover/abstract_failover.h"
#include "ray/common/client_connection.h"
#include "ray/failure_detector/failure_detector_master.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/http/http_client.h"
#include "ray/http/http_router.h"
#include "ray/rpc/gcs/gcs_server.h"

#include <boost/asio.hpp>
#include <vector>
using tcp = boost::asio::ip::tcp;

using FailureDetectMaster = ray::fd::FailureDetectorMaster;

namespace ray {
namespace gcs {

class GcsServer final : public rpc::GcsServerHandler {
 public:
  explicit GcsServer(const std::string &name, uint16_t port,
                     boost::asio::io_context &ioc);

 public:
  void StartRpcService();
  void StopRpcService();

  void HandleRegister(const rpc::RegisterRequest &request, rpc::RegisterReply *reply,
                      rpc::SendReplyCallback send_reply_callback) override;

 private:
  void AutoRouteUri(boost::system::error_code err,
                    std::shared_ptr<boost::asio::deadline_timer> timer);
  void CollectRayletInfo(const std::string &name, HttpParams &&params, std::string &&data,
                         std::shared_ptr<HttpReply> r);

 private:
  boost::asio::io_context &ioc_;
  FailureDetectMaster fd_;
  std::unique_ptr<AbstractFailover> failover_;
  rpc::AsioRpcServer gcs_server_;
  rpc::GcsAsioRpcService gcs_service_;
  std::shared_ptr<RedisGcsClient> gcs_client_;
  std::unordered_map<ClientID, std::shared_ptr<HttpAsyncClient>> http_clients_;
  bool has_route_table_ = false;
};

}  // namespace gcs
}  // namespace ray
