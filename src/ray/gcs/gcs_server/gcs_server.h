#pragma once

#include "failover/abstract_failover.h"
#include "ray/common/client_connection.h"
#include "ray/failure_detector/failure_detector_master.h"
#include "ray/rpc/gcs/gcs_server.h"

#include <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;

using FailureDetectMaster = ray::fd::FailureDetectorMaster;

namespace ray {
namespace gcs {

class GcsServer : public rpc::GcsServerHandler {
 public:
  explicit GcsServer(const std::string &name, uint16_t port,
                     boost::asio::io_context &ioc);

 public:
  void StartRpcService();
  void StopRpcService();

  void HandleRegister(const rpc::RegisterRequest &request, rpc::RegisterReply *reply,
                      rpc::SendReplyCallback send_reply_callback) override;

 private:
  boost::asio::io_context &ioc_;
  std::shared_ptr<FailureDetectMaster> fd_;
  std::unique_ptr<AbstractFailover> failover_;
  rpc::AsioRpcServer gcs_server_;
  rpc::GcsAsioRpcService gcs_service_;
};

}  // namespace gcs
}  // namespace ray
