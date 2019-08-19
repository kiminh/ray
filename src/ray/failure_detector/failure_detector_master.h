#pragma once

#include <ray/rpc/failure_detector/failure_detector_server.h>
#include "failure_detector.h"

namespace ray {
namespace fd {
class FailureDetectorMaster : public FailureDetector, public rpc::FailureDetectorHandler {
 public:
  explicit FailureDetectorMaster(boost::asio::io_context &ioc);

 public:
  void HandlePing(const rpc::BeaconMsg &request, rpc::BeaconAck *reply,
                  rpc::SendReplyCallback send_reply_callback) override;

  void RegiserServiceTo(rpc::AsioRpcServer &server);

 private:
  rpc::FailureDetectorAsioRpcService fd_service_;
};
}  // namespace fd
}  // namespace ray