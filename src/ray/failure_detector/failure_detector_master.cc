#include "failure_detector_master.h"

namespace ray {
namespace fd {

FailureDetectorMaster::FailureDetectorMaster(boost::asio::io_context &ioc)
    : FailureDetector(ioc), fd_service_(*this) {}

void FailureDetectorMaster::HandlePing(const rpc::BeaconMsg &request,
                                       rpc::BeaconAck *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  OnPingInternal(request, reply);
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void FailureDetectorMaster::RegiserServiceTo(rpc::AsioRpcServer &server) {
  server.RegisterService(fd_service_);
}

}  // namespace fd
}  // namespace ray