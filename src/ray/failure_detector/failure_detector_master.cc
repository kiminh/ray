#include "failure_detector_master.h"
#include <ray/common/ray_config.h>

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

void FailureDetectorMaster::Run() {
  uint32_t check_interval_seconds = RayConfig::instance().check_interval_seconds();
  uint32_t beacon_interval_seconds = RayConfig::instance().beacon_interval_seconds();
  uint32_t lease_seconds = RayConfig::instance().lease_seconds();
  uint32_t grace_seconds = RayConfig::instance().grace_seconds();
  FailureDetector::Start(check_interval_seconds, beacon_interval_seconds, lease_seconds,
                         grace_seconds);
}

void FailureDetectorMaster::RegiserServiceTo(rpc::AsioRpcServer &server) {
  server.RegisterService(fd_service_);
}

}  // namespace fd
}  // namespace ray