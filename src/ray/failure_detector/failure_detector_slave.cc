#include "failure_detector_slave.h"
#include "ray/common/ray_config.h"

namespace ray {
namespace fd {
FailureDetectorSlave::FailureDetectorSlave(boost::asio::io_context &ioc)
    : FailureDetector(ioc) {}

void FailureDetectorSlave::Run(const ip::detail::endpoint &target, uint32_t delay_ms) {
  uint32_t check_interval_seconds = RayConfig::instance().check_interval_seconds();
  uint32_t beacon_interval_seconds = RayConfig::instance().beacon_interval_seconds();
  uint32_t lease_seconds = RayConfig::instance().lease_seconds();
  uint32_t grace_seconds = RayConfig::instance().grace_seconds();
  FailureDetector::Start(check_interval_seconds, beacon_interval_seconds, lease_seconds,
                         grace_seconds);

  RegisterMaster(target, delay_ms);
}

}  // namespace fd
}  // namespace ray